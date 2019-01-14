import asyncio
import contextlib
import datetime
import io
import logging
import multiprocessing
import multiprocessing.managers
import os
import queue
import random
import time
import traceback
import signal
import sys
import weakref

from mahler.core.utils.std import stdredirect
import mahler.core.registrar
import mahler.core.utils.errors

logger = logging.getLogger('mahler.core.worker')

# TODO: If in debug mode,
#       when code change is detected, interrupt process and start over the worker with
#       up-to-date code.
#       In debug mode, if execution fails, stop with pdb. If user wants to continue, enter `c`
#       else, if the user make a modification, worker is reloaded and same task is retried.

STOPPING_TEMPLATE = '---\nStopping execution: {}\n---\n'
STARTING_TEMPLATE = '---\nStarting execution: {}\n---\n'

def sigterm_handler(signal, frame):
    if sigterm_handler.triggered:
        return
    else:
        sigterm_handler.triggered = True

    raise mahler.core.utils.errors.SignalInterrupt("Task killed by SIGTERM")


sigterm_handler.triggered = False


# From https://stackoverflow.com/questions/21104997/keyboard-interrupt-with-pythons-multiprocessing
# initilizer for SyncManager
def mgr_init():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)


@contextlib.contextmanager
def tmp_directory(working_dir=None):
    curdir= os.getcwd()
    try:
        if working_dir is not None:
            os.chdir(working_dir)
        yield
    finally:
        os.chdir(curdir)


class StdQueue():
    def __init__(self, queue, stream, mute=False, buffer_size=50):
        self.queue = queue
        self.stream = stream
        self.mute= mute
        self.buffer_size = buffer_size
        self.lines = []

    def write(self, text):
        n_chars = 0
        if '\n' in text:
            lines = text.split("\n")
            if not lines[-1]:
                lines = lines[:-1]
            for line in lines:
                # Last part may not be a line
                n_chars += self.writeline(line)
        else:
            self.lines.append(text)
            n_chars = len(text)

        if not self.mute:
            self.stream.write(text)

        return n_chars

    def writeline(self, line):
        if self.lines and not self.lines[-1].endswith('\n'):
            self.lines[-1] += line + '\n'
        else:
            self.lines.append(line + '\n')

        if len(self.lines) > self.buffer_size:
            self.flush()

        return len(line) + 1

    def flush(self):
        if self.lines:
            self.queue.put(''.join(self.lines))
            self.lines = []

    def close(self):
        self.flush()

    def read(self):
        raise NotImplementedError()

    def readline(self):
        raise NotImplementedError()

    def seek(self):
        raise NotImplementedError()

    def tell(self):
        raise NotImplementedError()


class TaskProcess(mahler.core.utils.errors.ProcessExceptionHandler):
    def __init__(self, task_id, state, stdout, stderr, data, volume, **kwargs):
        self.task_id = task_id
        self.state = state
        self.stdout = stdout
        self.stderr = stderr
        self.data = data
        self.volume = volume 
        super(TaskProcess, self).__init__(**kwargs)

    def try_catch_run(self):
        registrar = mahler.core.registrar.build(name='mongodb')
        task = next(iter(registrar.retrieve_tasks(id=self.task_id)))
        data, volume = task.run(self.state, stdout=self.stdout, stderr=self.stderr)
        if data:
            self.data.update(data)
        if volume:
            self.volume.update(volume)


class HeartBeatProcess(mahler.core.utils.errors.ProcessExceptionHandler):
    def __init__(self, task_id, **kwargs):
        self.task_id = task_id
        self.logger = logging.getLogger(__name__ + ".heartbeat")
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logging.getLogger().handlers[0].formatter)
        self.logger.addHandler(stream_handler)
        super(HeartBeatProcess, self).__init__(**kwargs)

    def try_catch_run(self):
        registrar = mahler.core.registrar.build(name='mongodb')
        task = next(iter(registrar.retrieve_tasks(id=self.task_id)))
        task._status.refresh()

        heartbeat_status = mahler.core.status.Running('heartbeat')

        # while not self.stop.is_set():
        while True:
            slept = 0
            try:
                while True:
                    time_to_sleep = min(task.heartbeat - slept, 5)
                    time.sleep(time_to_sleep)
                    slept += time_to_sleep
                    
                    if slept >= task.heartbeat:
                        break

                    # TODO: Replace this with observers on database
                    #       ex: tailable cursors in MongoDB
                    #       https://docs.mongodb.com/manual/core/tailable-cursors/
                    new_status = task.status
                    if new_status.name == 'Suspended':
                        raise mahler.core.utils.errors.RaceCondition('Suspended remotely')
                    elif task.status.name == 'Cancelled':
                        raise mahler.core.utils.errors.RaceCondition('Cancelled remotely')

                registrar.update_status(task, heartbeat_status)
            except mahler.core.utils.errors.RaceCondition as e:
                new_status = task.status
                if isinstance(new_status, mahler.core.status.Suspended):
                    raise mahler.core.utils.errors.SignalSuspend(
                        'Task suspended remotely: {}'.format(new_status.message))
                elif isinstance(new_status, mahler.core.status.Cancelled):
                    raise mahler.core.utils.errors.SignalCancel(
                        'Task cancelled remotely: {}'.format(new_status.message))
                else:
                    raise


def run(registrar, task, state, stdout, stderr):

    heartbeat = HeartBeatProcess(task.id)
    heartbeat.start()

    manager = multiprocessing.managers.SyncManager()
    manager.start(mgr_init)

    stdout_queue = manager.Queue()
    stderr_queue = manager.Queue()

    stdout = StdQueue(stdout_queue, sys.stdout, mute=not logger.isEnabledFor(logging.INFO),
                      buffer_size=50)
    stderr = StdQueue(stderr_queue, sys.stderr, mute=not logger.isEnabledFor(logging.INFO),
                      buffer_size=50)

    data = manager.dict()
    volume = manager.dict()

    try:
        task_thread = TaskProcess(task.id, state, stdout, stderr, data, volume)
        task_thread.start()

        while heartbeat.is_alive() and task_thread.is_alive():
            if not stdout_queue.empty():
                try:
                    registrar.update_stdout(task, stdout_queue.get(timeout=0.1))
                except queue.Empty as e:
                    pass

            if not stderr_queue.empty():
                try:
                    registrar.update_stderr(task, stderr_queue.get(timeout=0.1))
                except queue.Empty as e:
                    pass

        # If heartbeat fails, it probably means the status was changed remotely. We 
        # terminate the task and ignore any possible errors that occured concurrently, the
        # remote status change is what we keep.
        if not heartbeat.is_alive():
            task_thread.terminate()

            # Will raise the error
            heartbeat.join()

        if not task_thread.is_alive():
            heartbeat.terminate()

            # Will raise the error
            task_thread.join()

        data = dict()
        data.update(task_thread.data)

        volume = dict()
        volume.update(task_thread.volume)
    finally:
        stdout.flush()
        remaining_stdout = ""
        try:
            while not stdout_queue.empty():
                # TODO: Why do we get TypeError: Can't convert 'bool' object to str implicitly
                #       on KeyboardInterrupt?
                remaining_stdout += stdout_queue.get(timeout=0.01)
        except (queue.Empty, BaseException) as e:
            pass

        if remaining_stdout:
            task._stdout.refresh()
            registrar.update_stdout(task, remaining_stdout)

        stderr.flush()
        remaining_stderr = ""
        try:
            while not stderr_queue.empty():
                remaining_stderr += stderr_queue.get(timeout=0.01)
        except (queue.Empty, BaseException) as e:
            pass
        if remaining_stderr:
            task._stderr.refresh()
            registrar.update_stderr(task, remaining_stderr)

    return data, volume


def execute(registrar, state, task):

    # Load in
    task._stdout.refresh()
    task._stderr.refresh()

    utcnow = datetime.datetime.utcnow()

    registrar.update_stdout(task, STARTING_TEMPLATE.format(utcnow) + "\n")
    registrar.update_stderr(task, STARTING_TEMPLATE.format(utcnow) + "\n")

    try:
        data, volume = run(registrar, task, state, sys.stdout, sys.stderr)
        logger.debug('Saving output')
        registrar.set_output(task, data)
        logger.debug('Output saved')
        status = mahler.core.status.Completed('')

    except mahler.core.utils.errors.SignalSuspend as e:
        status = mahler.core.status.Suspended('Suspended remotely (status changed to Suspended)')
        raise

    except mahler.core.utils.errors.SignalCancel as e:
        status = mahler.core.status.Cancelled('Cancelled by user')
        raise

    except mahler.core.utils.errors.SignalInterrupt as e:
        status = mahler.core.status.Interrupted('Interrupted by system (SIGTERM)')
        raise

    except KeyboardInterrupt as e:
        status = mahler.core.status.Suspended('Suspended by user (KeyboardInterrupt)')
        raise

    except BaseException as e:
        # broken
        message = "execution error:{}: {}".format(type(e), e)
        logger.info(message)
        registrar.update_stderr(task, traceback.format_exc() + "\n")
        # status = mahler.core.status.FailedOver(str(e))
        raise mahler.core.utils.errors.ExecutionError(str(e)) from e

    finally:
        utcnow = datetime.datetime.utcnow()
        registrar.update_stdout(task, STARTING_TEMPLATE.format(utcnow) + "\n")
        registrar.update_stderr(task, STARTING_TEMPLATE.format(utcnow) + "\n")

    # TODO
    # NOTE: storage.write adds volume links to the registry.
    #       storage.write(registrar, volume)
    # registrar.set_volume(task, volume)

    return status


class Maintainer(multiprocessing.Process):
    def __init__(self, tags, container, sleep_time, **kwargs):
        super(Maintainer, self).__init__(**kwargs)
        self.tags = tags
        self.container = container
        self.sleep_time = sleep_time
        self.logger = logging.getLogger('mahler.daemon.' + self.__class__.__name__)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logging.getLogger().handlers[0].formatter)
        self.logger.addHandler(stream_handler)

    def run(self):
        registrar = mahler.core.registrar.build(name='mongodb')

        while True:
            updated = self.maintain(registrar)
            if updated:
                self.logger.info("{} task updated".format(updated))
            else:
                sleep_time = random.gauss(self.sleep_time, self.sleep_time / 10.)
                self.logger.info(
                    'No more task to maintain. Waiting {}s before trying again.'.format(sleep_time))
                time.sleep(sleep_time)

    def maintain(self, registrar):
        pass


class UnreportedMaintainer(Maintainer):
    def maintain(self, registrar):
        return registrar.maintain_unreported(limit=None)


class ReportMaintainer(Maintainer):
    def maintain(self, registrar):
        return registrar.maintain_reports(tags=self.tags, container=self.container, limit=None)


class LostTaskMaintainer(Maintainer):
    def maintain(self, registrar):
        return registrar.maintain_lost(tags=self.tags, container=self.container, limit=None)


class ToQueuedMaintainer(Maintainer):
    def maintain(self, registrar):
        return registrar.maintain_to_queue(tags=self.tags, container=self.container, limit=None)


class OnHoldMaintainer(Maintainer):
    def maintain(self, registrar):
        return registrar.maintain_onhold(tags=self.tags, container=self.container, limit=None)


def main(tags=tuple(), container=None, working_dir=None, max_tasks=10e10, depletion_patience=10,
         exhaust_wait_time=20, max_failedover_attempts=3, **kwargs):


    with tmp_directory(working_dir):
        _main(tags=tags, container=container, max_tasks=max_tasks,
              depletion_patience=depletion_patience, exhaust_wait_time=exhaust_wait_time,
              max_failedover_attempts=max_failedover_attempts)


def _main(tags=tuple(), container=None, max_tasks=10e10, depletion_patience=10,
          exhaust_wait_time=20, max_failedover_attempts=3):
    # TODO: Support config
    registrar = mahler.core.registrar.build(name='mongodb')
    dispatcher = Dispatcher(registrar)
    # tasks = list(technician.query(registrar))

    exhaust_failures = 0

    state = State()

    for i in range(int(max_tasks)):
        if exhaust_failures >= depletion_patience:
            print("Patience exhausted and no more task available.")
            break

        try:
            # TODO: When choosing the trial to execute, limit it based on the current container
            #       being used. Cannot pick trials which should be executed in a different
            #       container.

            # NOTE: Dispatcher should turn the task-document into an engine task,
            #       loading the volume at the same time
            task = dispatcher.pick(registrar, tags, container, state)  # tasks, registrar)
            exhaust_failures = 0
        except RuntimeError:
            logger.info('Dispatcher could not pick any task for execution.')
            # NOTE: Maintainance could be done in parallel while a task is being executed.
            exhaust_failures += 1
            print("{} (UTC): No more task available, waiting {} seconds before "
                  "trying again. {} attemps remaining.".format(
                      datetime.datetime.utcnow(), exhaust_wait_time,
                      depletion_patience - exhaust_failures))
            time.sleep(exhaust_wait_time)
            continue

        # set status of trial as running
        # Execute command from db
        # TODO: inside task.run, update the state
        # state.update(task=self, data=data, volume=volume)
        # TODO: inside task.run, convert data into measurements and volume into
        #       volume documents. Note that they are not registered yet.
        # TODO: Add heartbeat, maybe set in the worker itself.
        # TODO: If task is immutable, state should be kept.
        #               is mutable, state should be emptied in task and rebuilt based on output
        #       NOTE: This is to avoid memory leak causing out of memory errors.

        # NOTE: Volume is file-like object, but the task may require large objects that are not
        #       file-like.
        #       Pass the file-like object if not parsed yet, or 
        #       return {name: {file: file-like object, object: object}}
        #       Set object in state
        #       Either: 1. operator has parsers to turn 

        #       Model, optimizer

        #       Checkpoint -> load() -> state_dict -> model.load(), opimizer.load()
        # TODO: In restore(_status, file_like_checkpoint, batch_size, lr, momentum, ...)
        #       checkpoint = laboratorium.backend.pytorch.load(file_like_checkpoint)
        #       return dict(model=checkpoint['model'], optimizer=checkpoint['optimizer'],
        #                   device=device, ...)
        # NOTE: What gets out of restore() is set in `state`
        # NOTE: Checkpoint should be object specific, so that we can easily map
        #       {volume-name: {'file': file-like, 'object': object}}

        # TODO: Add heartbeat for reserved and running
        print('Executing task: {}'.format(task.id))
        assert task.status.name == 'Reserved'
        registrar.update_status(task, mahler.core.status.Running('start execution'))
        registrar.update_report(task.to_dict())
        try:
            new_status = execute(registrar, state, task)

        except KeyboardInterrupt as e:
            try:
                print('Execution of task {} suspended by user (KeyboardInterrupt)'.format(task.id))
                print()
                print('Execution will resume in 5 seconds.')
                print('To stop the worker, press crtl-c again before the countdown.')
                print()
                for i in range(5, 0, -1):
                    print('{}...'.format(i))
                    time.sleep(1)
                print()
            except KeyboardInterrupt as e:
                print()
                print('Now leaving worker...')
                print()
                raise SystemExit()
            finally:
                new_status = mahler.core.status.Suspended('Suspended by user (KeyboardInterrupt)')
                assert task.status.name == 'Running'
                registrar.update_status(task, new_status)
                print('Execution of task {} interrupted'.format(task.id))
                print('New status: {}'.format(new_status))

            print()
            print('Now resuming worker...')
            print()

            continue

        except mahler.core.utils.errors.SignalSuspend as e:
            print('Execution of task {} suspended'.format(task.id))
            print('New status: {}'.format(task.status))
            continue

        except mahler.core.utils.errors.SignalCancel as e:
            print('Execution of task {} cancelled'.format(task.id))
            print('New status: {}'.format(task.status))
            continue

        except mahler.core.utils.errors.SignalInterrupt as e:
            new_status = mahler.core.status.Interrupted('Interrupted by system (SIGTERM)')
            assert task.status.name == 'Running'
            registrar.update_status(task, new_status)
            print('Execution of task {} interrupted'.format(task.id))
            print('New status: {}'.format(new_status))
            raise

        except mahler.core.utils.errors.ExecutionError as e:

            new_status = mahler.core.status.Broken(str(e))
            assert task.status.name == 'Running'
            registrar.update_status(task, new_status)
            print('Execution of task {} crashed'.format(task.id))

            broke_n_times = sum(int(event['item']['name'] == new_status.name)
                                for event in task._status.history)
            if broke_n_times < max_failedover_attempts:
                new_status = mahler.core.status.FailedOver('Broke {} times'.format(broke_n_times))
                registrar.update_status(task, new_status)

            print('New status: {}'.format(new_status))

        except Exception as e:
            message = "mahler error: {}".format(e)
            print('Execution of task {} crashed because of problem in mahler'.format(task.id))
            new_status = mahler.core.status.Broken(message)
            registrar.update_status(task, new_status)
            new_status = mahler.core.status.FailedOver('mahler error')
            registrar.update_status(task, new_status)
            print('New status: {}'.format(new_status))
            raise e.__class__(message) from e

        else:
            assert task.status.name == 'Running'
            registrar.update_status(task, new_status)
            print('Execution of task {} stopped'.format(task.id))
            print('New status: {}'.format(new_status))

        finally:
            registrar.update_report(task.to_dict())


        # Use an interface with Kleio to fetch data and save it in lab's db.
        # Could be sacred, comet.ml or WandB...
        # technician.transfert(datamanagement, registrar)
        # Or rather... make the registrar or the knowledge-base to be backend specific.

        # Mark the run as completed and exit.
        # technician.complete(?)

    # if max_trials not reached and trials available from another container.
    # subprocess.Popen('laboratorium execute <some other container> <remaining max trials>')


def _check_tasks(tasks):
    non_operator_items = [item for item in tasks if not isinstance(item, Task)]
    if non_operator_items:
        raise ValueError(
            "Positional arguments must be operators on which the one being created "
            "will depend. Faulty arguments:\n{}".format(
                [(type(item), item) for item in non_operator_items]))


def register(task, container, after=None, before=None):
    if isinstance(after, Task):
        after = [after]
    _check_tasks(after)
    if isinstance(before, Task):
        before = [before]
    _check_tasks(before)

    # TODO: If after of before are defined, make sure that either
    #       1) they are all TaskTemplate and have the same experiment, or
    #       2) they are all Task and have the same trial.


def register_template(task, experiment, after=None, before=None):
    study.TaskTemplate()


def register_task(registrar, task, trial, after=None, before=None):
    # TODO: Build inputs
    # TODO: Build dependencies
    # NOTE: We don't build template, because instantiation of templates should not be 
    #       done here through the public interface, but rather automatically inside the 
    #       technician object.
    document = study.Task(trial, fct=task.op.import_string, inputs=task.inputs,
                          dependencies=task.dependencies)
    registrar.register_tasks([document])


# For registering...
    if not isinstance(container, (Experiment, Trial)):
        raise ValueError(
            "Given container must be of type Experiment or Trial: {}".format(type(container)))


class State(object):
    def __init__(self):
        self.data = dict()
        self.ids = dict()

    def get(self, dependencies, keys):
        return {key: self.data[key] for key in key
                if self.ids[key] in dependencies}

    def update(self, task, data):
        self.ids.update({key: task.id for key in data.keys()})
        self.data.update(data)


class Worker(object):
    def run(self, task):
        # if task needs restore 
        # create new task for the restore
        # execute it
        # execute 
        pass

    def register(self, task, data, volume):
        # TODO: if registering fails, set run as broken and rollback state.
        volume_paths = self.storate.write(self.registrar, volume)

        task.document.measurements = [Measurement(task.document, name, value)
                                      for (name, value) in data.items()]

        # TODO: Add cache timeout for the volume, we don't want to clutter the storage.
        #       Maybe clean only if not last instance of the same name
        #       in a given trial (workflow). Last instance is retained for further
        #       addition of dependent tasks. NOTE: That may only be necessary for the
        #       local FS volume plugin.
        task.document.artefacts = [Artefact(task.document, name, volume_paths[name])
                                   for name in volume.keys()]

        # TODO: Add data to the registry
        # NOTE: Must be *after* storage.write so that task.volume contains 
        #       the links to the storage.
        self.registrar.register_tasks([task.document])


class Dispatcher(object):

    __refs__ = weakref.WeakSet()

    def __init__(self, registrar):
        self._picked_task = None

        Dispatcher.__refs__.add(self)

    @property
    def picked_task(self):
        return self._picked_task

    def pick(self, registrar, tags, container, state):
        tasks = registrar.retrieve_tasks(
            tags=tags, container=container,
            status=mahler.core.status.Queued(''),
            limit=100)

        # TODO: Sort by priority
        # TODO: Pick tasks based on what is available in state (needs dependencies implementation)
        for task in tasks:
            # To reduce race conditions
            if random.random() >= 0.5:
                continue

            try:
                registrar.reserve(task)
                self._picked_task = task
                return task
            except (ValueError, mahler.core.registrar.RaceCondition) as e:
                logger.info('Task {} reserved by concurrent worker'.format(task.id))
                continue

        raise RuntimeError("No task available")
