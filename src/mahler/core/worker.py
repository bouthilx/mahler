import asyncio
import contextlib
from collections import defaultdict
import copy
import datetime
import io
import logging
import multiprocessing
import multiprocessing.managers
import os
import pprint
import queue
import random
import time
import traceback
import signal
import sys
import weakref

import cotyledon

from hurry.filesize import size as print_h_size

from mahler.core.utils.std import stdredirect
from mahler.core.utils.host import fetch_host_name, get_gpu_usage, ResourceUsageMonitor
import mahler.core.registrar
import mahler.core.utils.errors
from mahler.core.utils.flatten import flatten


logger = logging.getLogger('mahler.core.worker')


N_WORKERS = 5

# TODO: If in debug mode,
#       when code change is detected, interrupt process and start over the worker with
#       up-to-date code.
#       In debug mode, if execution fails, stop with pdb. If user wants to continue, enter `c`
#       else, if the user make a modification, worker is reloaded and same task is retried.

STOPPING_TEMPLATE = '---\nStopping execution: {}\n---\n'
STARTING_TEMPLATE = '---\nStarting execution: {}\n---\n'


def convert_h_size(resources):
    d = dict()
    for key, value in resources.items():
        if key.endswith('util'):
            d[key] = '{} %'.format(value)
        else:
            d[key] = print_h_size(value)
    
    return d


def sigterm_handler(signal, frame):
    if sigterm_handler.triggered:
        return
    else:
        sigterm_handler.triggered = True

    raise mahler.core.utils.errors.SignalInterruptWorker("Task killed by SIGTERM")


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
    def __init__(self, offline_task, state, stdout, stderr, data, volume, **kwargs):
        self.offline_task = offline_task
        self.state = state
        self.stdout = stdout
        self.stderr = stderr
        self.data = data
        self.volume = volume 
        super(TaskProcess, self).__init__(**kwargs)

    def try_catch_run(self):
        try:
            data, volume = self.offline_task.run(self.state, stdout=self.stdout, stderr=self.stderr)
        finally:
            self.stdout.flush()
            self.stderr.flush()
        if data:
            self.data.update(data)
        if volume:
            self.volume.update(volume)


def heartbeat(registrar, task, slept):
    # TODO: Replace this with observers on database
    #       ex: tailable cursors in MongoDB
    #       https://docs.mongodb.com/manual/core/tailable-cursors/
    new_status = task.status
    if new_status.name == 'Suspended':
        raise mahler.core.utils.errors.SignalSuspend(
                'Task suspended remotely: {}'.format(new_status.message))
    elif task.status.name == 'Cancelled':
        raise mahler.core.utils.errors.SignalCancel(
                'Task cancelled remotely: {}'.format(new_status.message))

    if slept < task.heartbeat:
        return False

    try:
        registrar.update_status(task, mahler.core.status.Running('heartbeat'))
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

    return True


def run(registrar, task, state, stdout, stderr):

    manager = multiprocessing.managers.SyncManager()
    manager.start(mgr_init)

    stdout_queue = manager.Queue()
    stderr_queue = manager.Queue()

    stdout = StdQueue(stdout_queue, sys.stdout, mute=True,  # not logger.isEnabledFor(logging.INFO),
                      buffer_size=1)
    stderr = StdQueue(stderr_queue, sys.stderr, mute=True,  # not logger.isEnabledFor(logging.INFO),
                      buffer_size=1)

    data = manager.dict()
    volume = manager.dict()

    try:
        task_thread = TaskProcess(task.get_offline(), state, stdout, stderr, data, volume)
        task_thread.start()

        usage_monitor = ResourceUsageMonitor(task_thread.pid)

        start = time.time()
        while task_thread.is_alive():

            if not stdout_queue.empty():
                try:
                    registrar.update_stdout(task, stdout_queue.get(timeout=0.01))
                except queue.Empty as e:
                    pass

            if not stderr_queue.empty():
                try:
                    registrar.update_stderr(task, stderr_queue.get(timeout=0.01))
                except queue.Empty as e:
                    pass

            time.sleep(1)
            usage_monitor.update()

            if heartbeat(registrar, task, slept=time.time() - start):
                start = time.time()
                registrar.add_metric(task, 'usage', usage_monitor.get())
                usage_monitor.reset()

        # Will raise the error if any
        task_thread.join()

        data = dict()
        data.update(task_thread.data)

        volume = dict()
        volume.update(task_thread.volume)
    except Exception:
        task_thread.terminate()
        raise
    finally:
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

    except mahler.core.utils.errors.SignalInterruptWorker as e:
        status = mahler.core.status.Interrupted(str(e))
        raise

    except mahler.core.utils.errors.SignalInterruptTask as e:
        status = mahler.core.status.Interrupted(str(e))
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
        registrar.update_stdout(task, STOPPING_TEMPLATE.format(utcnow) + "\n")
        registrar.update_stderr(task, STOPPING_TEMPLATE.format(utcnow) + "\n")

    # TODO
    # NOTE: storage.write adds volume links to the registry.
    #       storage.write(registrar, volume)
    # registrar.set_volume(task, volume)

    return status


def main(worker_id, queued, completed, working_dir=None, max_failedover_attempts=3):

    with tmp_directory(working_dir):
        _main(worker_id, queued, completed, max_failedover_attempts=max_failedover_attempts)


def _main(worker_id, queued, completed, max_failedover_attempts=3):
    # TODO: Support config

    registrar = mahler.core.registrar.build()
    # tasks = list(technician.query(registrar))

    state = State()

    while True:
        logger.info('Worker {} waiting for a task.'.format(worker_id))
        task_id = queued.get(block=True)
        task = list(registrar.retrieve_tasks(id=task_id))[0]

        # Make sure the task is new or was run on the same cluster.
        if task.host and task.host['env']['clustername'] != fetch_host_name():
            logger.warning('Task {} was executed on a different host: {}'.format(
                task.id, task.host['env']['clustername']))
         
        try:
            registrar.reserve(task)
        except (ValueError, mahler.core.registrar.RaceCondition) as e:
            logger.info('Task {} reserved by concurrent worker'.format(task.id))
            completed.put(task_id)
            continue


        # set status of trial as running
        # Execute command from db
        # TODO: inside task.run, update the state
        # state.update(task=self, data=data, volume=volume)
        # TODO: inside task.run, convert data into measurements and volume into
        #       volume documents. Note that they are not registered yet.
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

        # TODO: Add heartbeat for reserved as well
        print('Executing task: {}'.format(task.id))
        assert task.status.name == 'Reserved'
        
        registrar.update_status(task, mahler.core.status.Running('start execution'))
        registrar.update_report(task.to_dict())
        try:
            new_status = execute(registrar, state, task)

        except KeyboardInterrupt as e:
            try:
                print('Execution of task {} suspended by user (KeyboardInterrupt)'.format(task.id))
                time.sleep(7)
            except KeyboardInterrupt as e:
                raise SystemExit()
            finally:
                new_status = mahler.core.status.Suspended('Suspended by user (KeyboardInterrupt)')
                assert task.status.name == 'Running'
                registrar.update_status(task, new_status)
                print('Execution of task {} interrupted'.format(task.id))
                print('New status: {}'.format(new_status))

            print()
            print('Now resuming worker {}...'.format(worker_id))
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

        except mahler.core.utils.errors.SignalInterruptWorker as e:
            new_status = mahler.core.status.Interrupted(str(e))
            assert task.status.name == 'Running'
            registrar.update_status(task, new_status)
            print('Execution of task {} interrupted'.format(task.id))
            print('New status: {}'.format(new_status))
            print('Now interrupting worker...')
            raise

        except mahler.core.utils.errors.SignalInterruptTask as e:
            new_status = mahler.core.status.Interrupted(str(e))
            assert task.status.name == 'Running'
            registrar.update_status(task, new_status)
            print('Execution of task {} interrupted'.format(task.id))
            print('New status: {}'.format(new_status))
            print('Attempting to resuming work with another task...')
            continue

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
            completed.put(task.id)
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


class Worker(cotyledon.Service):
    name = 'worker'

    def __init__(self, worker_id, queued, completed, working_dir, max_failedover_attempts):
        self.id = worker_id
        self.queued = queued
        self.completed = completed

        self.working_dir = working_dir
        self.max_failedover_attempts = max_failedover_attempts

    def run(self):
        main(self.id, self.queued, self.completed, self.working_dir, self.max_failedover_attempts)


class OldWorker(object):
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


class Dispatcher(cotyledon.Service):
    name = 'dispatcher'

    def __init__(self, worker_id, queued, completed, tags=tuple(), container=None, max_tasks=10e10,
                 depletion_patience=10, exhaust_wait_time=20):
        self.worker_id = worker_id
        self.running = []
        self.queued = queued
        self.completed = completed
        self.cached = {}

        self.tags = tags
        self.container = container

        self.max_tasks = max_tasks
        self.depletion_patience = depletion_patience
        self.exhaust_wait_time = exhaust_wait_time

        self.usage_buffer = 1.1

        self.tasks_completed = 0

    def get_task_available(self):
        projection = {'facility.resources': 1}  # TODO: Maybe we need more fields...
        task_docs = self.registrar.retrieve_tasks(
            tags=self.tags, container=self.container,
            status=mahler.core.status.Queued(''),
            limit=100, sort=[('registry.reported_on', 1)],
            host=[fetch_host_name(), None],
            _return_doc=True, _projection=projection)

        def compute_usage(task):
            return self.compute_max_metric(task), task
        
        tasks = []
        # TODO: Sort by priority
        # TODO: Pick tasks based on what is available in state (needs dependencies implementation)
        for i, task_doc in enumerate(task_docs):

            if task_doc['id'] not in self.cached:
                task = mahler.core.task.Task(
                    op=None, arguments=None, id=task_doc['id'], name=None,
                    resources=task_doc['facility']['resources'], registrar=self.registrar)
                task._metrics.refresh()
                tasks.append((self.compute_max_metric(task), task))

        for usage, task in sorted(tasks, reverse=True, key=lambda t: t[0]['gpu.memory']):
            yield usage, task

    def cache(self, task):
        self.cached[task.id] = task  # , self.compute_max_metric(task)

    def queue(self, task):
        self.cache(task)
        self.queued.put(task.id)

    def get_cached(self):
        while not self.completed.empty():
            try:
                task_id = self.completed.get(timeout=0.01)
            except queue.Empty:
                break

            self.cached.pop(task_id)
            self.tasks_completed += 1

        summed_resources = defaultdict(int)
        for task in self.cached.values():
            max_metrics = self.compute_max_metric(task)
            for name, value in max_metrics.items():
                summed_resources[name] += value

        return summed_resources

    def get_resources_available(self):
        usage = get_gpu_usage()
        # We assume we are alone on the gpu
        avail = flatten({
            'gpu': {
                'memory': usage['memory']['total'],
                'util': 100}})  # - usage['util']}})

        cached = self.get_cached()

        # TODO: Consider cpu mem and cpu_percent as well

        for name, value in cached.items():
            avail[name] -= value

        # TODO: test this for cases where estimation is to optimistic.
        avail['gpu.util'] = min(avail['gpu.util'], 100 - usage['util'])

        return avail

    def compute_max_metric(self, task):
        # NOTE: Don't trust metrics accumulated before 10 minutes (heartbeat=1min)
        #       A model may not be executed before that point.
        #       If no hints given by user in resources.usage, then assume the worst.
        #       This will limit crashes and we can recover efficiency when enough 
        #       metrics have been accumulated.
        WORST_GPU_MEMORY_USAGE = 10 * 2 ** 30  # 10GB
        WORST_GPU_UTIL = 90  # 90 %
        stats = {}
        if len(task.metrics['usage']) < 2:
            resources = flatten(task.resources)
            stats['gpu'] = {
                'memory': resources.get('usage.gpu.memory', WORST_GPU_MEMORY_USAGE),
                'util': resources.get('usage.gpu.util', WORST_GPU_UTIL)}
            # task.resources['usage']['cpu']['total']['cpu_percent']
            # task.resources['usage']['cpu']['total']['mem']
        else:
            memory = 0
            total_mem_used = 0
            util = 0
            for usage in task.metrics['usage']:
                if usage['gpu']['memory']['process']['max'] > memory:
                    memory = usage['gpu']['memory']['process']['max']
                    total_mem_used = usage['gpu']['memory']['used']['max']
                    # We use mean for util because reaching 100 is not critical.
                    # Well... it is simply impossible.
                    util = usage['gpu']['util']['mean']

            if total_mem_used == 0.:
                util = 0
            else:
                util = int(memory / total_mem_used * util + 0.5)

            stats['gpu'] = {
                'memory': memory,
                'util': util}

            # TODO: Include cpu stats for dispatching decisions.

        stats = flatten(stats)

        logger.debug('Expected usage for task {}:\n{}'.format(
            task.id, pprint.pformat(convert_h_size(stats))))

        return stats

    def have_enough(self, usage, resources):
        return all(usage[key] * self.usage_buffer < resources[key] for key in usage.keys())

    def increase_usage(self, resources, usage):
        summed_resources = copy.deepcopy(resources)
        for name, value in usage.items():
            summed_resources[name] -= value

        return summed_resources

    def run(self):

        self.registrar = mahler.core.registrar.build()

        exhaust_failures = 0
        while self.tasks_completed < self.max_tasks:

            try:
                if exhaust_failures >= self.depletion_patience and not self.cached:
                    print("Patience exhausted and no more task available. "
                          "No more workers. Leaving now...")
                    raise SystemExit(0)
                elif exhaust_failures >= self.depletion_patience:
                    print("Patience exhausted and no more task available. "
                          "Waiting for worker...")
                    time.sleep(self.exhaust_wait_time * 10)
                    exhaust_failures -= 1

                queued = False
                found_tasks = False
                resources_available = self.get_resources_available()
                logger.info('Attempting to fetch tasks')
                logger.debug(pprint.pformat(convert_h_size(resources_available)))
                # TODO: Sort by inverse resources usage, so that expensive are queued first
                for usage, task in self.get_task_available():
                    found_tasks = True 
                    exhaust_failures = 0
                    if not self.cached or self.have_enough(usage, resources_available):
                        self.queue(task)
                        queued = True
                        resources_available = self.increase_usage(resources_available, usage)
                        logger.info('Queued task {}.'.format(task.id))
                        logger.debug(pprint.pformat(convert_h_size(resources_available)))

                if not found_tasks:
                    logger.info('Dispatcher could not pick any task for execution.')
                    # NOTE: Maintainance could be done in parallel while a task is being executed.
                    exhaust_failures += 1
                    print("{} (UTC): No more task available, waiting {} seconds before "
                          "trying again. {} attemps remaining.".format(
                              datetime.datetime.utcnow(), self.exhaust_wait_time,
                              self.depletion_patience - exhaust_failures))
                    time.sleep(self.exhaust_wait_time)
                elif not queued:
                    print()
                    print("Waiting for free resources to queue additional tasks.")
                    print()
                    print('Available resources:')
                    pprint.pprint(convert_h_size(resources_available))
                    print()
                    print('Resources usage estimation:')
                    for task in self.cached.values():
                        print('Task {}: {}'.format(task.id, ' '.join(sorted(task.tags))))
                        pprint.pprint(convert_h_size(self.compute_max_metric(task)))
                    print()
                    time.sleep(60)

            except KeyboardInterrupt as e:
                try:
                    print('')
                    print('Execution will resume in 5 seconds.')
                    print('To stop the workers, press crtl-c again before the countdown.')
                    print()
                    for i in range(5, 0, -1):
                        print('{}...'.format(i))
                        time.sleep(1)
                    print()
                except KeyboardInterrupt as e:
                    print()
                    print('Now leaving workers...')
                    print()
                    raise SystemExit()

                print()
                print('Now resuming workers...')
                print()


class Manager(cotyledon.ServiceManager):
    def __init__(self, tags, container, max_tasks,
                 depletion_patience, exhaust_wait_time, 
                 working_dir, max_failedover_attempts):
        super(Manager, self).__init__()
        data_manager = multiprocessing.Manager()
        queued = data_manager.Queue()
        completed = data_manager.Queue()
        dispatcher = self.add(Dispatcher, args=(queued, completed, tags, container, max_tasks,
                                                depletion_patience, exhaust_wait_time))
        self.add(Worker, args=(queued, completed, working_dir, max_failedover_attempts), workers=5)

        self.max_failedover_attempts = 5

        def on_dead_worker(service_id, worker_id, exitcode):
            if service_id == dispatcher and exitcode == 0:
                self.shutdown()
            else:
                self.max_failedover_attempts -= 1
                print('A worker crashed. {} restart '
                      'remainings.'.format(self.max_failedover_attempts))
                if self.max_failedover_attempts <= 0:
                    self.shutdown()
        
        self.register_hooks(on_dead_worker=on_dead_worker)


def start(tags=tuple(), container=None, max_tasks=10e10,
          depletion_patience=10, exhaust_wait_time=20, working_dir=None, max_failedover_attempts=3,
          debug=False):

    # data_manager = multiprocessing.Manager()
    # queued = data_manager.Queue()
    # completed = data_manager.Queue()

    # main(0, queued, completed, working_dir=None, max_failedover_attempts=3)

    Manager(tags=tags, container=container, max_tasks=max_tasks,
            depletion_patience=depletion_patience, exhaust_wait_time=exhaust_wait_time, 
            working_dir=working_dir, max_failedover_attempts=max_failedover_attempts).run()
