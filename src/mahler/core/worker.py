import contextlib
import datetime
import io
import logging
import os
import time
import sys
import traceback
import weakref

import mahler.core.registrar
import mahler.core.utils.errors

logger = logging.getLogger('mahler.core.worker')


STOPPING_TEMPLATE = '---\nStopping execution: {}\n---\n'
STARTING_TEMPLATE = '---\nStarting execution: {}\n---\n'

def sigterm_handler():
    if sigterm_handler.triggered:
        return
    else:
        sigterm_handler.triggered = True

    raise mahler.core.utils.errors.SignalInterrupt("Task killed by SIGTERM")


sigterm_handler.triggered = False


@contextlib.contextmanager
def tmp_directory(working_dir=None):
    curdir= os.getcwd()
    try:
        if working_dir is not None:
            os.chdir(working_dir)
        yield
    finally:
        os.chdir(curdir)


def execute(registrar, state, task):
    stdout = io.StringIO()
    stderr = io.StringIO()

    utcnow = datetime.datetime.utcnow()
    stdout.write(STARTING_TEMPLATE.format(utcnow))
    stderr.write(STARTING_TEMPLATE.format(utcnow))

    try:
        logger.info('Starting execution')
        data, volume = task.run(state, stdout=stdout, stderr=stderr)
        logger.info('Execution completed')
        logger.debug('Saving output')
        registrar.set_output(task, data)
        logger.debug('Output saved')
        status = mahler.core.status.Completed('')

    except mahler.core.utils.errors.SignalCancel as e:
        status = mahler.core.status.Cancelled('Cancelled by user')

    except mahler.core.utils.errors.SignalInterrupt as e:
        status = mahler.core.status.Interrupted('Interrupted by system (SIGTERM)')

    except KeyboardInterrupt as e:
        status = mahler.core.status.Suspended('Suspended by user (KeyboardInterrupt)')

    except BaseException as e:
        # broken
        message = "execution error: {}".format(e)
        logger.info(message)
        stderr.write(traceback.format_exc())
        status = mahler.core.status.Broken(str(e))
        # status = mahler.core.status.FailedOver(str(e))

    finally:
        utcnow = datetime.datetime.utcnow()
        stdout.write(STOPPING_TEMPLATE.format(utcnow))
        stderr.write(STOPPING_TEMPLATE.format(utcnow))
        logger.debug(stdout.getvalue())
        logger.debug(stderr.getvalue())
        if stdout.getvalue():
            task._stdout.refresh()
            registrar.update_stdout(task, stdout.getvalue())
        if stderr.getvalue():
            task._stderr.refresh()
            registrar.update_stderr(task, stderr.getvalue())

    # TODO
    # NOTE: storage.write adds volume links to the registry.
    #       storage.write(registrar, volume)
    # registrar.set_volume(task, volume)

    return status


def main(tags=tuple(), container=None, working_dir=None, max_tasks=10e10, depletion_patience=12,
         exhaust_wait_time=10, max_maintain=10, **kwargs):

    with tmp_directory(working_dir):
        _main(tags=tags, container=container, max_tasks=max_tasks,
              depletion_patience=depletion_patience, exhaust_wait_time=exhaust_wait_time,
              max_maintain=max_maintain)


def _main(tags=tuple(), container=None, max_tasks=10e10, depletion_patience=12,
          exhaust_wait_time=10, max_maintain=10):
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
            task = dispatcher.pick(tags, container, state)  # tasks, registrar)
            exhaust_failures = 0
        except RuntimeError:
            logger.info('Dispatcher could not pick any task for execution.')
            # NOTE: Maintainance could be done in parallel while a task is being executed.
            updated = registrar.maintain(tags, container, limit=max_maintain)
            if updated:
                logger.info("{} task status updated and now queued".format(updated))
                continue

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
        try:
            new_status = execute(registrar, state, task)
        except BaseException as e:
            message = "system error: {}".format(e)
            registrar.update_status(task, mahler.core.status.Broken(message))
            registrar.update_status(task, mahler.core.status.FailedOver('system error'))
            raise

        registrar.update_status(task, new_status)
        print('Executing of task {} stopped'.format(task.id))
        print('New status: {}'.format(new_status))

        if isinstance(new_status, mahler.core.status.Broken):
            broke_n_times = sum(int(event['item']['name'] == new_status.name)
                                for event in task._status.history)
            registrar.update_status(
                task, mahler.core.status.FailedOver('Broke {} times'.format(broke_n_times)))

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
        self.registrar = registrar
        self._picked_task = None

        self.__refs__.add(self)

    @property
    def picked_task(self):
        return self._picked_task

    def pick(self, tags, container, state):
        tasks = self.registrar.retrieve_tasks(
            tags=tags, container=container,
            status=mahler.core.status.Queued(''))
        # TODO: Sort by priority
        # TODO: Pick tasks based on what is available in state (needs dependencies implementation)
        for task in tasks:
            try:
                self.registrar.reserve(task)
                self._picked_task = task
                return task
            except (ValueError, mahler.core.registrar.RaceCondition) as e:
                logger.info('Task {} reserved by concurrent worker'.format(task.id))
                continue

        raise RuntimeError("No task available")
