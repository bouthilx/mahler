import mahler.core.operator
import mahler.core.task
import mahler.core.registrar
import mahler.core.status
from mahler.core.worker import Dispatcher


def operator(restore=None, resources=None, immutable=False, resumable=False):

    def call(f):

        operator = mahler.core.operator.Operator(
            f, restore=restore, resources=resources, immutable=immutable)

        return operator

    return call


def get_current_task_id():
    if not Dispatcher.__refs__:
        return None

    task_id = next(iter(Dispatcher.__refs__)).picked_task

    return task_id


class Client(object):
    CURRENT = 'current'

    def __init__(self, **kwargs):
        registrar = kwargs.get('registrar', dict())
        if isinstance(registrar, mahler.core.registrar.Registrar):
            self.registrar = registrar
        elif isinstance(registrar, dict):
            self.registrar = mahler.core.registrar.build(**registrar)
        else:
            raise ValueError("Invalid registrar argument: {}".format(registrar))

    def register(self, task, priority=0, after=None, before=None, tags=tuple(), container=None):
        # TODO: Set dependencies
        task._container = container
        self.registrar.register_tasks([task])
        # self.change_priority(task, priority)
        self.add_tags(task, tags)
        self.registrar.update_report(task.to_dict())
        return task

    def get_current_task(self):
        task_id = get_current_task_id()

        if task_id is None:
            return None

        try:
            return next(iter(self.find(id=task_id)))
        except StopIteration:
            return None

    def find(self, id=None, tags=tuple(), container=None, status=None, host=None, _return_doc=False,
             _projection=None):
        return self.registrar.retrieve_tasks(
            id=id, tags=tags, container=container, status=status, host=host,
            _return_doc=_return_doc, _projection=_projection)

    def add_tags(self, task, tags, message=''):
        task = self._create_shallow_task(task)
        task._tags.refresh()
        rval = self.registrar.add_tags(task, tags, message)
        self.registrar.update_report(task.to_dict())
        return rval

    def add_metric(self, task, stats, metric_type='stat'):
        task = self._create_shallow_task(task)
        task._metrics.refresh()
        rval = self.registrar.add_metric(task, metric_type, stats)
        return rval

    def remove_tags(self, task, tags):
        return

    def change_priority(self, task, priority):
        return

    def _create_shallow_task(self, task):
        if not isinstance(task, mahler.core.task.Task):
            task = mahler.core.task.Task(op=None, arguments=None, id=task, name=None,
                                         registrar=self.registrar)

        return task

    def cancel(self, task, message):
        return self._update_status(task, mahler.core.status.Cancelled(message))

    def suspend(self, task, message):
        return self._update_status(task, mahler.core.status.Suspended(message))

    def switchover(self, task, message):
        return self._update_status(task, mahler.core.status.SwitchedOver(message))

    def acknowledge(self, task, message):
        return self._update_status(task, mahler.core.status.Acknowledged(message))

    def resume(self, task, message):
        try:
            return self._update_status(task, mahler.core.status.Queued(message))
        except ValueError:
            return self._update_status(task, mahler.core.status.OnHold(message))
        except BaseException as e:
            print(type(e), str(e))
            raise

    def _update_status(self, task, new_status):
        task = self._create_shallow_task(task)
        task._status.refresh()
        rval = self.registrar.update_status(task, new_status)
        self.registrar.update_report(task.to_dict())
        return rval

    def close(self):
        self.registrar.close()
