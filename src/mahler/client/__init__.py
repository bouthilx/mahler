import mahler.core.operator
import mahler.core.registrar
import mahler.core.status
from mahler.core.worker import Dispatcher


def operator(restore=None, resources=None, immutable=False, resumable=False):

    def call(f):

        operator = mahler.core.operator.Operator(
            f, restore=restore, resources=resources, immutable=immutable)

        return operator

    return call


class Client(object):
    CURRENT = 'current'

    def __init__(self, **kwargs):
        registrar = kwargs.get('registrar', dict(name='mongodb'))
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

    def get_task(self):
        if not Dispatcher.__refs__:
            return None

        return next(iter(Dispatcher.__refs__)).picked_task
        # if not os.environ['_MAHLER_TASK_ID']:
        #     return None

        # return self.find(id=os.environ['_MAHLER_TASK_ID'])

    def find(self, id=None, tags=tuple(), container=None, status=None):
        return self.registrar.retrieve_tasks(id=id, tags=tags, container=container, status=status)

    def add_tags(self, task, tags, message=''):
        return self.registrar.add_tags(task, tags, message)

    def remove_tags(self, task, tags):
        return

    def change_priority(self, task, priority):
        return

    def cancel(self, task, message):
        return self.registrar.update_status(task, mahler.core.status.Cancelled(message))

    def suspend(self, task, message):
        return self.registrar.update_status(task, mahler.core.status.Suspended(message))

    def switchover(self, task, message):
        return self.registrar.update_status(task, mahler.core.status.SwitchedOver(message))

    def acknowledge(self, task, message):
        return self.registrar.update_status(task, mahler.core.status.Acknowledged(message))

    def resume(self, task, message):
        try:
            return self.registrar.update_status(task, mahler.core.status.Queued(message))
        except ValueError:
            return self.registrar.update_status(task, mahler.core.status.OnHold(message))
