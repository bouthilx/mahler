import mahler.core.operator
import mahler.core.registrar
from mahler.core.worker import Dispatcher


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

    def operator(self, *args, **kwargs):
        return mahler.core.operator.wrap(*args, **kwargs)

    def register(self, task, priority=0, after=None, before=None, tags=tuple(), container=None):
        # TODO: Set dependencies
        task._container = container
        self.registrar.register_tasks([task])
        # self.change_priority(task, priority)
        self.add_tags(task, tags)
        # self.registrar.update_report(task)

    def find(self, tags=tuple(), status=None):
        return self.registrar.retrieve_tasks(tags=tags, status=status)

    def get_task(self):
        if not Dispatcher.__refs__:
            return None

        return next(iter(Dispatcher.__refs__)).picked_task
        # if not os.environ['_MAHLER_TASK_ID']:
        #     return None

        # return self.find(id=os.environ['_MAHLER_TASK_ID'])

    def add_tags(self, task, tags, message=''):
        return self.registrar.add_tags(task, tags, message)

    def remove_tags(self, task, tags):
        return

    def change_priority(self, task, priority):
        return

    def suspend(self, task):
        return

    def switchover(self, task):
        return

    def acknowledge(self, task):
        return
