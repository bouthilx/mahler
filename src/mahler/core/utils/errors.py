import multiprocessing as mp
import sys

from tblib import pickling_support


pickling_support.install()


class SignalCancel(Exception):
    pass


class SignalInterruptTask(Exception):
    pass


class SignalInterruptWorker(Exception):
    pass


class SignalSuspend(Exception):
    pass


class ExecutionError(Exception):
    pass


class RaceCondition(Exception):
    pass


class ProcessExceptionHandler(mp.Process):
    def __init__(self, **kwargs):
        self.queue = mp.Queue(5)
        super(ProcessExceptionHandler, self).__init__(**kwargs)

    def try_catch_run(self):
        pass

    def run(self):
        try:
            self.try_catch_run()
        except:
            self.queue.put(sys.exc_info())

    def join(self):
        super(ProcessExceptionHandler, self).join()
        if not self.queue.empty():
            exc = self.queue.get(block=False)
            raise exc[1].with_traceback(exc[2])
