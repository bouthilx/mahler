import multiprocessing as mp
import queue
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


class SignalRaceCondition(Exception):
    """If a task is being executed by two workers."""
    pass


class ExecutionError(Exception):
    pass


class RaceCondition(Exception):
    pass


class ProcessExceptionHandler(mp.Process):
    def __init__(self, **kwargs):
        self.queue = mp.Queue()
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
        try:
            exc = self.queue.get(timeout=0.1)
        except queue.Empty:
            return

        raise exc[1].with_traceback(exc[2])
