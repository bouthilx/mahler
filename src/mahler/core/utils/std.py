import contextlib
import logging
import pdb
import sys


@contextlib.contextmanager
def stdredirect(stdout, stderr):
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    root_logger = logging.getLogger()
    old_stdout_logger_handler = root_logger.handlers[0]
    old_set_trace = pdb.set_trace
    new_stdout_logger_handler = logging.StreamHandler(stdout)

    def redirect():
        sys.stdout = stdout
        sys.stderr = stderr
        pdb.set_trace = set_trace
        root_logger.addHandler(new_stdout_logger_handler)
        root_logger.removeHandler(old_stdout_logger_handler)

    def undo():
        sys.stdout = old_stdout
        sys.stderr = old_stderr
        pdb.set_trace = old_set_trace
        root_logger.addHandler(old_stdout_logger_handler)
        root_logger.removeHandler(new_stdout_logger_handler)

    def set_trace(*, header=None):
        undo()

        pdb_cmd = pdb.Pdb(stdout=sys.__stdout__)
        if header is not None:
            pdb_cmd.message(header)
        pdb_cmd.set_trace(sys._getframe().f_back)

        redirect()

    redirect()
    try:
        yield None
    finally:
        undo()
