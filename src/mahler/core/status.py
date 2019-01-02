"""
new -> onhold
    -> queued
    -> suspend
    
# There is unmet dependencies so it cannot be reserved for execution yet.
onhold -> queued
       -> suspend

# Dependencies have been met and task can be reserved for execution
queued -> reserved
       -> suspend

# Task reserved by a process for execution
reserved -> running
         -> suspend

# Task is being executed
running -> broken
        -> interrupted
        -> suspended
        -> completed

# Execution of the task failed
broken -> failover
       -> switchover
       -> acknowledged

# Interrupted by system, should be re-executed whenever possible
interrupted -> reserved
            -> suspend

# Stopped intentionally by client, should not be re-executed
suspended -> queued

# System detects something was wrong, should be re-executed
failover -> reserved

# User acknowledge broken trial and request re-execution
switchover -> reserved

# User acknowledge broken trial and should *not* be re-executed
acknowledged -|

# Task completed
completed -|
"""
import logging
import pkg_resources


logger = logging.getLogger('mahler.core.status')


class Status(object):
    heartbeat = False

    def __init__(self, message, id=None):
        self.id = id
        self.message = message

    @property
    def name(self):
        return self.__class__.__name__

    def can_follow(self, status):
        return any(isinstance(status, s) for s in self.follows)

    def to_dict(self):
        return dict(
            name=self.name,
            message=self.message)

    def __repr__(self):
        return '{}("{}")'.format(self.name, self.message)


class OnHold(Status):
    """
    """
    @property
    def follows(self):
        return [None, Suspended]

    def can_follow(self, status):
        return isinstance(status, Suspended) or status is None


class Queued(Status):
    """
    """
    @property
    def follows(self):
        return [OnHold, Interrupted, FailedOver, SwitchedOver]


class Reserved(Status):
    """
    """
    heartbeat = True

    @property
    def follows(self):
        return [Queued, Reserved]


class Running(Status):
    """
    """
    heartbeat = True

    @property
    def follows(self):
        return [Reserved, Running]


class Completed(Status):
    """
    """
    @property
    def follows(self):
        return [Running]


class Interrupted(Status):
    """
    """
    @property
    def follows(self):
        return [Reserved, Running]


class Suspended(Status):
    """
    """
    @property
    def follows(self):
        return [OnHold, Reserved, Running]


class Broken(Status):
    """
    """
    @property
    def follows(self):
        return [Running]


class FailedOver(Status):
    """
    """
    @property
    def follows(self):
        return [Reserved, Broken]


class Acknowledged(Status):
    """
    """
    @property
    def follows(self):
        return [Broken]


class SwitchedOver(Status):
    """
    """
    @property
    def follows(self):
        return [Acknowledged]


class Cancelled(Status):
    """
    """
    @property
    def follows(self):
        return [OnHold, Reserved, Running]


def is_running(task, local=False):
    if local:
        # TODO: Fetch task host and PID and compare with current
        logger.warning('`mahler.core.status.is_running(task, local=True)` is not Implemented')

    return isinstance(task.status, Running)


def build(name, *args, **kwargs):

    plugins = {
        entry_point.name: entry_point.load()
        for entry_point
        in pkg_resources.iter_entry_points('Status')
    }

    if name in plugins:
        return plugins[name](*args, **kwargs)

    subclasses = dict((subclass.__name__, subclass) for subclass in Status.__subclasses__())
    return subclasses[name](*args, **kwargs)
