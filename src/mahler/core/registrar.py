# -*- coding: utf-8 -*-
"""
:mod:`mahler.core.registrar` -- Abstract class for registrar plugins
====================================================================

.. module:: init_only
   :platform: Unix
   :synopsis: Defines the abstract class for registrar plugins
"""
import datetime
import logging
import pkg_resources
import pprint

from mahler.core import config
from mahler.core.task import Task
from mahler.core.operator import Operator
import mahler.core.status


logger = logging.getLogger('mahler.core.registrar')


class RegistrarDB(object):
    def __init__(self):
        pass

    def register_task(self, task):
        raise NotImplemented()
    
    def retrieve_tasks(self, id=None, tags=tuple(), container=None, status=None):
        raise NotImplemented()

    def add_event(self, event_type, event_object):
        raise NotImplemented()

    def retrieve_events(self, event_type, task):
        raise NotImplemented()

    def set_output(self, task, output):
        raise NotImplemented()

    def set_volume(self, task, volume):
        raise NotImplemented()


class Registrar(object):
    def __init__(self, db):
        self._db = db

    @classmethod
    def create_event(cls, event_type, item, timestamp=None, creator=None, ref_id=None):
        creation_timestamp = datetime.datetime.utcnow()
        runtime_timestamp = timestamp if timestamp else creation_timestamp

        if not isinstance(runtime_timestamp, datetime.datetime):
            raise TypeError(
                "Timestamp must be of type datetime.datetime, not '{}'".format(
                    type(runtime_timestamp)))

        event = {
            'id': ref_id + 1 if ref_id else 1,
            'item': item,
            'type': event_type,
            'creation_timestamp': creation_timestamp,
            'runtime_timestamp': runtime_timestamp
        }

        return event

    # NOTE: Only retrieves the storage specification, not the actual data.
    def retrieve_volumes(self, task):
        raise NotImplemented()

    def maintain(self, tags, container=None, limit=10e10):
        """
            1. check for onhold that can be queued
            2. check for interrupted that can be queued
            3. check for failed-over that can be queued
            4. check for switched-over that can be queued
            5. check for lost tasks that can be failed-over
        """
        updated = self.maintain_onhold(tags, container, limit=limit)
        updated += self.maintain_to_queue(tags, container, limit=limit - updated)
        updated += self.maintain_lost(tags, container, limit=limit - updated)

        return updated

    def maintain_onhold(self, tags, container=None, limit=10e10):
        updated = 0
        for task in self.retrieve_tasks(tags=tags, container=container,
                                        status=mahler.core.status.OnHold('')):
            if updated >= limit:
                return updated

            # TODO: Implement dependecies and test
            try:
                self.update_status(task, mahler.core.status.Queued('dependencies met'))
            except (ValueError, RaceCondition) as e:
                logger.debug('Task {} status changed concurrently'.format(task.id))
                continue

            updated += 1

        return updated

    def maintain_to_queue(self, tags, container=None, limit=10e10):
        statuses = [mahler.core.status.Interrupted(''),
                    mahler.core.status.FailedOver(''),
                    mahler.core.status.SwitchedOver('')]
        updated = 0
        for status in statuses:
            for task in self.retrieve_tasks(tags=tags, container=container, status=status):
                if updated >= limit:
                    return updated

                try:
                    self.update_status(
                        task, mahler.core.status.Queued('re-queue {} task'.format(status.name)))
                except (ValueError, RaceCondition) as e:
                    logger.debug('Task {} status changed concurrently'.format(task.id))
                    continue

                updated += 1

        return updated

    def maintain_lost(self, tags, container=None, limit=10e10):
        return 0

    def register_tasks(self, tasks, message='new task'):
        """

        Notes
        -----
            There is no need to register a task again, only update moving parts
        """
        for task in tasks:
            assert task.id is None
            self._db.register_task(task)
            task._registrar = self
            self.update_status(task, mahler.core.status.OnHold(message))
            self.add_tags(task, [task.name])

    # TODO: Register reports

    def retrieve_tasks(self, id=None, tags=tuple(), container=None, status=None):
        """
        """
        for task_document in self._db.retrieve_tasks(id, tags, container, status):
            operator = Operator(**task_document['op'])
            task = Task(operator, arguments=task_document['arguments'], id=task_document['id'],
                        name=task_document['name'], registrar=self)
            task._container = task_document['registry']['container']
            yield task

    def reserve(self, task, message="for execution"):
        """
        """
        # TODO: Fetch host and PID and set
        return self.update_status(task, mahler.core.status.Reserved(message))

    # def update_priority(self, task, priority):
    #     # TODO: Create timestamp
    #     db_operation = self._db.add_event('priority', event)
    #     if self.retrieve_status('status', task)[-1] in ['reserved', 'running', 'completed']
    #         db_operation.rollback()
    #         raise RaceCondition("Cannot change priority")
    #     db.table('tasks.priorities').insert()

    def update_status(self, task, status):
        # TODO: Create timestamp
        current_status = task.status
        if current_status is None and not isinstance(status, mahler.core.status.OnHold):
            raise ValueError("Task was not fully initialized yet, cannot set status: {}".format(
                                 status))

        if not status.can_follow(current_status):
            raise ValueError("Task with status {} cannot be set to {}".format(
                                 current_status, status))

        event = self.create_event(
            'set', status.to_dict(), ref_id=current_status.id if current_status else None)
        event['task_id'] = task.id
        # TODO: What is creator?
        event['creator_id'] = task.id
        # Fail if another process changed the status meanwhile
        # if ref_id + 1 exist:
        #     raise RaceCondition(Current status was not the most recent one)
        db_operation = self._db.add_event('status', event)

    def retrieve_status(self, task):
        return self._db.retrieve_events('status', task)

    # def update_host(self, task, host):
    #     # TODO: Create timestamp
    #     assert mahler.status.is_reserved(task, local=True)
    #     db.table('tasks.host').insert()

    def update_stdout(self, task, stdout):
        # TODO: Fetch host and PID and set
        assert mahler.core.status.is_running(task, local=True)
        ref_id = task._stdout.history[-1]['id'] if task._stdout.history else None
        event = self.create_event('add', stdout, ref_id=ref_id)
        event['task_id'] = task.id
        # TODO: What is creator?
        event['creator_id'] = task.id
        # Fail if another process changed the status meanwhile
        # if ref_id + 1 exist:
        #     raise RaceCondition(Current status was not the most recent one)
        db_operation = self._db.add_event('stdout', event)

    def update_stderr(self, task, stderr):
        # TODO: Fetch host and PID and set
        assert mahler.core.status.is_running(task, local=True)
        ref_id = task._stderr.history[-1]['id'] if task._stderr.history else None
        event = self.create_event('add', stderr, ref_id=ref_id)
        event['task_id'] = task.id
        # TODO: What is creator?
        event['creator_id'] = task.id
        # Fail if another process changed the status meanwhile
        # if ref_id + 1 exist:
        #     raise RaceCondition(Current status was not the most recent one)
        db_operation = self._db.add_event('stderr', event)

    def add_tags(self, task, tags, message=''):
        for tag in tags:
            if tag in task.tags:
                logger.info("tag {} already belongs to task {}, ignoring it.".format(
                    tag, task.id))
                continue

            ref_id = task._tags.history[-1]['id'] if task.tags else None
            ref_id = self._add_tag(task, tag, message, ref_id=ref_id)
            # task._tags.refresh()

    def _add_tag(self, task, tag, message, ref_id=None):
        event = self.create_event('add', dict(tag=tag, message=message), ref_id=ref_id)
        event['task_id'] = task.id
        event['creator_id'] = task.id
        self._db.add_event('tags', event)

    def remove_tags(self, task, tags, message=''):
        for tag in tags:
            if tag not in task.tags:
                raise RuntimeError(
                    "Cannot remove tag {}, it does not belong to task {}".format(
                        tag, task.id))

            event = self.create_event('remove', dict(tag=tag, message=message))
            event['task_id'] = task.id
            event['creator_id'] = task.id
            self._db.add_event('tags', event)

    def retrieve_tags(self, task):
        return self._db.retrieve_events('tags', task)

    def set_output(self, task, output):
        # TODO: Check status as well as process ID
        assert mahler.core.status.is_running(task, local=True)
        # TODO: Implement...
        self._db.set_output(task, output)

    def set_volume(self, task, volume):
        # TODO: Check status as well as process ID
        assert mahler.core.status.is_running(task, local=True)
        self._db.set_volume(task, volume)

    def retrieve_output(self, task):
        return self._db.retrieve_output(task)


def build(**kwargs):

    plugins = {
        entry_point.name: entry_point
        for entry_point
        in pkg_resources.iter_entry_points('RegistryDB')
    }

    registry_type = kwargs.get('type', mahler.core.config.registry.type)

    load_config(type=registry_type)

    if registry_type is None:
        raise ValueError(
            "No type provided to build a registrar:\n{}".format(pprint.pformat(kwargs)))

    config = mahler.core.config.registry[registry_type].to_dict()
    config.update(kwargs)
    
    plugin = plugins[registry_type].load()
    registrar_db = plugin.build(**config)

    return Registrar(registrar_db)


def load_config(**kwargs):
    """
    Note
    ----
        A plugin may only define and modify its own sub-configuration.
        It should not modify the configuration of the core or other plugins
    """

    plugins = {
        entry_point.name: entry_point
        for entry_point
        in pkg_resources.iter_entry_points('RegistryDB')
    }

    registry_type = kwargs.get('type', mahler.core.config.registry.type)

    if registry_type is None or registry_type == 'None':
        raise ValueError(
            "No type provided to build a registrar:\n{}".format(pprint.pformat(kwargs)))

    plugin = plugins[registry_type].load()

    mahler.core.config.registry[registry_type] = plugin.define_config()
    plugin.parse_config_files(mahler.core.config.registry[registry_type])


class RaceCondition(Exception):
    pass
