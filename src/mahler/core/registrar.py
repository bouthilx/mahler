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
    
    def retrieve_tasks(self, id=None, tags=tuple(), container=None, status=None, limit=None):
        raise NotImplemented()

    def add_event(self, event_type, event_object):
        raise NotImplemented()

    def retrieve_events(self, event_type, task):
        raise NotImplemented()

    def set_output(self, task, output):
        raise NotImplemented()

    def set_volume(self, task, volume):
        raise NotImplemented()

    def retrieve_output(self, task):
        raise NotImplemented()

    def retrieve_volume(self, task):
        raise NotImplemented()


class Registrar(object):
    def __init__(self, db):
        self._db = db

    @classmethod
    def create_event(cls, event_type, task_id, item, timestamp=None, creator=None, ref_id=None):
        if timestamp is None:
            timestamp = datetime.datetime.utcnow()

        if not isinstance(timestamp, datetime.datetime):
            raise TypeError(
                "timestamp must be of type datetime.datetime, not '{}'".format(
                    type(timestamp)))

        inc_id = ref_id + 1 if ref_id else 1

        event = {
            'task_id': task_id,
            'inc_id': inc_id,
            'key': "{}.{}".format(str(task_id), inc_id),
            'item': item,
            'type': event_type,
            # 'creation_timestamp': creation_timestamp,
            'runtime_timestamp': timestamp
        }

        return event

    # NOTE: Only retrieves the storage specification, not the actual data.
    def retrieve_volumes(self, task):
        raise NotImplemented()

    def maintain(self, tags, container=None, limit=100):
        """
            0. Update reports
            1. check for lost tasks that can be failed-over
            2. check for interrupted that can be re-queued
            3. check for failed-over that can be re-queued
            4. check for switched-over that can be re-queued
            5. check for onhold that can be queued
        """
        updated = self.maintain_reports(tags, container, limit=limit)
        logger.info('Updated {} reports'.format(updated))
        updated = self.maintain_lost(tags, container, limit=limit)
        updated += self.maintain_to_queue(tags, container, limit=limit - updated)
        updated += self.maintain_onhold(tags, container, limit=limit - updated)

        return updated

    def maintain_reports(self, tags, container=None, limit=100):
        updated = 0
        volatile_status = [mahler.core.status.Queued(''),
                           mahler.core.status.Reserved(''),
                           mahler.core.status.Running('')]
        queueable_status = [mahler.core.status.OnHold(''),
                            mahler.core.status.Interrupted(''),
                            mahler.core.status.FailedOver(''),
                            mahler.core.status.SwitchedOver('')]
        mutable_status = [mahler.core.status.Suspended(''),
                          mahler.core.status.Acknowledged(''),
                          mahler.core.status.Cancelled(''),
                          mahler.core.status.Broken('')]

        def is_outdated(task, task_document):
            return ((task.status.name != task_document['registry']['status']) or
                    (set(task.tags) != set(task_document['registry']['tags'])))

        projection = {'registry.status': 1, 'registry.tags': 1}

        for status_family in [volatile_status, queueable_status, mutable_status]:

            limit -= updated
            
            task_iterator = self.retrieve_tasks(
                tags=tags, container=container, status=status_family, limit=limit,
                _return_doc=True, _projection=projection)

            for task_document in task_iterator:

                task = Task(op=None, arguments=None, id=task_document['id'],
                            name=None, registrar=self)

                if is_outdated(task, task_document):
                    self.update_report(task)
                    updated += 1

        return updated

    def maintain_to_queue(self, tags, container=None, limit=100):
        queueable_status = [mahler.core.status.OnHold(''),
                            mahler.core.status.Interrupted(''),
                            mahler.core.status.FailedOver(''),
                            mahler.core.status.SwitchedOver('')]

        status_names = [status.name for status in queueable_status]

        projection = {'registry.status': 1}

        task_iterator = self.retrieve_tasks(
            tags=tags, container=container, status=queueable_status, limit=limit,
            _return_doc=True, _projection=projection)

        updated = 0
        for task_document in task_iterator:
            task = Task(op=None, arguments=None, id=task_document['id'],
                        name=None, registrar=self)

            if task.status.name not in status_names:
                self.update_report(task)
                continue

            try:
                self.update_status(
                    task,
                    mahler.core.status.Queued(
                        're-queue {} task'.format(task_document['registry']['status'])))
                self.update_report(task)
            except (ValueError, RaceCondition) as e:
                logger.debug('Task {} status changed concurrently'.format(task.id))
                continue

            updated += 1

        return updated

    def maintain_onhold(self, tags, container=None, limit=100):
        onhold_status = mahler.core.status.OnHold('')

        updated = 0

        # TODO: Implement dependencies and test
        projection = {'registry.status': 1}  # , 'bounds.dependencies': 1}

        task_iterator = self.retrieve_tasks(
            tags=tags, container=container, status=onhold_status, limit=limit,
            _return_doc=True, _projection=projection)

        for task_document in task_iterator:
            task = Task(op=None, arguments=None, id=task_document['id'],
                        name=None, registrar=self)
            if task.status.name != onhold_status.name:
                self.update_report(task)
                continue

            # TODO: Implement dependencies and test
            # task._dependencies = task_document['bounds.dependencies']
            try:
                self.update_status(task, mahler.core.status.Queued('dependencies met'))
                self.update_report(task)
            except (ValueError, RaceCondition) as e:
                logger.debug('Task {} status changed concurrently'.format(task.id))
                continue

            updated += 1

        return updated

    def maintain_lost(self, tags, container=None, limit=100):
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
            self.update_report(task, upsert=True)

    def update_report(self, task, upsert=False):
        self._db.update_report(task, upsert)

    def retrieve_tasks(self, id=None, tags=tuple(), container=None, status=None, limit=None,
                       use_report=True, _return_doc=False, _projection=None):
        """
        """
        task_iterator = self._db.retrieve_tasks(id, tags, container, status, limit=limit,
                                                use_report=use_report, projection=_projection)
        for task_document in task_iterator:
            if _return_doc:
                yield task_document
                continue

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
            'set', task.id, status.to_dict(), ref_id=current_status.id if current_status else None)
        # Fail if another process changed the status meanwhile
        # if ref_id + 1 exist:
        #     raise RaceCondition(Current status was not the most recent one)
        db_operation = self._db.add_event('status', event)
        task._status.history.append(event)


    def retrieve_status(self, task):
        return self._db.retrieve_events('status', task)

    # def update_host(self, task, host):
    #     # TODO: Create timestamp
    #     assert mahler.status.is_reserved(task, local=True)
    #     db.table('tasks.host').insert()

    def update_stdout(self, task, stdout):
        # TODO: Fetch host and PID and set
        # assert mahler.core.status.is_running(task, local=True)
        ref_id = task._stdout.history[-1]['inc_id'] if task._stdout.history else None
        event = self.create_event('add', task.id, stdout, ref_id=ref_id)
        # Fail if another process changed the status meanwhile
        # if ref_id + 1 exist:
        #     raise RaceCondition(Current status was not the most recent one)
        db_operation = self._db.add_event('stdout', event)
        task._stdout.history.append(event)

    def update_stderr(self, task, stderr):
        # TODO: Fetch host and PID and set
        # assert mahler.core.status.is_running(task, local=True)
        ref_id = task._stderr.history[-1]['inc_id'] if task._stderr.history else None
        event = self.create_event('add', task.id, stderr, ref_id=ref_id)
        # Fail if another process changed the status meanwhile
        # if ref_id + 1 exist:
        #     raise RaceCondition(Current status was not the most recent one)
        db_operation = self._db.add_event('stderr', event)
        task._stderr.history.append(event)

    def add_tags(self, task, tags, message=''):
        for tag in tags:
            if tag in task.tags:
                logger.info("tag {} already belongs to task {}, ignoring it.".format(
                    tag, task.id))
                continue

            ref_id = task._tags.history[-1]['inc_id'] if task.tags else None
            event = self.create_event('add', task.id, dict(tag=tag, message=message), ref_id=ref_id)
            self._db.add_event('tags', event)
            task._tags.history.append(event)

    def remove_tags(self, task, tags, message=''):
        for tag in tags:
            if tag not in task.tags:
                raise RuntimeError(
                    "Cannot remove tag {}, it does not belong to task {}".format(
                        tag, task.id))

            ref_id = task._tags.history[-1]['inc_id'] if task.tags else None
            event = self.create_event('remove', task.id, dict(tag=tag, message=message),
                                      ref_id=ref_id)
            self._db.add_event('tags', event)
            task._tags.history.append(event)

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

    def retrieve_volume(self, task):
        return self._db.retrieve_volume(task)


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
