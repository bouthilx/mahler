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
from mahler.core.utils.errors import RaceCondition
from mahler.core.utils.host import fetch_host_info
import mahler.core.status


logger = logging.getLogger('mahler.core.registrar')


TMP_BROKEN = [
    "GPU not available",
    "SSL handshake failed",
    "[Errno 111] Connection refused"
]


MIN_TIME_WAITING = 5


class RegistrarDB(object):
    def __init__(self):
        pass

    def register_task(self, task):
        raise NotImplemented()
    
    def retrieve_tasks(self, id=None, tags=tuple(), container=None, status=None, limit=None,
                       sort=None):
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

        # TODO: Maintain reports for mismatch on output and volume?

        return updated

    def maintain_unreported(self, limit=100):

        projection = {'arguments': 1, 'name': 1, 'id': 1, 'op': 1,
                      'registry': 1}

        # Querying from immutable cores
        for task_document in self.retrieve_tasks(_return_doc=True, _projection=projection):
            # First make sure the task was registered long enough that it is worth looking for a
            # report
            created_on = task_document['id'].generation_time
            now = datetime.datetime.now(datetime.timezone.utc)
            time_since_creation = (now - created_on).total_seconds()
            if time_since_creation < MIN_TIME_WAITING:
                continue

            # Looking for a report
            report_iterator = self.retrieve_tasks(id=task_document['id'], _return_doc=True,
                                                  _projection={'id': 1})
            if sum(1 for _ in report_iterator) < 1:
                logger.info('Adding missing report for {}'.format(task_document['id']))
                operator = Operator(**task_document['op'])
                task = Task(operator, arguments=task_document['arguments'], id=task_document['id'],
                            name=task_document['name'], registrar=self)
                task._container = task_document['registry']['container']

                self.update_report(task.to_dict(), upsert=True)

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
            # First make sure the report was updated long enough that it is worth looking
            # at the attributes
            updated_on = task_document['registry']['reported_on'].generation_time
            now = datetime.datetime.now(datetime.timezone.utc)
            time_since_update = (now - updated_on).total_seconds()
            if time_since_update < MIN_TIME_WAITING:
                return False

            return ((task.status.name != task_document['registry']['status']) or
                    (set(task.tags) != set(task_document['registry']['tags'])) or
                    (task_document['output'] != task.output))

        projection = {'registry.status': 1, 'registry.tags': 1, 'output': 1,
                      'registry.reported_on': 1}

        for status_family in [volatile_status, queueable_status, mutable_status]:

            if limit:
                limit -= updated
            
            task_iterator = self.retrieve_tasks(
                tags=tags, container=container, status=status_family, limit=limit,
                _return_doc=True, _projection=projection)

            for task_document in task_iterator:

                task = Task(op=None, arguments=None, id=task_document['id'],
                            name=None, registrar=self)

                if is_outdated(task, task_document):
                    self.update_report(task.to_dict(), update_output=True)
                    updated += 1

        return updated

    def maintain_to_queue(self, tags, container=None, limit=100):
        queueable_status = [mahler.core.status.OnHold(''),
                            mahler.core.status.Interrupted(''),
                            mahler.core.status.FailedOver(''),
                            mahler.core.status.SwitchedOver('')]

        status_names = [status.name for status in queueable_status]

        projection = {'registry.status': 1, 'registry.reported_on': 1}

        task_iterator = self.retrieve_tasks(
            tags=tags, container=container, status=queueable_status, limit=limit,
            _return_doc=True, _projection=projection)

        updated = 0
        for task_document in task_iterator:
            task = Task(op=None, arguments=None, id=task_document['id'],
                        name=None, registrar=self)

            # First make sure the task was updated since long enough that it is worth trying
            # to update it now.
            updated_on = task_document['registry']['reported_on'].generation_time
            now = datetime.datetime.now(datetime.timezone.utc)
            time_since_update = (now - updated_on).total_seconds()
            if time_since_update < MIN_TIME_WAITING:
                continue

            if task.status.name not in status_names:
                # Report is outdated, leave it to maintain_report to update it.
                continue

            try:
                if task.output:
                    self.update_status(
                        task,
                        mahler.core.status.Completed('Task was completed and have output.'),
                        _force=True)
                else:
                    self.update_status(
                        task,
                        mahler.core.status.Queued(
                            're-queue {} task'.format(task_document['registry']['status'])))
                self.update_report(task.to_dict())
            except (ValueError, RaceCondition) as e:
                logger.debug('Task {} status changed concurrently'.format(task.id))
                continue

            updated += 1

        return updated

    def maintain_broken(self, tags, container=None, limit=100):
        onhold_status = mahler.core.status.Broken('')

        updated = 0

        # TODO: Implement dependencies and test
        projection = {'registry.status': 1}

        task_iterator = self.retrieve_tasks(
            tags=tags, container=container, status=onhold_status, limit=limit,
            _return_doc=True, _projection=projection)

        for task_document in task_iterator:
            task = Task(op=None, arguments=None, id=task_document['id'],
                        name=None, registrar=self)
            if task.status.name != onhold_status.name:
                # Report is outdated, leave it to maintain_report to update it.
                continue

            if (not task.output and
                all(message_snipet not in task.status.message for message_snipet in TMP_BROKEN)):
                continue

            try:
                if task.output:
                    new_status = mahler.core.status.Completed('Failover completed trial')
                    self.update_status(task, new_status, _force=True)
                else:
                    new_status = mahler.core.status.FailedOver('Crashed because of broken node')
                    self.update_status(task, new_status)
                self.update_report(task.to_dict())
            except (ValueError, RaceCondition) as e:
                logger.debug('Task {} status changed concurrently'.format(task.id))
                continue

            updated += 1

        return updated

    def maintain_onhold(self, tags, container=None, limit=100):
        onhold_status = mahler.core.status.OnHold('')

        updated = 0

        # TODO: Implement dependencies and test
        projection = {'registry.status': 1}

        task_iterator = self.retrieve_tasks(
            tags=tags, container=container, status=onhold_status, limit=limit,
            _return_doc=True, _projection=projection)

        for task_document in task_iterator:
            task = Task(op=None, arguments=None, id=task_document['id'],
                        name=None, registrar=self)
            if task.status.name != onhold_status.name:
                # Report is outdated, leave it to maintain_report to update it.
                continue

            # TODO: Implement dependencies and test
            # task._dependencies = task_document['bounds.dependencies']
            try:
                self.update_status(task, mahler.core.status.Queued('dependencies met'))
                self.update_report(task.to_dict())
            except (ValueError, RaceCondition) as e:
                logger.debug('Task {} status changed concurrently'.format(task.id))
                continue

            updated += 1

        return updated

    def maintain_lost(self, tags, container=None, limit=100):
        perishable_status = [mahler.core.status.Reserved(''),
                             mahler.core.status.Running('')]

        status_names = [status.name for status in perishable_status]

        updated = 0

        projection = {'id': 1, 'registry.heartbeat': 1}

        task_iterator = self.retrieve_tasks(
            tags=tags, container=container, status=perishable_status, limit=limit,
            _return_doc=True, _projection=projection)

        for task_document in task_iterator:
            task = Task(op=None, arguments=None, id=task_document['id'],
                        name=None, registrar=self)
            if task.status.name not in status_names:
                # Report is outdated, leave it to maintain_report to update it.
                continue

            heartbeat_frequency = task_document['registry']['heartbeat']

            last_heartbeat = task._status.history[-1]['id'].generation_time
            now = datetime.datetime.now(datetime.timezone.utc)
            time_since_heartbeat = (now - last_heartbeat).total_seconds()
            if time_since_heartbeat < 2 * heartbeat_frequency:
                continue

            message = 'Lost heartbeat since {:0.02f}s ({:0.02f} x heartbeat)'.format(
                time_since_heartbeat, time_since_heartbeat / heartbeat_frequency)
            new_status = mahler.core.status.FailedOver(message)
            try:
                self.update_status(task, new_status)
            except (ValueError, RaceCondition) as e:
                logger.debug('Task {} status changed concurrently'.format(task_document['id']))
                continue
            else:
                self.update_report(task.to_dict())

            updated += 1

        return updated

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
            try:
                self.update_report(task.to_dict(report=True), upsert=True)
            except RaceCondition:
                pass

    def update_report(self, task_report, update_output=False, upsert=False):
        # create event
        # update report
        event = self.create_event('snapshot', task_report['id'], task_report.pop('timestamp'))
        event.pop('inc_id')
        event.pop('key')
        # NOTE: There should be no RaceCondition, snapshot events have no unique key
        self._db.add_event('report.timestamp', event)
        task_report['registry']['reported_on'] = event['id']
        self._db.update_report(task_report, update_output=update_output, upsert=upsert)

    def retrieve_tasks(self, id=None, tags=tuple(), container=None, status=None, limit=None,
                       sort=None, host=None, use_report=True, _return_doc=False, _projection=None):
        """
        """
        task_iterator = self._db.retrieve_tasks(
            id, tags, container, status, limit=limit, sort=sort, host=host, use_report=use_report,
            projection=_projection)
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
        current_status = task.status
        if not isinstance(current_status, mahler.core.status.Queued):
            raise ValueError("Task with status {} cannot be reserved".format(current_status))
        self.update_status(task, mahler.core.status.Reserved(message), current_status)

        # TODO: Fetch host and PID and set
        self._update_host(task)

    def _update_host(self, task):
        # TODO: In workers, query based on host. Only query same host or no host.
        ref_id = task._host.history[-1]['inc_id'] if task._host.history else None
        event = self.create_event('set', task.id, fetch_host_info(), ref_id=ref_id)
        # Fail if another process changed the status meanwhile
        # if ref_id + 1 exist:
        #     raise RaceCondition(Current status was not the most recent one)
        db_operation = self._db.add_event('host', event)
        task._host.history.append(event)

    # def update_priority(self, task, priority):
    #     # TODO: Create timestamp
    #     db_operation = self._db.add_event('priority', event)
    #     if self.retrieve_status('status', task)[-1] in ['reserved', 'running', 'completed']
    #         db_operation.rollback()
    #         raise RaceCondition("Cannot change priority")
    #     db.table('tasks.priorities').insert()

    def update_status(self, task, status, current_status=None, _force=False):
        # Avoid fresh status, update_status execution was intended based on a status
        # that may be different than the most recent one, we don't want to test based on a
        # different one..
        if current_status is None and task._status.history:
            last_status_event = task._status.history[-1]
            current_status = mahler.core.status.build(**last_status_event['item'])
            current_status.id = last_status_event['inc_id']

        if current_status is None and not isinstance(status, mahler.core.status.OnHold):
            raise ValueError("Task was not fully initialized yet, cannot set status: {}".format(
                                 status))

        if not status.can_follow(current_status) and not _force:
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

    def set_output(self, task, output, _force=False):
        # TODO: Check status as well as process ID
        # assert mahler.core.status.is_running(task, local=True)
        # TODO: Implement...
        if task.output is not None and not _force:
            raise RuntimeError('Cannot set output of task {} twice'.format(task.id))
        self._db.set_output(task, output)
        self.update_report(task.to_dict(), update_output=True)

    def set_volume(self, task, volume):
        # TODO: Check status as well as process ID
        # assert mahler.core.status.is_running(task, local=True)
        if task.volume is not None:
            raise RuntimeError('Cannot set volume of task {} twice'.format(task.id))
        self._db.set_volume(task, volume)
        self.update_report(task.to_dict())

    def retrieve_output(self, task):
        return self._db.retrieve_output(task)

    def retrieve_volume(self, task):
        return self._db.retrieve_volume(task)

    def close(self):
        self._db.close()


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
