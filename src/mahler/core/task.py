from collections import defaultdict
import copy
import datetime
import logging
import os
import sys

from flask import Flask
from flask_caching import Cache

from mahler.core.attributes import EventBasedItemAttribute, EventBasedListAttribute
from mahler.core import config
import mahler.core.status


logger = logging.getLogger('mahler.core.task')


app = Flask(__name__)
# Check Configuring Flask-Cache section for more details
cache = Cache(app, config={'CACHE_TYPE': 'simple'})


# Must differentiate tasks
# 1. wrapped(function) -> operator
# 2. registered(wrapped(function)) -> task
# 3. registered data for a task -> task document



def follow(*operators, **kwargs):
    non_operator_items = [item for item in args if not isinstance(item, Operator)]
    if non_operator_items:
        raise ValueError(
            "Positional arguments must be operators on which the one being created "
            "will depend. Faulty arguments:\n{}".format(
                [(type(item), item) for item in non_operator_items]))

    new_operator = operator(**kwargs)

    for operator in operators:
        operator.precede(new_operator)

    return new_operator


def register(fct, *operators, **kwargs):
    if isinstance(fct, str):
        fct = importlib.import_module(function_string)

    return fct.register(*operators, **kwargs)


class Task(object):
    """A task view from the orchestration engine.

    A task object is a view on registry instance. Tasks can only be executed
    if registered in registry. Tasks cannot be modified directly, modifications
    are registered in the registry. Modifications are observable through the task
    object once registered, as the task object will fetch the new attributes
    from the registry.

    Task objects are lazy views on the registry, which means they cannot be expected to be
    up-to-date representations from the registry. This is to provide massive concurrency efficiency.
    Aggressive update schemes for up-to-date views should be used sparingly.
    """

    def __init__(self, op, arguments, attributes=None, id=None, name=None, registrar=None,
                 container=None, resources=None, heartbeat=config.heartbeat):
        self.op = op
        self._arguments = arguments
        if attributes is None:
            attributes = {}
        self._attributes = attributes
        self._name = name
        self._parent = None
        self._dependencies = []
        self._container = container
        self._resources = resources if resources else {}
        self.id = id
        self.heartbeat = int(heartbeat)
        self._priority = EventBasedItemAttribute(self, 'priority')
        self._status = EventBasedItemAttribute(self, 'status')
        self._host = EventBasedItemAttribute(self, 'host')
        self._tags = EventBasedListAttribute(self, 'tags')
        self._stdout = EventBasedListAttribute(self, 'stdout')
        self._stderr = EventBasedListAttribute(self, 'stderr')
        self._metrics = EventBasedListAttribute(self, 'metrics')
        self._output = None
        self._volume = None
        self._registrar = registrar

        # if document is not None:
        #     self._verify_document_cohesion()

    def _verify_document_cohesion(self):
        self._verify_inputs_cohesion()
        self._verify_document_cohesion()

    def _verify_inputs_cohesion(self):
        raise NotImplementedError

    def _verify_dependencies_cohesion(self):
        raise NotImplementedError

    # in registry, task is saved as
    # function='some.module.function'
    # NOTE: When loading `function`, we should get back access to the restore function.
    # inputs=dict() # for both function and restore
    # 

    @property
    def is_registered(self):
        return self.id is not None

    def register(self, registrar):
        pass

    def get_offline(self):
        return Task(self.op, self.arguments, attributes=self.attributes, id=self.id, name=self.name,
                    registrar=None, container=self.container, heartbeat=self.heartbeat)

    def run(self, state, stdout=sys.stdout, stderr=sys.stderr):
        if not self.is_registered:
            raise RuntimeError("Cannot execute task if not registered")

        # TODO Pop state based on current task id and dependence ids
        # TODO: If not immutable, update task id reference for all inputs that are
        #       fetched for the current task. This way, we can require that all objects
        #       for state must be direct dependencies of the current task, otherwise we pop
        #       them out and rely of the op.restore() the get them back with proper version.
        # TODO: Needs dependencies implementation
        # inputs = state.get(self.dependencies, self.op.arguments.keys())
        inputs = {}
        inputs.update(self.arguments)

        # TODO: Returned inputs are those actually used for the run, including inputs for the 
        #       restore() if applicable, and inputs for the function itself. Note that
        #       inputs for the functions itself can be objects that are not in the current
        #       `inputs` object, but rather coming for the output of `restore()`. This is 
        #       partly why we need to output for inputs used from `op.run()`.
        os.environ['_MAHLER_TASK_ID'] = str(self.id)
        rval = self.op.run(inputs, stdout=stdout, stderr=stderr)
        os.environ['_MAHLER_TASK_ID'] = ''
        # NOTE: Inputs is modified inplace, so it now contains restored input if op.restore()
        #       got executed during op.run()

        if isinstance(rval, tuple) and len(rval) == 2:
            data, volume = rval
        elif rval is None:
            data = {}
            volume = {}
        else:
            data = rval
            volume = {}

        if not self.op.immutable:
            state.update(self, inputs)
        state.update(self, data)
        # Filter dict(file='', object=object) and update state with object only
        state.update(self, {name:data['object'] for name, data in volume.items()})

        return data, volume

    # STATIC

    @property
    def parent(self):
        return self._parent

    @property
    def dependencies(self):
        return self._dependencies

    @property
    def container(self):
        return self._container

    @property
    def arguments(self):
        return self._arguments

    @property
    def attributes(self):
        return self._attributes

    @property
    def resources(self):
        resources = {}
        if self.op:
            resources.update(copy.deepcopy(self.op.resources))

        if self._resources:
            resources.update(self._resources)

        return resources

    @property
    def resumable(self):
        return self._resumable

    @property
    def immutable(self):
        return self._immutable

    # EventSourced

    @property
    def priority(self):
        return self._priority.value

    @property
    def created_on(self):
        return self.id.generation_time

    @property
    def started_on(self):
        events = self._status.events 
        if not events:
            return None

        # TODO: Support interruption, that means there may me other status between runnings
        found_running = False
        started_on = None
        for i, event in enumerate(events[::-1]):
            found_running = found_running or event['item']['name'] == 'Running'

            if found_running and event['item']['name'] != 'Running':
                break

            if found_running:
                started_on = event['id'].generation_time

        # If not found, started_on is None
        return started_on

    @property
    def stopped_on(self):
        if not self.started_on:
            return None

        next_status = None
        found_running = False
        stopped_on = None
        for i, event in enumerate(self._status.history[::-1]):
            if event['item']['name'] == 'Running':
                break

            next_status = event

        if next_status is None:
            return event['id'].generation_time
        
        # If status changed before next heartbeat, this is the stopped time. Otherwise,
        # we take last running timestamp + heartbeat, assuming the task was lost and
        # the gap between running and next status is not reliable.
        return min(next_status['id'].generation_time,
                   event['id'].generation_time + datetime.timedelta(seconds=self.heartbeat))

    @property
    def duration(self):
        if not self.started_on:
            return None

        # TODO: Support interruption, that means we should compute durations in each
        #       running sequence. In other words, duration <= (stopped_on - started_on).
        return (self.stopped_on - self.started_on).total_seconds()

    @property
    def updated_on(self):
        if not self._status.value:
            return None

        return self._status.last_item['id'].generation_time

    def get_recent_status(self):
        value = self._status.value
        if not value:
            return None

        status = mahler.core.status.build(**value)
        status.id = self._status.last_item['inc_id']
        status.timestamp = self._status.last_item['runtime_timestamp']
        return status

    @property
    @cache.memoize(timeout=60)
    def status(self):
        return self.get_recent_status()

    @property
    def host(self):
        value = self._host.value
        if not value:
            return {}

        return copy.deepcopy(value)

    @property
    def tags(self):
        return [tag['tag'] for tag in self._tags.value]

    @property
    def stdout(self):
        return "".join(self._stdout.value)

    @property
    def stderr(self):
        return "".join(self._stderr.value)

    @property
    @cache.memoize(timeout=60)
    def metrics(self):
        return self.get_recent_metrics()

    def get_recent_metrics(self):
        # TODO: Organize by types, and sort by creation time.
        #       Make it be defaultdict(list) so that unavailable metrics can be considered as
        #       present but empty.
        metrics = defaultdict(list)
        for metric in self._metrics.value:
            metrics[metric['type']].append(metric['value'])

        return metrics

    @property
    def output(self):
        # None until completion
        if self._registrar is None:
            return None

        if self._output is None:
            # NOTE: If not in DB, then it is None
            self._output = self._registrar.retrieve_output(self)

        return self._output

    @property
    def volume(self):
        if self._registrar is None:
            return None

        if self._volume is None:
            # NOTE: If not in DB, then it is None
            self._volume = self._registrar.retrieve_volume(self)

        return self._volume

    @property
    def name(self):
        if self._name:
            return self._name

        return self.op.name if self.op else None

    def to_dict(self, report=True):

        task_document = dict(
            name=self.name,
            bounds=dict(
                # priority is event-sourced
                parent=self.parent.id if self.parent else None,
                dependencies=[task.id for task in self.dependencies]),
            registry=dict(
                # started_on is dynamic
                # stopped_on
                # updated_on
                # duration
                # status is event-sourced
                # tags is event-sourced
                heartbeat=int(self.heartbeat),
                container=self.container),
            facility=dict(
                # host is event-sourced
                resources=self.resources),
            op=self.op.to_dict() if self.op else {},
            arguments=self.arguments,
            attributes=self.attributes,
            # stdout is event-sourced
            # stderr is event-sourced
            output=self.output,
            volume=self.volume)

        # Only use last version in reports
        # facility:
        #     <host>:
        #         CPUs
        #         GPUs
        #         platform
        #         env
        #           contains (among other things)
        #           <clustername>
        #           <user>
        #           <slurm_job_nodelist>

        if self.id:
            task_document['id'] = self.id

        if report and self.id is None:
            raise RuntimeError("Cannot build report if task is not registered")

        report_document = task_document

        # TODO:
        # report['bounds']['priority'] = self.priority
                # started_on is dynamic
                # stopped_on
                # updated_on
                # duration

        # reported_on should be the last creation_timestamp of the event sourced attributes.
        # There is however the risk of having new events coming in since the last of a given
        # attribute.
        # Ex: status last event is at time t and is the most recent one, but tag event
        #     is at time t - delta and not yet in the report. That means report with 
        #     reported_on is not correct.
        # Solution:
        # Global reported_on for simplicity + timestamp for each attribute
        # reported_on:
        #     report
        #     status
        #     tags

        # It is not possible to register a status at time t - 1 if a status at time t is registered.
        # This is because the IDs would conflict, to be able to register the status of time t - 1
        # the status of time t must be known from the process, that means status of time t -1 would
        # be registered at time t + 1.

        if report:
            report_document['registry']['started_on'] = self.started_on
            report_document['registry']['stopped_on'] = self.stopped_on
            report_document['registry']['updated_on'] = self.updated_on
            # reported_on is set at registration based on snapshot-event id
            # report_document['registry']['reported_on'] = datetime.datetime.utcnow()
            report_document['registry']['duration'] = self.duration
            report_document['registry']['status'] = self.get_recent_status().name
            report_document['registry']['tags'] = self.tags

            report_document['facility']['host'] = self.host

        report_document['timestamp'] = {
            'status': self._status.history[-1]['id'] if self._status.history else None,
            'tags': self._tags.history[-1]['id'] if self._tags.history else None
            }

        # TODO
        # report['facility']['host'] = self.host

        return report_document
