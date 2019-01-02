import copy
import logging
import sys

from mahler.core.attributes import EventBasedItemAttribute, EventBasedListAttribute
import mahler.core.status


logger = logging.getLogger('mahler.core.task')


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

    def __init__(self, op, arguments, id=None, name=None, registrar=None):
        self.op = op
        self._arguments = arguments
        self._name = name
        self._parent = None
        self._dependencies = []
        self._container = None
        self._ressources = {}
        self.id = id
        self._priority = EventBasedItemAttribute(self, 'priority')
        self._status = EventBasedItemAttribute(self, 'status')
        self._tags = EventBasedListAttribute(self, 'tags')
        # self._host
        self._stdout = EventBasedListAttribute(self, 'stdout')
        self._stderr = EventBasedListAttribute(self, 'stderr')
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
        data, volume = self.op.run(inputs, stdout=stdout, stderr=stderr)
        # NOTE: Inputs is modified inplace, so it now contains restored input if op.restore()
        #       got executed during op.run()

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
    def ressources(self):
        ressources = copy.deepcopy(self.op.ressources)
        ressources.update(self._ressources)
        return ressources

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
    def status(self):
        # TODO: This may create mismatch between status and ID if there is a refresh
        #       between self._status.value and self._status.last_id
        if not self._status.value:
            return None

        status = mahler.core.status.build(**self._status.value)
        status.id = self._status.last_id
        return status

    @property
    def tags(self):
        return [tag['tag'] for tag in self._tags.value]

    @property
    def host(self):
        # TODO
        return ""  # self._host

    @property
    def stdout(self):
        # TODO
        return "\n".join(self._stdout.value)

    @property
    def stderr(self):
        # TODO
        return "\n".join(self._stderr.value)

    # None until completion

    @property
    def output(self):
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
            self._volume = self._registrar.retrieve_volume(task)

        return self._volume

    @property
    def name(self):
        if self._name:
            return self._name

        return self.op.name

    def to_dict(self):
        task_document = dict(
            name=self.name,
            bounds=dict(
                # priority is event-sourced
                parent=self.parent.id if self.parent else None,
                dependencies=[task.id for task in self.dependencies]),
            registry=dict(
                # status is event-sourced
                # tags is event-sourced
                container=self.container),
            facility=dict(
                # host is event-sourced
                ressources=self.ressources),
            op=self.op.to_dict(),
            arguments=self.arguments,
            # stdout is event-sourced
            # stderr is event-sourced
            output=self.output,
            volume=self.volume)

        if self.id:
            task_document['id'] = self.id

        return task_document
