import logging

from laboratorium import core


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

    def __init__(self, op, inputs, document=None):
        self.op = op
        self.inputs = inputs
        self.document = document

        if document is not None:
            self._verify_document_cohesion()

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
        return self.document is not None and self.document.id is not None

    def register(self, registrar):
        pass

    def run(self, state):
        if not self.is_registered:
            raise RuntimeError("Cannot execute task if not registered")

        # TODO Pop state based on current task id and dependence ids
        # TODO: If not immutable, update task id reference for all inputs that are
        #       fetched for the current task. This way, we can require that all objects
        #       for state must be direct dependencies of the current task, otherwise we pop
        #       them out and rely of the op.restore() the get them back with proper version.
        inputs = state.get(self.document.dependencies, self.inputs.keys())
        inputs.update(self.document.inputs)

        # TODO: Returned inputs are those actually used for the run, including inputs for the 
        #       restore() if applicable, and inputs for the function itself. Note that
        #       inputs for the functions itself can be objects that are not in the current
        #       `inputs` object, but rather coming for the output of `restore()`. This is 
        #       partly why we need to output for inputs used from `op.run()`.
        data, volume, new_tasks = self.op.run(inputs)
        # NOTE: Inputs is modified inplace, so it now contains restored input if op.restore()
        #       got executed during op.run()

        if not self.op.immutable:
            state.update(self, inputs)
        state.update(self, data)
        # Filter dict(file='', object=object) and update state with object only
        state.update(self, {name:data['object'] for name, data in volume.items()})

        return data, volume, new_tasks
