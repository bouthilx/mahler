from collections import OrderedDict
import inspect
import importlib
import logging

logger = logging.getLogger('laboratorium.engine.task')


def wrap(restore=None, ressources=None, immutable=False):

    def call(f):

        operator = Operator(f, restore=restore, ressources=ressources, immutable=immutable)

        return operator

    return call



class Operator(object):
    def __init__(self, fct, fct_signature=None, restore=None, restore_signature=None, ressources=None, immutable=False):
        # Avoid wrapping operators. This is necessary for import tests.
        if isinstance(fct, Operator):
            fct = fct._fct
        self._fct = fct
        # TODO
        # self.node = DAGNode(self)
        self._restore = restore
        self.ressources = ressources
        self.immutable = immutable

        if fct_signature is None:
            fct_signature = self._parse(fct)
        self._fct_signature = fct_signature

        if restore and restore_signature is None:
            restore_signature = self._parse(restore)
        self._restore_signature = restore_signature

        self._verify_importability(self._fct)
        if self._restore:
            self._verify_importability(self._restore)

        logger.debug("Built operator for {}".format(self.module_string))

    def _parse(self, function):
        signature = inspect.signature(function)
        not_supported = [inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.VAR_POSITIONAL,
                         inspect.Parameter.VAR_KEYWORD]
        arguments = OrderedDict()
        for parameter in signature.parameters.values():
            if parameter.kind in not_supported:
                raise ValueError('Function unsupported')
            # inspect.Parameter.empty is passed here if there is no default value
            arguments[parameter.name] = parameter.default
        return arguments

    def _verify_importability(self, function):
        module_string, function_name = self._get_module_string(function)
        try:
            imported_module = importlib.import_module(module_string)
        except BaseException as e:
            raise e

        if not hasattr(imported_module, function_name):
            raise TypeError(
                "Cannot find function '{}' inside module '{}'. Is it a nested definition?".format(
                    function_name, module_string))
        
        error_message = ("Seems like the imported function is different than "
                         "the passed one. That is very weird. Module string "
                         "for import was {}.".format(module_string))
        assert getattr(imported_module, function_name) is function, error_message

    def _get_module_string(self, function):
        module = function
        modules = []
        while not modules or module.__name__ != modules[0]:
            modules.insert(0, module.__name__)
            module = inspect.getmodule(module)

        return ".".join(modules[:-1]), modules[-1]

    # def follow(self, *operators):
    #     """
    #     Add operators that the current operator will automatically follow.

    #     Raises
    #     ------
    #     ValueError:
    #         If a given operator is already in the graph of automatic following or preceding
    #         operators.
    #     """
    #     self.node.add_parents(*[o.node for o in operators])

    # def precede(self, *operators):
    #     """
    #     Add operators that will automatically follow the current operator.

    #     Raises
    #     ------
    #     ValueError:
    #         If a given operator is already in the graph of automatic following or preceding
    #         operators.
    #     """
    #     self.node.add_children(*[o.node for o in operators])

    @property
    def import_string(self):
        return "{}:{}".format(*self._get_module_string(self._fct))

    def is_incomplete(self, inputs):
        missing_arguments = []
        for name, value in self._fct_signature.items():
            if inputs.get(name, value) is inspect.Parameter.empty:
                missing_arguments.append(name)

        return missing_arguments

    def restore(self, inputs):
        restore_inputs = {}
        for name, value in self._restore_signature.items():
            restore_inputs[name] = inputs.get(name, value)
            if restore_inputs[name] is inspect.Parameter.empty:
                raise TypeError("Argument {} is missing".format(name))
        inputs.update(self._restore(**restore_inputs))

    def run(self, inputs):
        if self._restore and self.is_incomplete(inputs):
            self.restore(inputs)
        elif self.is_incomplete(inputs):
            raise TypeError("Missing arguments: {}".format(self.is_incomplete(inputs)))

        fct_inputs = {}
        for name, value in self._fct_signature.items():
            fct_inputs[name] = inputs.get(name, value)
        return self._fct(**fct_inputs)

    def delay(self, **kwargs):
        # Fetch default arguments of task (and restore if given)
        # Make sure all arguments have name:value. Positional arguments is forbidden

        # Get importable string

        # Create task document with function string, arguments
        # TODO: Turn arguments not supported as-is by pymongo into pickled objects.
        if isinstance(container, Experiment):
            # create TaskTemplate
            task_document = core.task.Task()
        else:
            # create Task
            task_document = core.task.Task()

        task = Task()

        return Task(op=self)

    def __call__(self, *args, **kwargs):
        return self._fct(*args, **kwargs)
