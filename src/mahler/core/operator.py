from collections import OrderedDict
import contextlib
import inspect
import importlib
import logging
import os
import pdb
import sys

from mahler.core.task import Task

logger = logging.getLogger('mahler.core.operator')


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


def wrap(restore=None, resources=None, immutable=False, resumable=False):

    def call(f):

        operator = Operator(f, restore=restore, resources=resources, immutable=immutable)

        return operator

    return call



class Operator(object):
    def __init__(self, fct, fct_signature=None, restore=None, restore_signature=None,
                 resources=None, immutable=False, resumable=False):
        if isinstance(fct, str):
            self.module_string = fct
            try:
                if '.' not in sys.path:
                    sys.path.append('.')
                self._fct = self.import_function(fct)
            except ImportError as e:
                logger.info('Cannot import function: {}'.format(str(e)))
                self._fct = None
        else:
            # Avoid wrapping operators. This is necessary for import tests.
            if isinstance(fct, Operator):
                fct = fct._fct

            try:
                self.module_string = self.get_module_string(fct)
            except RuntimeError as e:
                logger.warning(str(e))
                self.module_string = None
            self._fct = fct

        # TODO
        # self.node = DAGNode(self)
        self._restore = restore
        self._resources = resources
        self.immutable = immutable
        self.resumable = resumable

        if fct_signature is None and self._fct:
            fct_signature = self._parse(self._fct)
        else:
            fct_signature = None
        self._fct_signature = fct_signature

        if restore and restore_signature is None and self._restore:
            restore_signature = self._parse(restore)
        else:
            restore_signature = None

        self._restore_signature = restore_signature

        logger.debug("Built operator for {}".format(self.module_string))

    @property
    def name(self):
        return self._fct.__name__

    @property
    def resources(self):
        return self._resources if self._resources else {}

    def to_dict(self):
        op_document = dict(
            immutable = self.immutable,
            resumable = self.resumable,
            restore = self._restore,
            fct = self.module_string)

        return op_document

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

    def import_function(self, module_string):
        module_strings = module_string.split(".")
        module_string = '.'.join(module_strings[:-1])
        function_name = module_strings[-1]

        imported_module = importlib.import_module(module_string)
        function = getattr(imported_module, function_name)
        if isinstance(function, Operator):
            function = function._fct

        return function

    def get_module_string(self, function):
        module_string, function_name = self._get_module_string(function)
        return module_string + "." + function_name

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
        # 
        # error_message = ("Seems like the imported function is different than "
        #                  "the passed one. That is very weird. Module string "
        #                  "for import was {}.".format(module_string))
        # assert getattr(imported_module, function_name) is function, error_message

    def _get_module_string(self, function):
        module = function
        modules = []
        while not modules or module.__name__ != modules[0]:
            modules.insert(0, module.__name__)
            module = inspect.getmodule(module)

        if modules[0] == "__main__":
            raise RuntimeError("Cannot register operators defined in __main__")

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

    def run(self, inputs, stdout=sys.stdout, stderr=sys.stderr):
        if self._fct is None:
            try:
                self.import_function(self.module_string)
            except ImportError as e:
                raise ImportError('Cannot import function: {}'.format(str(e))) from e
            else:
                raise RuntimeError('Function was not imported, but it can...')

        # TODO
        # if self._restore and self.is_incomplete(inputs):
        #     self.restore(inputs)
        # elif self.is_incomplete(inputs):
        #     raise TypeError("Missing arguments: {}".format(self.is_incomplete(inputs)))

        logger.debug('Building inputs')
        fct_inputs = {}
        for name, value in self._fct_signature.items():
            fct_inputs[name] = inputs.get(name, value)

        logger.debug('Executing function')
        # with contextlib.redirect_stdout(stdout):
        #     with contextlib.redirect_stderr(stderr):
        with stdredirect(stdout, stderr):
            data, volume = self._fct(**fct_inputs)
        logger.debug('Executing completed')

        return data, volume

    def delay(self, *args, **kwargs):

        self._verify_importability(self._fct)
        if self._restore:
            self._verify_importability(self._restore)

        # Fetch default arguments of task (and restore if given)
        # Make sure all arguments have name:value. Positional arguments is forbidden

        # Get importable string

        # Create task document with function string, arguments
        # TODO: Turn arguments not supported as-is by pymongo into pickled objects.
        # task_document = core.task.Task()

        # task = Task()

        return Task(op=self, arguments=kwargs)

    def __call__(self, *args, **kwargs):
        return self._fct(*args, **kwargs)
