from importlib import import_module

from ..exceptions import InvalidClassPathError, PipelineComponentInitializationError

DECLARATIVE_INIT_METHOD_NAME = "from_file_data"


def find_class(class_path):
    try:
        module_name, class_name = class_path.split(":")
        module = import_module(module_name)
        return getattr(module, class_name)
    except ValueError as e:
        raise InvalidClassPathError(
            f"Class path '{class_path}' is not in the correct format."
        ) from e
    except ImportError as e:
        raise InvalidClassPathError(
            f"Module '{module_name}' could not be imported."
        ) from e
    except AttributeError as e:
        raise InvalidClassPathError(
            f"Class '{class_name}' does not exist in module '{module_name}'."
        ) from e


class ClassLoader:
    """Loads a class from a string path and instantiates it with the given arguments."""

    def find_class_initializer(self, implementation, factory=None):
        class_definition = find_class(implementation)
        factory_method = factory or DECLARATIVE_INIT_METHOD_NAME
        if hasattr(class_definition, factory_method):
            return getattr(class_definition, factory_method)
        else:
            return class_definition

    def load_class(self, implementation, arguments=None, factory=None):
        arguments = arguments or {}
        initializer = self.find_class_initializer(implementation, factory)
        try:
            return initializer(**arguments)
        except TypeError as e:
            raise PipelineComponentInitializationError(initializer, arguments) from e
