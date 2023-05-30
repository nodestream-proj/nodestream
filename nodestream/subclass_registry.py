from functools import wraps

from .exceptions import AlreadyInRegistryError, MissingFromRegistryError


class SubclassRegistry:
    """A registry for subclasses of a base class."""

    __slots__ = ("registry", "linked_base")

    def __init__(self) -> None:
        self.registry = {}
        self.linked_base = None

    def connect_baseclass(self, base_class):
        """Connect a base class to this registry."""

        old_init_subclass = base_class.__init_subclass__
        base_class.__subclass__registry__ = self
        self.linked_base = base_class

        @classmethod
        @wraps(old_init_subclass)
        def init_subclass(cls, alias=None, *args, **kwargs):
            alias = alias or cls.__name__

            if alias in self.registry:
                raise AlreadyInRegistryError(alias)

            self.registry[alias] = cls
            return old_init_subclass(*args, **kwargs)

        base_class.__init_subclass__ = init_subclass
        return base_class

    def name_for(self, cls):
        """Get the name of a class in the registry."""

        for k, v in self.registry.items():
            if v is cls:
                return k

        return None

    def get(self, name):
        """Get a subclass by name."""
        try:
            return self.registry[name]
        except KeyError:
            raise MissingFromRegistryError(name)

    @property
    def all_subclasses(self):
        return self.registry.values()
