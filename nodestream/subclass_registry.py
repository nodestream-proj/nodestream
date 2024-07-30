from functools import wraps

MISSING_FROM_REGISTRY_MESSAGE_FORMAT = "{registered_class_name} '{name}' is not registered. Did you forget to install a plugin?"


class AlreadyInRegistryError(ValueError):
    """Raised when a subclass with the same name is already in the subclass registry."""

    def __init__(self, name, registry: "SubclassRegistry", *args: object) -> None:
        super().__init__(
            f"{name} is already registered inside registry for: {registry.linked_base.__name__}",
            *args,
        )


class MissingFromRegistryError(ValueError):
    """Raised when a subclass is not in the subclass registry."""

    def __init__(self, name, registry: "SubclassRegistry", *args: object) -> None:
        super().__init__(
            MISSING_FROM_REGISTRY_MESSAGE_FORMAT.format(
                name=name,
                registered_class_name=registry.linked_base.__name__,
            ),
            *args,
        )


class SubclassRegistry:
    """A registry for subclasses of a base class."""

    __slots__ = ("registry", "linked_base", "ignore_overrides")

    def __init__(self, ignore_overrides: bool = False) -> None:
        self.registry = {}
        self.linked_base = None
        self.ignore_overrides = ignore_overrides

    def __contains__(self, sub_class):
        return sub_class in self.registry

    def connect_baseclass(self, base_class):
        """Connect a base class to this registry."""

        old_init_subclass = base_class.__init_subclass__
        base_class.__subclass__registry__ = self
        self.linked_base = base_class

        @classmethod
        @wraps(old_init_subclass)
        def init_subclass(cls, alias=None, *args, **kwargs):
            alias = alias or cls.__name__

            if alias in self.registry and not self.ignore_overrides:
                raise AlreadyInRegistryError(alias, self)

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
            raise MissingFromRegistryError(name, self)

    @property
    def all_subclasses(self):
        return self.registry.values()
