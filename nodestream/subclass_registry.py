from functools import wraps


class AlreadyInRegistryError(ValueError):
    def __init__(self, name, *args: object) -> None:
        super().__init__(f"{name} is already registered", *args)


class MissingFromRegistryError(ValueError):
    def __init__(self, name, *args: object) -> None:
        super().__init__(f"{name} is not in the subclass registry", *args)


class SubclassRegistry:
    __slots__ = ("registry", "linked_base")

    def __init__(self) -> None:
        self.registry = {}
        self.linked_base = None

    def connect_baseclass(self, base_class):
        old_init_subclass = base_class.__init_subclass__
        base_class.__subclass__registry__ = self
        self.linked_base = base_class

        @classmethod
        @wraps(old_init_subclass)
        def init_subclass(cls, name=None, *args, **kwargs):
            if name in self.registry:
                raise AlreadyInRegistryError(name)

            self.registry[name] = cls
            return old_init_subclass(*args, **kwargs)

        base_class.__init_subclass__ = init_subclass
        return base_class

    # TODO: Implement a Bi-dict here?
    def name_for(self, cls):
        for k, v in self.registry.items():
            if v is cls:
                return k

        return None

    def get(self, name):
        try:
            return self.registry[name]
        except KeyError:
            raise MissingFromRegistryError(name)
