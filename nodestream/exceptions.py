class InvalidClassPathError(ValueError):
    """Raised when a class path is invalid."""

    pass


class InvalidPipelineDefinitionError(ValueError):
    """Raised when a pipeline definition is invalid."""

    pass


class InvalidPipelineFileError(ValueError):
    """Raised when a pipeline file is invalid."""

    pass


class PipelineComponentInitializationError(ValueError):
    """Raised when a component fails to initialize."""

    def __init__(self, initializer, init_arguments, *args: object) -> None:
        super().__init__(
            "Failed to Initialize Component in Declarative Pipeline. Likely the arguments are incorrect.",
            *args,
        )
        self.initializer = initializer
        self.init_arguments = init_arguments


class AlreadyInRegistryError(ValueError):
    """Raised when a subclass with the same name is already in the subclass registry."""

    def __init__(self, name, *args: object) -> None:
        super().__init__(f"{name} is already registered", *args)


class MissingFromRegistryError(ValueError):
    """Raised when a subclass is not in the subclass registry."""

    def __init__(self, name, *args: object) -> None:
        super().__init__(f"{name} is not in the subclass registry", *args)


class InvalidKeyLengthError(ValueError):
    """Raised when a related nodes have differing lengths of key parts returned from a value provider.."""

    def __init__(self, district_lengths, *args: object) -> None:
        lengths = f"({','.join((str(length) for length in district_lengths))})"
        error = f"Node Relationships do not have a consistent key length. Lengths are: ({lengths}) "
        super().__init__(error, *args)


class InvalidFlagError(ValueError):
    """Raised when a normalization flag is not valid."""

    def __init__(self, flag_name, *args: object) -> None:
        super().__init__(
            f"Normalization flag with name '{flag_name}' is not valid.`", *args
        )


class UnhandledBranchError(ValueError):
    """Raised when a branch is not handled in a switch case."""

    def __init__(self, missing_branch_value, *args: object) -> None:
        super().__init__(
            f"'{missing_branch_value}' was not matched in switch case", *args
        )


class MissingExpectedPipelineError(ValueError):
    pass
