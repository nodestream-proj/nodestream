from pathlib import Path


class InvalidClassPathError(ValueError):
    pass


class InvalidPipelineDefinitionError(ValueError):
    pass


class InvalidPipelineFileError(ValueError):
    pass


class PipelineComponentInitilizationError(ValueError):
    def __init__(self, initializier, init_arguments, *args: object) -> None:
        super().__init__(
            "Failed to Initialize Component in Declarative Pipeline.", *args
        )
        self.initializier = initializier
        self.init_arguments = init_arguments


class AlreadyInRegistryError(ValueError):
    def __init__(self, name, *args: object) -> None:
        super().__init__(f"{name} is already registered", *args)


class MissingFromRegistryError(ValueError):
    def __init__(self, name, *args: object) -> None:
        super().__init__(f"{name} is not in the subclass registry", *args)


class InvalidKeyLengthError(ValueError):
    def __init__(self, district_lengths, *args: object) -> None:
        lengths = f"({','.join((str(l) for l in district_lengths))})"
        error = f"Node Relationships do not have a consistent key length. Lengths are: ({lengths}) "
        super().__init__(error, *args)


class InvalidFlagError(ValueError):
    def __init__(self, flag_name, *args: object) -> None:
        super().__init__(
            f"Normalization flag with name '{flag_name}' is not valid.`", *args
        )


class UnhandledBranchError(ValueError):
    def __init__(self, missing_branch_value, *args: object) -> None:
        super().__init__(
            f"'{missing_branch_value}' was not matched in switch case", *args
        )


class MissingProjectFileError(ValueError):
    def __init__(self, file: Path, *args: object) -> None:
        super().__init__(f"'{file}' does not exist", *args)
