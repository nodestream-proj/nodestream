from abc import ABC, abstractclassmethod, abstractmethod
from pathlib import Path
from typing import Type

from schema import Schema
from yaml import SafeDumper, SafeLoader, dump, load


class DescribesYamlSchema(ABC):
    """A mixin for classes that can be described by a YAML schema."""

    @abstractclassmethod
    def describe_yaml_schema(self) -> Schema:
        raise NotImplementedError


class LoadsFromYaml(DescribesYamlSchema):
    """A mixin for classes that can be read from a YAML file."""

    @abstractclassmethod
    def from_file_data(cls, data):
        """Create an instance of this class from the given file data.

        Args:
            data: The data read from the file.

        Returns:
            An instance of this class.
        """
        raise NotImplementedError

    @classmethod
    def get_loader(cls) -> Type[SafeLoader]:
        """Get the YAML loader to use when reading this object from a file.

        Returns:
            The YAML loader to use when reading this object from a file.
        """
        return SafeLoader

    @classmethod
    def read_from_file(cls, file_path: Path) -> "LoadsFromYaml":
        """Read this object from a YAML file.

        Args:
            file_path: The path to the file to read from.

        Raises:
            FileNotFoundError: If the file does not exist.

        Returns:
            An instance of this class.
        """
        if not file_path.exists():
            raise FileNotFoundError(f"File '{file_path}' does not exist.")

        with file_path.open("r") as f:
            file_data = load(f, Loader=cls.get_loader())
            validated_file_data = cls.describe_yaml_schema().validate(file_data)
            return cls.from_file_data(validated_file_data)


class SavesToYaml(DescribesYamlSchema):
    """A mixin for classes that can be written to a YAML file."""

    @abstractmethod
    def to_file_data(self):
        """Get the data to write to a YAML file.

        Returns:
            The data to write to a YAML file.
        """
        raise NotImplementedError

    @classmethod
    def get_dumper(cls) -> Type[SafeDumper]:
        """Get the YAML dumper to use when writing this object to a file.

        Returns:
            The YAML dumper to use when writing this object to a file.
        """
        return SafeDumper

    def write_to_file(self, file_path: Path):
        """Write this object to a YAML file.

        If the file already exists, it will be overwritten.

        Args:
            file_path: The path to the file to write to.

        Returns:
            None
        """
        file_data = self.to_file_data()
        validated_file_data = self.describe_yaml_schema().validate(file_data)
        with file_path.open("w") as f:
            dump(
                validated_file_data,
                f,
                Dumper=self.get_dumper(),
                indent=2,
                sort_keys=True,
            )
