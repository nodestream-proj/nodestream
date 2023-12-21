from abc import ABC, abstractmethod
from logging import getLogger


class MigratorInput:
    """Asks questions as the schema change detector works.

    This class is used by the schema change detector to ask questions to the
    user as it works. The schema change detector will ask questions to the
    user when it is not sure about the answer.

    The default implementation of this class will always answer no to all
    questions.
    """

    def ask_yes_no(self, question: str) -> bool:
        """Ask a yes/no question.

        Args:
            question: The question to ask.

        Returns:
            True if the user answered yes, False if the user answered no.
        """
        return False

    def format_ask_type_renamed(self, old_type: str, new_type: str) -> str:
        """Format a question about a type being renamed.

        Args:
            old_type: The old type name.
            new_type: The new type name.

        Returns:
            The formatted question.
        """
        return f"Did you rename {old_type} to {new_type}?"

    def ask_type_renamed(self, old_type: str, new_type: str) -> bool:
        """Ask if a type was renamed.

        Args:
            old_type: The old type name.
            new_type: The new type name.

        Returns:
            True if the user answered yes, False if the user answered no.
        """
        question = self.format_ask_type_renamed(old_type, new_type)
        return self.ask_yes_no(question)

    def format_ask_property_renamed(
        self, object_type: str, old_property_name: str, new_property_name: str
    ) -> str:
        """Format a question about a property being renamed.

        Args:
            object_type: The type of the object that the property belongs to.
            old_property_name: The old property name.
            new_property_name: The new property name.

        Returns:
            The formatted question.
        """
        return f"Did you rename {old_property_name} to {new_property_name} on {object_type}?"

    def ask_property_renamed(
        self, object_type: str, old_property_name: str, new_property_name: str
    ) -> bool:
        """Ask if a property was renamed.

        Args:
            object_type: The type of the object that the property belongs to.
            old_property_name: The old property name.
            new_property_name: The new property name.

        Returns:
            True if the user answered yes, False if the user answered no.
        """
        question = self.format_ask_property_renamed(
            object_type, old_property_name, new_property_name
        )
        return self.ask_yes_no(question)


class MigratorOutput(ABC):
    """Abstract class for printing messages as the schema change detector works.

    This class is used by the schema change detector to print messages as it
    works. The schema change detector will print messages to the user when it
    needs to communicate something.
    """

    @abstractmethod
    def print_message(self, message: str) -> None:
        """Print a message to the user.

        Args:
            message: The message to print.
        """
        raise NotImplementedError

    @abstractmethod
    def print_exception(self, exception: Exception) -> None:
        """Print an exception to the user.

        Args:
            exception: The exception to print.
        """
        raise NotImplementedError


class LoggingMigratorOutput(MigratorOutput):
    """A message printer that logs messages."""

    def __init__(self) -> None:
        self.logger = getLogger(self.__class__.__name__)

    def print_message(self, message: str) -> None:
        self.logger.debug(message)

    def print_exception(self, exception: Exception) -> None:
        self.logger.exception(exception)
