import rich_click as click


class ArmoniKCLIError(click.ClickException):
    """Base exception for ArmoniK CLI errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class InternalError(ArmoniKCLIError):
    """Error raised when an unknown internal error occured."""


class NotFoundError(ArmoniKCLIError):
    """Error raised when a given object of the API is not found."""


class ConfigFileFormatError(ArmoniKCLIError):
    """Error raised when a configuration file is incorrectly formatted."""
