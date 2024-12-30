import rich_click as click


class ArmoniKCLIError(click.ClickException):
    """Base exception for ArmoniK CLI errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class InternalError(ArmoniKCLIError):
    """Error raised when an unknown internal error occured."""


class InternalArmoniKError(ArmoniKCLIError):
    """Error raised when there's an error in ArmoniK, you need to check the logs there for more information."""


class NotFoundError(ArmoniKCLIError):
    """Error raised when a given object of the API is not found."""
