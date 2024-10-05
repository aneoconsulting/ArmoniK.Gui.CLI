from functools import wraps, partial

import grpc
import rich_click as click

from armonik_cli import console


class ArmoniKCLIError(click.ClickException):
    """Base exception for ArmoniK CLI errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class InternalError(ArmoniKCLIError):
    """Error raised when an unknown internal error occured."""


class NotFoundError(ArmoniKCLIError):
    """Error raised when a given object of the API is not found."""


def error_handler(func):
    """A decorator to manage the correct display of errors.."""
    # Allow to call the decorator with parenthesis.
    if not func:
        return partial(error_handler)

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except click.ClickException:
            raise
        except grpc.RpcError as err:
            status_code = err.code()
            error_details = f"{err.details()}."

            if status_code == grpc.StatusCode.NOT_FOUND:
                raise NotFoundError(error_details)
            else:
                raise
        except Exception:
            if "debug" in kwargs and kwargs["debug"]:
                console.get_console().print_exception()
            else:
                raise InternalError("An internal fatal error occured.")

    return wrapper
