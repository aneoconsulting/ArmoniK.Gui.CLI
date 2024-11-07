from functools import wraps, partial

import grpc
import rich_click as click

from armonik_cli.core.console import console
from armonik_cli.exceptions import NotFoundError, InternalError


def error_handler(func=None):
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
                raise InternalError("An internal fatal error occured.")
        except Exception:
            if "debug" in kwargs and kwargs["debug"]:
                console.print_exception()
            else:
                raise InternalError("An internal fatal error occured.")

    return wrapper
