from functools import wraps, partial

import grpc
import rich_click as click

from armonik_cli.core.console import console
from armonik_cli.exceptions import NotFoundError, InternalError


def error_handler(func=None):
    """Decorator to ensure correct display of errors.

    Args:
        func: The command function to be decorated. If None, a partial function is returned,
            allowing the decorator to be used with parentheses.

    Returns:
        The wrapped function with added CLI options.
    """
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
            click.echo(
                click.style(
                    f"Failed with error:\n[status_code={status_code.name}]: {error_details}", "red"
                )
            )
            if "debug" in kwargs and kwargs["debug"]:
                console.print_exception()
            if status_code == grpc.StatusCode.NOT_FOUND:
                raise NotFoundError(error_details)
            else:
                raise InternalError("An internal fatal error occured.")
        except Exception:
            console.print_exception()

    return wrapper


def base_command(func=None):
    """Decorator to add common CLI options to a Click command function, including
    'endpoint', 'output', and 'debug'. These options are automatically passed
    as arguments to the decorated function.

    The following options are added to the command:
    - `--endpoint` (required): Specifies the cluster endpoint.
    - `--output`: Sets the output format, with options 'yaml', 'json', or 'table' (default is 'json').
    - `--debug`: Enables debug mode, printing additional logs if set.

    Warning:
        If the decorated function has parameters with the same names as the options added by
        this decorator, this can lead to conflicts and unpredictable behavior.

    Args:
        func: The command function to be decorated. If None, a partial function is returned,
            allowing the decorator to be used with parentheses.

    Returns:
        The wrapped function with added CLI options.
    """

    # Allow to call the decorator with parenthesis.
    if not func:
        return partial(base_command)

    # Define the wrapper function with added Click options
    @click.option(
        "-e",
        "--endpoint",
        type=str,
        required=True,
        help="Endpoint of the cluster to connect to.",
        metavar="ENDPOINT",
    )
    @click.option(
        "-o",
        "--output",
        type=click.Choice(["yaml", "json", "table"], case_sensitive=False),
        default="json",
        show_default=True,
        help="Commands output format.",
        metavar="FORMAT",
    )
    @click.option(
        "--debug", is_flag=True, default=False, help="Print debug logs and internal errors."
    )
    @error_handler
    @wraps(func)
    def wrapper(endpoint: str, output: str, debug: bool, *args, **kwargs):
        kwargs["endpoint"] = endpoint
        kwargs["output"] = output
        kwargs["debug"] = debug
        return func(*args, **kwargs)

    return wrapper
