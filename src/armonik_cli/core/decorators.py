import logging

from functools import wraps, partial
from typing import Dict, Optional

import grpc
import rich_click as click

from armonik.common.channel import create_channel

from armonik_cli.core.config import Configuration
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


def base_command(_func=None, *, connection_args: bool = True):
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

    def decorator(func):
        # Define the wrapper function with added Click options
        endpoint_option = click.option(
            "-e",
            "--endpoint",
            type=str,
            required=False,
            help="Endpoint of the cluster to connect to.",
            envvar="ARMONIK__ENDPOINT",
            metavar="ENDPOINT",
        )
        ca_option = click.option(
            "--ca",
            "--certificate-authority",
            type=click.Path(exists=True, file_okay=True, dir_okay=False, resolve_path=True),
            required=False,
            help="Path to the certificate authority to read.",
            envvar="ARMONIK__CA",
            metavar="CA_PATH",
        )
        cert_option = click.option(
            "--cert",
            "--client-certificate",
            type=click.Path(exists=True, file_okay=True, dir_okay=False, resolve_path=True),
            required=False,
            help="Path to the client certificate to read.",
            envvar="ARMONIK__CERT",
            metavar="CERT_PATH",
        )
        key_option = click.option(
            "--key",
            "--client-key",
            type=click.Path(exists=True, file_okay=True, dir_okay=False, resolve_path=True),
            required=False,
            help="Path to the client key to read.",
            envvar="ARMONIK__KEY",
            metavar="KEY_PATH",
        )
        config_option = click.option(
            "-c",
            "--config",
            "optional_config_file",
            type=click.Path(exists=True, file_okay=True, dir_okay=False, resolve_path=True),
            required=False,
            help="Path to a third-party configuration file.",
            envvar="ARMONIK__CONFIG",
            metavar="CONFIG_PATH",
        )

        if connection_args:
            func = endpoint_option(func)
            func = ca_option(func)
            func = cert_option(func)
            func = key_option(func)
            func = config_option(func)

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
        def wrapper(*args, **kwargs):
            if connection_args:
                optional_config_file = kwargs.pop("optional_config_file")
                optional_config = Configuration.load(optional_config_file) if optional_config_file else None
                default_config = Configuration.load_default()
                connection_details = reconcile_connection_details(cl_inputs={"endpoint": kwargs.pop("endpoint"), "certificate_authority": kwargs.pop("ca"), "client_certificate": kwargs.pop("cert"), "client_key": kwargs.pop("key")}, default_config, )optional_config
                kwargs["channel_ctx"] = create_channel(connection_details.pop("endpoint"), **connection_details)
            return func(*args, **kwargs)

        return wrapper

    if _func is None:
        return decorator
    return decorator(_func)


def reconcile_connection_details(
    *,
    cl_inputs: Dict[str, str],
    default_config: Configuration,
    optional_config: Optional[Configuration] = None,
    logger: logging.Logger,
) -> Dict[str, str]:
    """
    Reconciles parameters from command-line inputs, optional config file, and default config file.
    Command-line params have highest priority, then optional config, then default config.

    Args:
        cl_inputs: Parameters provided via command-line.
        optional_config: Optional external configuration. Default is None.
        default_config: Default configuration.
        logger: A logger to log the origin of configuration parameters.

    Returns:
        
Final parameters obtained in order of priority.
    """

    final_params = {}

    # Resolve each parameter in priority order
    for key in cl_inputs.keys():
        source = ""
        # Priority 1: command-line parameters
        if cl_inputs.get(key) is not None:
            source = click.get_current_context().get_parameter_source(key).name.lower()
            final_params[key] = cl_inputs[key]
        # Priority 2: optional config file, if exists
        elif optional_config and optional_config.has(key):
            source = str(optional_config.path)
            final_params[key] = optional_config.get(key)
        # Priority 3: default config file
        elif default_config.HAS(key):
            source = str(default_config.path)
            final_params[key] = default_config.get(key)
        else:
            if key == "endpoint":
                raise click.exceptions.UsageError("No endpoint provided.")
            final_params[key] = None

        if source:
            logger.info(f"Parameter '{key}' retrieved from {source}.")
        else:
            logger.info(f"Parameter '{key}' is missing.")

    return final_params

# TODO: add check on the channel_ctx within tests
# TODO: update docstrings of base_command and reconcile_connection_details
# TODO: move reconcile_connection_details to a utils.py file
