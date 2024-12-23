import json

import rich_click as click

from pathlib import Path

from pydantic import ValidationError, BaseModel
from rich.prompt import Prompt

from armonik_cli.core import console, MutuallyExclusiveOption, base_command
from armonik_cli.settings import Settings


key_argument = click.argument("key", required=True, type=str, metavar="KEY")


@click.group()
def config():
    """Display or change configuration settings for ArmoniK CLI."""
    pass


@config.command()
@key_argument
@base_command(connection_args=False)
def get(key: str, output: str, debug: bool) -> None:
    """Retrieve the value of a configuration setting by its KEY."""
    config = Settings.load_default_config()
    try:
        console.formatted_print({key: config.get_field(key)}, format=output)
    except AttributeError:
        console.print(f"Warning: '{key}' is not a known configuration key.")


# The function cannot be called 'set' directly, as this causes a conflict with the constructor of the built-in set object.
@config.command("set")
@key_argument
@click.argument("value", required=True, type=str, metavar="VALUE")
@base_command(connection_args=False)
def set_(key: str, value: str, output: str, debug: bool) -> None:
    """Update a configuration setting with a VALUE for the given KEY."""
    config = Settings.load_default_config()
    try:
        config.set_field(key, value)
        config.save_default_config()
        console.print(f"Updated '{key}' configuration with value '{config.get_field(key)}'.")
    except AttributeError:
        console.print(f"Warning: '{key}' is not a known configuration key.")
    except ValidationError:
        console.print(f"Error: '{value}' is not a correct value for key '{key}'.")


@config.command()
@base_command(connection_args=False)
def show(output: str, debug: bool) -> None:
    """Display current configuration settings."""
    console.formatted_print(Settings.load_default_config().dict(), format=output)


@config.command()
@base_command(connection_args=False)
def list(output: str, debug: bool) -> None:
    """List all configuration settings."""
    pass
