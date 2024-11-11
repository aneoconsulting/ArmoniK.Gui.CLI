import json

import rich_click as click

from pathlib import Path

from rich.prompt import Prompt

from armonik_cli.core import console, Configuration, MutuallyExclusiveOption, base_command


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
    config = Configuration.load_default()
    if config.has(key):
        return console.formatted_print({key: config.get(key)}, format=output)
    return console.print(f"Warning: '{key}' is not a known configuration key.")


# The function cannot be called 'set' directly, as this causes a conflict with the constructor of the built-in set object.
@config.command("set")
@key_argument
@click.argument("value", required=True, type=str, metavar="VALUE")
@base_command(connection_args=False)
def set_(key: str, value: str, output: str, debug: bool) -> None:
    """Update a configuration setting with a VALUE for the given KEY."""
    config = Configuration.load_default()
    if config.has(key):
        config.set(key, value)
        return console.print(f"Updated '{key}' configuration with value '{value}'.")
    return console.print(f"Warning: '{key}' is not a known configuration key.")


@config.command()
@base_command(connection_args=False)
def list(output: str, debug: bool) -> None:
    """Display all configuration settings."""
    config = Configuration.load_default()
    console.formatted_print(config.to_dict(), format=output)


@config.command()
@click.option(
    "--local",
    is_flag=True,
    help="Set to a local cluster without TLS enabled.",
    cls=MutuallyExclusiveOption,
    mutual=["interactive", "source"],
)
@click.option(
    "--interactive",
    is_flag=True,
    help="Use interactive prompts.",
    cls=MutuallyExclusiveOption,
    mutual=["local", "source"],
)
@click.option(
    "--source",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Use the deployment generated folder to retrieve connection details.",
    cls=MutuallyExclusiveOption,
    mutual=["local", "interactive"],
)
@base_command(connection_args=False)
def set_connection(local: bool, interactive: bool, source: str, output: str, debug: bool) -> None:
    """Update all cluster connection configuration settings at once."""
    endpoint = ""
    if local:
        endpoint = "172.17.119.85:5001"
    elif interactive:
        endpoint = Prompt.get_input(console, "Endpoint: ", password=False)
    elif source:
        with (Path(source) / "armonik-output.json").open() as output_file:
            endpoint = json.loads(output_file.read())["armonik"]["control_plane_url"]
    else:
        return
    config = Configuration.load_default()
    config.set("endpoint", endpoint)
