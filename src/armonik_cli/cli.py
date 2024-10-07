import rich_click as click


from armonik_cli import commands, version
from armonik_cli.console import console


@click.group(name="armonik")
@click.version_option(version=version.__version__, prog_name="armonik")
def cli() -> None:
    """
    ArmoniK CLI is a tool to monitor and manage ArmoniK clusters.
    """
    pass


cli.add_command(commands.sessions)
