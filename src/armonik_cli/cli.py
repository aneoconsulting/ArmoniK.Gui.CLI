import rich_click as click


from armonik_cli import commands, version


@click.group(name="armonik")
@click.version_option(version=version.__version__, prog_name="armonik")
def cli():
    """
    ArmoniK CLI is a tool to monitor and manage ArmoniK clusters.
    """
    pass


cli.add_command(commands.sessions)
