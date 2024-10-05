import grpc
import rich_click as click

from armonik.client.sessions import ArmoniKSessions
from rich.table import Table

from armonik_cli import console, version


@click.group(name="armonik")
@click.version_option(version=version.__version__, prog_name="armonik")
def cli():
    """
    ArmoniK CLI is a tool to monitor and manage ArmoniK clusters.
    """
    pass


@cli.group(name="sessions")
def sessions():
    """Manage cluster sessions."""
    pass


@sessions.command()
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
    type=click.Choice(["fancy", "json"], case_sensitive=False),
    default="fancy",
    show_default=True,
    help="Endpoint of the cluster to connect to.",
    metavar="FORMAT",
)
def list(endpoint: str, output: str):
    """List the sessions of an ArmoniK cluster."""
    with grpc.insecure_channel(endpoint) as channel:
        sessions_client = ArmoniKSessions(channel)
        total, sessions = sessions_client.list_sessions()

    if total > 0:
        if output == "fancy":
            table = Table(box=None)
            table.add_column("ID")
            table.add_column("Status")
            table.add_column("Duration")

            for session in sessions:
                table.add_row(session.session_id, str(session.status), str(session.duration))

            console.get_console().print(table)
        else:
            sessions = [s.__dict__ for s in sessions]
            console.get_console().print(sessions)

    console.get_console().print(f"\n{total} sessions found.")
