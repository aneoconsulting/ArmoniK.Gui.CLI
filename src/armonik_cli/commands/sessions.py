import json

import grpc
import rich_click as click

from armonik.client.sessions import ArmoniKSessions
from armonik.common import SessionStatus
from rich.table import Table

from armonik_cli.console import console
from armonik_cli.utils import CLIJSONEncoder


@click.group(name="sessions")
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
        for session in sessions:
            session.status = (
                SessionStatus.name_from_value(session.status).split("_")[-1].capitalize()
            )
        if output == "fancy":
            table = Table(box=None)
            table.add_column("ID")
            table.add_column("Status")
            table.add_column("Duration")

            for session in sessions:
                table.add_row(session.session_id, str(session.status), str(session.duration))

            console.print(table)
        else:
            console.print_json(json.dumps(sessions, cls=CLIJSONEncoder))

    console.print(f"\n{total} sessions found.")
