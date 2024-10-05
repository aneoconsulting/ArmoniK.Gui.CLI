import grpc
import rich_click as click


from armonik.client.sessions import ArmoniKSessions
from armonik.common import SessionStatus, Session

from armonik_cli.console import console
from armonik_cli.errors import error_handler
from armonik_cli.commands.common import endpoint_option, output_option, debug_option


SESSION_TABLE_COLS = [("ID", "SessionId"), ("Status", "Status"), ("CreatedAt", "CreatedAt")]


@click.group(name="sessions")
def sessions():
    """Manage cluster sessions."""
    pass


@sessions.command()
@endpoint_option
@output_option
@debug_option
@error_handler
def list(endpoint: str, output: str, debug: bool):
    """List the sessions of an ArmoniK cluster."""
    with grpc.insecure_channel(endpoint) as channel:
        sessions_client = ArmoniKSessions(channel)
        total, sessions = sessions_client.list_sessions()

    if total > 0:
        sessions = [_clean_up_status(s) for s in sessions]
        console.formatted_print(
            sessions, format=output, table_cols=SESSION_TABLE_COLS
        )

    console.print(f"\n{total} sessions found.")


@sessions.command()
@endpoint_option
@output_option
@debug_option
@click.argument("session-id", required=True, type=str, metavar="SESSION_ID")
@error_handler
def get(endpoint: str, output: str, session_id: str, debug: bool):
    """Get details of a given session."""
    with grpc.insecure_channel(endpoint) as channel:
        sessions_client = ArmoniKSessions(channel)
        session = sessions_client.get_session(session_id=session_id)
        session = _clean_up_status(session)
        console.formatted_print(session, format=output, table_cols=SESSION_TABLE_COLS)


def _clean_up_status(session: Session) -> Session:
    session.status = SessionStatus.name_from_value(session.status).split("_")[-1].capitalize()
    return session
