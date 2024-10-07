import grpc
import rich_click as click

from datetime import timedelta
from typing import List, Union

from armonik.client.sessions import ArmoniKSessions
from armonik.common import SessionStatus, Session, TaskOptions

from armonik_cli.console import console
from armonik_cli.errors import error_handler
from armonik_cli.commands.common import endpoint_option, output_option, debug_option


SESSION_TABLE_COLS = [("ID", "SessionId"), ("Status", "Status"), ("CreatedAt", "CreatedAt")]


@click.group(name="sessions")
def sessions() -> None:
    """Manage cluster sessions."""
    pass


@sessions.command()
@endpoint_option
@output_option
@debug_option
@error_handler
def list(endpoint: str, output: str, debug: bool) -> None:
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
def get(endpoint: str, output: str, session_id: str, debug: bool) -> None:
    """Get details of a given session."""
    with grpc.insecure_channel(endpoint) as channel:
        sessions_client = ArmoniKSessions(channel)
        session = sessions_client.get_session(session_id=session_id)
        session = _clean_up_status(session)
        console.formatted_print(session, format=output, table_cols=SESSION_TABLE_COLS)


@sessions.command()
@endpoint_option
@click.option(
    "--max-retries",
    type=int,
    required=True,
    help="Maximum default number of execution attempts for session tasks.",
    metavar="NUM_RETRIES",
)
@click.option(
    "--max-duration",
    type=str,
    required=True,
    help="Maximum default task execution time (format HH:MM:SS.MS).",
    metavar="DURATION",
)
@click.option(
    "--priority", type=int, required=True, help="Default task priority.", metavar="PRIORITY"
)
@click.option(
    "--partition",
    type=str,
    multiple=True,
    help="Partition to add to the session.",
    metavar="PARTITION",
)
@click.option(
    "--default-partition",
    type=str,
    default="default",
    show_default=True,
    help="Default partition.",
    metavar="PARTITION",
)
@click.option(
    "--application-name", type=str, required=False, help="Default application name.", metavar="NAME"
)
@click.option(
    "--application-version",
    type=str,
    required=False,
    help="Default application version.",
    metavar="VERSION",
)
@click.option(
    "--application-namespace",
    type=str,
    required=False,
    help="Default application namespace.",
    metavar="NAMESPACE",
)
@click.option(
    "--application-service",
    type=str,
    required=False,
    help="Default application service.",
    metavar="SERVICE",
)
@click.option(
    "--engine-type", type=str, required=False, help="Default engine type.", metavar="ENGINE_TYPE"
)
@click.option(
    "--option",
    type=str,
    required=False,
    multiple=True,
    default=[],
    help="Additional default options.",
    metavar="NAME=VALUE",
)
@output_option
@debug_option
@error_handler
def create(
    endpoint: str,
    max_retries: int,
    max_duration: str,
    priority: int,
    partition: Union[List[str], None],
    default_partition: str,
    application_name: Union[str, None],
    application_version: Union[str, None],
    application_namespace: Union[str, None],
    application_service: Union[str, None],
    engine_type: Union[str, None],
    option: List[str],
    output: str,
    debug: bool,
) -> None:
    """Create a new session."""

    # Validate max_duration format.
    try:
        max_duration_td = _parse_time_delta(max_duration)
    except ValueError:
        raise click.BadParameter("Invalid format for max duration. Use HH:MM:SS.MS.")

    # Validate options format.
    options = {}
    for item in option:
        try:
            k, v = item.split("=")
            options[k] = v
        except ValueError:
            raise click.BadParameter(f"Invalid format for option: {item}. Use key=value.")

    with grpc.insecure_channel(endpoint) as channel:
        sessions_client = ArmoniKSessions(channel)
        session_id = sessions_client.create_session(
            default_task_options=TaskOptions(
                max_duration=max_duration_td,
                priority=priority,
                max_retries=max_retries,
                partition_id=default_partition,
                application_name=application_name,
                application_version=application_version,
                application_namespace=application_namespace,
                application_service=application_service,
                engine_type=engine_type,
                options=options,
            ),
            partition_ids=partition,
        )
    console.formatted_print({"SessionId": session_id}, format=output)


def _clean_up_status(session: Session) -> Session:
    session.status = SessionStatus.name_from_value(session.status).split("_")[-1].capitalize()
    return session


def _parse_time_delta(time_str: str) -> timedelta:
    """
    Parses a time string in the format "HH:MM:SS.MS" into a datetime.timedelta object.

    Args:
        time_str (str): A string representing a time duration in hours, minutes,
                        seconds, and milliseconds (e.g., "12:34:56.789").

    Returns:
        timedelta: A datetime.timedelta object representing the parsed time duration.

    Raises:
        ValueError: If the input string is not in the correct format.
    """
    hours, minutes, seconds = time_str.split(":")
    sec, microseconds = (seconds.split(".") + ["0"])[:2]  # Handle missing milliseconds
    return timedelta(
        hours=int(hours),
        minutes=int(minutes),
        seconds=int(sec),
        milliseconds=int(microseconds.ljust(3, "0")),  # Ensure 3 digits for milliseconds
    )
