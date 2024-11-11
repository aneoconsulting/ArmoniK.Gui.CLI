import rich_click as click

from typing import cast

from rich_click.utils import OptionGroupDict

from armonik_cli import commands, __version__
from armonik_cli.core import Configuration, console


COMMON_OPTIONS = cast(
    OptionGroupDict, {"name": "Common options", "options": ["--debug", "--output", "--help"]}
)
CONNECTION_OPTIONS = cast(
    OptionGroupDict, {"name": "Connection options", "options": ["--endpoint"]}
)
click.rich_click.OPTION_GROUPS = {
    "armonik": [
        {
            "name": "Common options",
            "options": ["--version", "--help"],
        }
    ],
    "armonik session create": [
        {
            "name": "Session configuration options",
            "options": [
                "--max-duration",
                "--max-retries",
                "--priority",
                "--partition",
                "--default-partition",
                "--application-name",
                "--application-version",
                "--application-namespace",
                "--application-service",
                "--engine-type",
                "--option",
            ],
        },
        CONNECTION_OPTIONS,
        COMMON_OPTIONS,
    ],
    "armonik session list": [CONNECTION_OPTIONS, COMMON_OPTIONS],
    "armonik session get": [CONNECTION_OPTIONS, COMMON_OPTIONS],
    "armonik session pause": [CONNECTION_OPTIONS, COMMON_OPTIONS],
    "armonik session resume": [CONNECTION_OPTIONS, COMMON_OPTIONS],
    "armonik session delete": [CONNECTION_OPTIONS, COMMON_OPTIONS],
    "armonik session cancel": [CONNECTION_OPTIONS, COMMON_OPTIONS],
    "armonik session purge": [CONNECTION_OPTIONS, COMMON_OPTIONS],
    "armonik session close": [CONNECTION_OPTIONS, COMMON_OPTIONS],
    "armonik session stop-submission": [
        {
            "name": "Submission interruption options",
            "options": ["--clients-only", "--workers-only"],
        },
        CONNECTION_OPTIONS,
        COMMON_OPTIONS,
    ],
}


@click.group(name="armonik", context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(version=__version__, prog_name="armonik")
@click.pass_context
def cli(ctx: click.Context) -> None:
    """
    ArmoniK CLI is a tool to monitor and manage ArmoniK clusters.
    """
    if "help" not in ctx.args:
        if not Configuration.default_path.exists():
            console.print(f"Created configuration file at {Configuration.default_path}")
            Configuration.create_default_if_not_exists()


cli.add_command(commands.session)
cli.add_command(commands.config)
