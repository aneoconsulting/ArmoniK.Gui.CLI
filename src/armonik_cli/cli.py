import rich_click as click

from armonik import __version__ as api_version
from rich.text import Text
from rich.style import Style

from armonik_cli import commands, __version__ as cli_version
from armonik_cli.core import console


ascii_cli_name = """
 ░░░░░╗ ░░░░░░╗ ░░░╗   ░░░╗ ░░░░░░╗ ░░░╗   ░░╗░░╗░░╗  ░░╗
░░╔══░░╗░░╔══░░╗░░░░╗ ░░░░║░░╔═══░░╗░░░░╗  ░░║░░║░░║ ░░╔╝
▒▒▒▒▒▒▒║▒▒▒▒▒▒╔╝▒▒╔▒▒▒▒╔▒▒║▒▒║   ▒▒║▒▒╔▒▒╗ ▒▒║▒▒║▒▒▒▒▒╔╝ 
▓▓╔══▓▓║▓▓╔══▓▓╗▓▓║╚▓▓╔╝▓▓║▓▓║   ▓▓║▓▓║╚▓▓╗▓▓║▓▓║▓▓╔═▓▓╗ 
██║  ██║██║  ██║██║ ╚═╝ ██║╚██████╔╝██║ ╚████║██║██║  ██╗
╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝╚═╝  ╚═╝
"""


def version_callback(ctx: click.Context, param: click.Parameter, value: bool) -> None:
    """Callback function for the `--version` option.
    Displays the ASCII logo of the CLI, along with the current CLI and API versions,
    and exits the program.

    Args:
        ctx: The current Click context.
        param: The parameter associated with the callback.
        value: The flag value indicating if the `--version` option was set.

    Returns:
        None: This function does not return any value, but it exits the program after printing the version information.
    """
    if not value or ctx.resilient_parsing:
        return

    color = "#FE5100"
    console.print(Text(ascii_cli_name, style=f"bold {color}"))
    console.print(Text("-" * (console.width // 2), style=Style(color=color)))
    console.print(
        Text("CLI version: ", style="bold white").append(cli_version, style=f"bold {color}")
    )
    console.print(
        Text("API version: ", style="bold white").append(api_version, style=f"bold {color}")
    )
    ctx.exit()


@click.group(name="armonik")
@click.option(
    "--version",
    "-V",
    is_flag=True,
    expose_value=False,
    is_eager=True,
    help="Show the version and exit.",
    callback=version_callback,
)
def cli() -> None:
    """
    ArmoniK CLI is a tool to monitor and manage ArmoniK clusters.
    """
    pass


cli.add_command(commands.sessions)
