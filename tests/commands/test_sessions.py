import pytest

from armonik.client import ArmoniKSessions
from armonik.common import Session
from click.testing import CliRunner

from armonik_cli.commands.sessions import list


@pytest.mark.parametrize(
    ("args", "mock_return", "output_id"),
    [
        (["--endpoint", "endpoint"], (0, []), "sessions_list_empty"),
        (
            ["--endpoint", "endpoint", "-o", "json"],
            (1, [Session(session_id="id")]),
            "sessions_list",
        ),
    ],
)
def test_armonik_sessions_list(mocker, cmd_outputs, args, mock_return, output_id):
    mocker.patch.object(ArmoniKSessions, "list_sessions", return_value=mock_return)
    runner = CliRunner()
    result = runner.invoke(list, args)
    assert result.exit_code == 0
    assert result.output == cmd_outputs[output_id]
