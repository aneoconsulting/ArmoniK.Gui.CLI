from click.testing import CliRunner

from armonik_cli import cli


def test_armonik_version():
    runner = CliRunner()
    result = runner.invoke(cli.cli, ["--version"])
    assert result.exit_code == 0
    assert result.output.startswith("armonik, version ")
