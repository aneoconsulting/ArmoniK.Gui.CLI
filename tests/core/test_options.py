import pytest
import rich_click as click

from click.testing import CliRunner

from armonik_cli.core.options import MutuallyExclusiveOption


@click.command()
@click.option("--option-a", cls=MutuallyExclusiveOption, mutual=["option_b"])
@click.option("--option-b", cls=MutuallyExclusiveOption, mutual=["option_a"])
def dummy_command(option_a, option_b):
    pass


@pytest.mark.parametrize("cmd", ["--option-a value_a", "--option-b value_b"])
def test_no_conflict(cmd):
    runner = CliRunner()
    result = runner.invoke(dummy_command, cmd.split())
    assert result.exit_code == 0


def test_mutual_exclusion_error():
    runner = CliRunner()
    result = runner.invoke(dummy_command, ["--option-a", "value_a", "--option-b", "value_b"])
    assert result.exit_code == 2
    assert "Illegal usage: `option_a` cannot be used together with 'option_b'." in result.output
