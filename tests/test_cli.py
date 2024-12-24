import pytest

from conftest import run_cmd_and_assert_exit_code


def test_armonik_version():
    result = run_cmd_and_assert_exit_code("--version")
    assert result.output.startswith("armonik, version ")


@pytest.mark.parametrize("flag", ["--help", "-h"])
def test_armonik_help(flag):
    run_cmd_and_assert_exit_code(flag)
