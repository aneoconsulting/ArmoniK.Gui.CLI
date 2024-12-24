import pytest

from conftest import run_cmd_and_assert_exit_code


@pytest.mark.parametrize("flag", ["-V", "--version"])
def test_armonik_version(flag):
    result = run_cmd_and_assert_exit_code(flag)
    output = result.output.split("\n")[:-1]
    assert output[-1].startswith("API version: ")
    assert output[-2].startswith("CLI version: ")


def test_armonik_help():
    run_cmd_and_assert_exit_code("--help")
