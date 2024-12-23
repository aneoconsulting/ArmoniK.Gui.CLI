import pytest

from pydantic import HttpUrl

from armonik_cli.settings import Settings

from conftest import run_cmd_and_assert_exit_code, reformat_cmd_output


@pytest.mark.parametrize(
    ("cmd", "has_return", "get_return", "output"),
    [
        ("config get endpoint", HttpUrl("endpoint"), {"endpoint": "endpoint"}),
        ("config get not", None, "Warning: 'not' is not a known configuration key."),
    ],
)
def test_config_get(mocker, cmd, get_return, output):
    mocker.patch.object(Settings, "get_field", return_value=get_return)
    result = run_cmd_and_assert_exit_code(cmd)
    assert reformat_cmd_output(result.output, deserialize=has_return) == output


@pytest.mark.parametrize(
    ("cmd", "key_exists", "output"),
    [
        ("config set endpoint value", True, "Updated 'endpoint' configuration with value 'value'."),
        ("config set not value", False, "Warning: 'not' is not a known configuration key."),
    ],
)
def test_config_set(mocker, cmd, key_exists, output):
    mocker.patch.object(Configuration, "has", return_value=has_return)
    result = run_cmd_and_assert_exit_code(cmd)
    assert reformat_cmd_output(result.output) == output


def test_config_list(mocker):
    mock_config = {"config": "config"}
    mocker.patch.object(Configuration, "to_dict", return_value=mock_config)
    result = run_cmd_and_assert_exit_code("config list")
    assert reformat_cmd_output(result.output, deserialize=True) == mock_config
