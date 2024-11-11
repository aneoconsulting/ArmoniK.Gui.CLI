import pytest

from armonik_cli.core import Configuration

from conftest import run_cmd_and_assert_exit_code, reformat_cmd_output


@pytest.mark.parametrize(
    ("cmd", "has_return", "get_return", "output"),
    [
        ("config get endpoint", True, "endpoint", {"endpoint": "endpoint"}),
        ("config get not", False, None, "Warning: 'not' is not a known configuration key."),
    ],
)
def test_config_get(mocker, cmd, has_return, get_return, output):
    mocker.patch.object(Configuration, "has", return_value=has_return)
    mocker.patch.object(Configuration, "get", return_value=get_return)
    result = run_cmd_and_assert_exit_code(cmd)
    assert reformat_cmd_output(result.output, deserialize=has_return) == output


@pytest.mark.parametrize(
    ("cmd", "has_return", "output"),
    [
        ("config set endpoint value", True, "Updated 'endpoint' configuration with value 'value'."),
        ("config set not value", False, "Warning: 'not' is not a known configuration key."),
    ],
)
def test_config_set(mocker, cmd, has_return, output):
    mocker.patch.object(Configuration, "has", return_value=has_return)
    result = run_cmd_and_assert_exit_code(cmd)
    assert reformat_cmd_output(result.output) == output


def test_config_list(mocker):
    mock_config = {"config": "config"}
    mocker.patch.object(Configuration, "to_dict", return_value=mock_config)
    result = run_cmd_and_assert_exit_code("config list")
    assert reformat_cmd_output(result.output, deserialize=True) == mock_config


def test_config_set_connection():
    pass
