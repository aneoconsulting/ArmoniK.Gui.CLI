from conftest import run_cmd_and_assert_exit_code


def test_armonik_version():
    result = run_cmd_and_assert_exit_code("--version")
    assert result.output.startswith("armonik, version ")


def test_armonik_help():
    run_cmd_and_assert_exit_code("--help")
