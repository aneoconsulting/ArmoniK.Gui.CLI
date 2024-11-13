import pytest

from grpc import RpcError, StatusCode

from armonik_cli.core.decorators import error_handler, base_command
from armonik_cli.exceptions import NotFoundError, InternalError


class DummyRpcError(RpcError):
    def __init__(self, code, details):
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


@pytest.mark.parametrize(
    ("exception", "code"),
    [(NotFoundError, StatusCode.NOT_FOUND), (InternalError, StatusCode.UNAVAILABLE)],
)
def test_error_handler_rpc_error(exception, code):
    @error_handler
    def raise_error(code, details):
        raise DummyRpcError(code=code, details=details)

    with pytest.raises(exception):
        raise_error(code, "")


@pytest.mark.parametrize("decorator", [error_handler, error_handler()])
def test_error_handler_other_no_debug(decorator):
    @decorator
    def raise_error():
        raise ValueError()

    with pytest.raises(InternalError):
        raise_error()


@pytest.mark.parametrize("decorator", [error_handler, error_handler()])
def test_error_handler_other_debug(decorator):
    @decorator
    def raise_error(debug=None):
        raise ValueError()

    raise_error(debug=True)


@pytest.mark.parametrize(
    ("decorator", "connection_args"),
    [(base_command, True), (base_command(), True), (base_command(connection_args=False), False)],
)
def test_base_command(decorator, connection_args):
    @decorator
    def test_func():
        pass

    assert test_func.__name__ == "test_func"
    assert len(test_func.__click_params__) == 7 if connection_args else 2
    assert (
        sorted([param.name for param in test_func.__click_params__])
        == ["ca", "cert", "debug", "endpoint", "key", "optional_config_file", "output"]
        if connection_args
        else ["debug", "output"]
    )
