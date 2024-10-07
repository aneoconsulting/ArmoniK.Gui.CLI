import pytest

from grpc import RpcError, StatusCode

from armonik_cli.errors import error_handler, NotFoundError, InternalError


class DummyRpcError(RpcError):
    def __init__(self, code, details):
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


@pytest.mark.parametrize(("exception", "code"),
    [
        (NotFoundError, StatusCode.NOT_FOUND),
        (InternalError, StatusCode.UNAVAILABLE)
    ]
)
def test_error_handler_rpc_error(exception, code):
    @error_handler
    def raise_error(code, details):
        raise DummyRpcError(code=code, details=details)

    with pytest.raises(exception):
        raise_error(code, "")


def test_error_handler_other_no_debug():
    @error_handler
    def raise_error():
        raise ValueError()

    with pytest.raises(InternalError):
        raise_error()


def test_error_handler_other_debug():
    @error_handler
    def raise_error(debug=None):
        raise ValueError()

    raise_error(debug=True)
