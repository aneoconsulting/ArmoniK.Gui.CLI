import pytest

from armonik.common import Session, SessionStatus
from armonik.common.filter import SessionFilter

from armonik_cli.core.filters import FilterParser


@pytest.mark.parametrize(
    ("args", "expr", "filter"),
    [
        ((Session, SessionFilter, SessionStatus), "session_id = id", Session.session_id == "id"),
        ((Session, SessionFilter, SessionStatus), "session_id != id", Session.session_id != "id"),
        (
            (Session, SessionFilter, SessionStatus),
            "session_id startswith id",
            Session.session_id.startswith("id"),
        ),
        (
            (Session, SessionFilter, SessionStatus),
            "session_id endswith id",
            Session.session_id.endswith("id"),
        ),
        (
            (Session, SessionFilter, SessionStatus),
            "session_id contains id",
            Session.session_id.contains("id"),
        ),
        (
            (Session, SessionFilter, SessionStatus),
            "session_id notcontains id",
            -Session.session_id.contains("id"),
        ),
        (
            (Session, SessionFilter, SessionStatus),
            "status = running",
            Session.status == SessionStatus.RUNNING,
        ),
        (
            (Session, SessionFilter, SessionStatus),
            "status != running",
            Session.status != SessionStatus.RUNNING,
        ),
        (
            (Session, SessionFilter, SessionStatus),
            "session_id = id and status != running",
            (Session.session_id == "id") & (Session.status != SessionStatus.RUNNING),
        ),
        (
            (Session, SessionFilter, SessionStatus),
            "session_id = id or status != running",
            (Session.session_id == "id") | (Session.status != SessionStatus.RUNNING),
        ),
        (
            (Session, SessionFilter, SessionStatus),
            "session_id = id and status != running or session_id startswith ok",
            (Session.session_id == "id") & (Session.status != SessionStatus.RUNNING)
            | (Session.session_id.startswith("ok")),
        ),
    ],
)
def test_filter_parser(args, expr, filter):
    assert FilterParser(*args).parse(expr).to_dict() == filter.to_dict()
