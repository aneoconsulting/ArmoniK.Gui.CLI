import json

import pytest

from datetime import timedelta, datetime

from armonik.common import Session, TaskOptions, SessionStatus

from armonik_cli.core.serialize import CLIJSONEncoder


@pytest.mark.parametrize(
    ("obj", "obj_dict"),
    [
        (
            TaskOptions(
                max_duration=timedelta(minutes=5),
                priority=1,
                max_retries=2,
                partition_id="default",
                application_name="app",
                application_namespace="ns",
                application_service="svc",
                application_version="v1",
                engine_type="eng",
                options={"k1": "v1", "k2": "v2"},
            ),
            {
                "MaxDuration": "0:05:00",
                "Priority": 1,
                "MaxRetries": 2,
                "PartitionId": "default",
                "ApplicationName": "app",
                "ApplicationNamespace": "ns",
                "ApplicationService": "svc",
                "ApplicationVersion": "v1",
                "EngineType": "eng",
                "Options": {"k1": "v1", "k2": "v2"},
            },
        ),
        (
            Session(
                session_id="id",
                status=SessionStatus.RUNNING,
                client_submission=True,
                worker_submission=False,
                partition_ids=["default"],
                options=None,
                created_at=datetime(year=2024, month=11, day=11),
                cancelled_at=None,
                closed_at=None,
                purged_at=None,
                deleted_at=None,
                duration=timedelta(hours=1),
            ),
            {
                "SessionId": "id",
                "Status": 1,
                "ClientSubmission": True,
                "WorkerSubmission": False,
                "PartitionIds": ["default"],
                "Options": None,
                "CreatedAt": "2024-11-11 00:00:00",
                "CancelledAt": None,
                "ClosedAt": None,
                "PurgedAt": None,
                "DeletedAt": None,
                "Duration": "1:00:00",
            },
        ),
    ],
)
def test_serialize(obj, obj_dict):
    assert obj_dict == json.loads(json.dumps(obj, cls=CLIJSONEncoder))
