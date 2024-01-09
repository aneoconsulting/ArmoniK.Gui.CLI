import datetime
from .conftest import rpc_called, get_client, grpc_endpoint
import armonik_cli.admin as admin
from armonik.client import ArmoniKSessions
from armonik.common import TaskOptions
from armonik.common.enumwrapper import SessionStatus
from armonik.client.sessions import SessionFieldFilter



def test_create_session():
    sessions_client: ArmoniKSessions = get_client("Sessions")
    default_task_options = TaskOptions(
        max_duration=datetime.timedelta(seconds=1),
        priority=1,
        max_retries=1
    )
    session_id = sessions_client.create_session(default_task_options)

    assert rpc_called("Sessions", "CreateSession")
    assert session_id == "session-id"

    # TODO: Mock must be updated to return something and so that changes the following assertions (empty [])

def test_list_sessions_all():
    sessions = admin.list_sessions(admin.create_session_client(grpc_endpoint), None)
    assert rpc_called("Sessions", "ListSessions", 1)
    assert sessions == []

def test_list_sessions_running():
    sessions = admin.list_sessions(admin.create_session_client(grpc_endpoint), SessionFieldFilter.STATUS == SessionStatus.RUNNING)
    assert rpc_called("Sessions", "ListSessions", 2)
    assert sessions == []

def test_list_sessions_cancelled():
    sessions = admin.list_sessions(admin.create_session_client(grpc_endpoint), SessionFieldFilter.STATUS == SessionStatus.CANCELLED)
    assert rpc_called("Sessions", "ListSessions", 3)
    assert sessions == []

def test_cancel_single_session():
    admin.cancel_sessions(admin.create_session_client(grpc_endpoint), ["session-id"] )
    assert rpc_called("Sessions", "CancelSession")

def test_cancel_multiple_sessions():
    admin.cancel_sessions(admin.create_session_client(grpc_endpoint), ["session-id1","session-id2","session-id3"] )
    assert rpc_called("Sessions", "CancelSession", 4)

def test_list_tasks_all():
    tasks = admin.list_tasks(admin.create_task_client(grpc_endpoint), admin.create_task_filter("session-id", True, False, False) )
    assert rpc_called("Tasks", "ListTasksDetailed")
    assert tasks == []

def test_list_tasks_creating():
    tasks = admin.list_tasks(admin.create_task_client(grpc_endpoint), admin.create_task_filter("session-id", True, False, False) )
    assert rpc_called("Tasks", "ListTasksDetailed", 2)
    assert tasks == []

def test_list_tasks_error():
    tasks = admin.list_tasks(admin.create_task_client(grpc_endpoint), admin.create_task_filter("session-id", True, False, False) )
    assert rpc_called("Tasks", "ListTasksDetailed", 3)
    assert tasks == []

def test_check_single_task():
    task = admin.check_task(admin.create_task_client(grpc_endpoint), ["task-id"])
    assert rpc_called("Tasks", "GetTask")
    assert task[0].id == "task-id"

def test_check_multiple_task():
    task = admin.check_task(admin.create_task_client(grpc_endpoint), ["task-id", "task-id", "task-id"])
    assert rpc_called("Tasks", "GetTask", 4)
    assert task[0].id == "task-id"
    assert task[1].id == "task-id"
    assert task[2].id == "task-id"

def test_check_non_existing_task():
    task = admin.check_task(admin.create_task_client(grpc_endpoint), ["non-existing-id"])
    assert rpc_called("Tasks", "GetTask", 5)
    assert task == []

