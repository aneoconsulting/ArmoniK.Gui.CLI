import sys
import argparse
import grpc
from armonik.client.sessions import ArmoniKSessions, SessionFieldFilter
from armonik.client.tasks import ArmoniKTasks, TaskFieldFilter
from armonik.common.enumwrapper import TASK_STATUS_ERROR, TASK_STATUS_CREATING , SESSION_STATUS_RUNNING, SESSION_STATUS_CANCELLED
from armonik.common.filter import Filter


def create_channel(endpoint: str,  ca: str, key: str, cert: str) -> grpc.Channel:
    """
    Create a gRPC channel for communication with the ArmoniK control plane

    Args:
        ca (str): CA file path for mutual TLS
        cert (str): Certificate file path for mutual TLS
        key (str): Private key file path for mutual TLS
        endpoint (str): ArmoniK control plane endpoint

    Returns:
        grpc.Channel: gRPC channel for communication
    """    
    try:
        if ca:
            with open(ca, 'rb') as ca_file:
                ca_data = ca_file.read()
            if cert and key:
                with open(cert, 'rb') as cert_file, open(key, 'rb') as key_file:
                    key_data = key_file.read()
                    cert_data = cert_file.read()
            else:
                key_data = None
                cert_data = None

            credentials = grpc.ssl_channel_credentials(ca_data, key_data, cert_data)
            return grpc.secure_channel(endpoint, credentials)
        else:
            return grpc.insecure_channel(endpoint)
    except FileNotFoundError as e:
        print(e)
        sys.exit(1)    



def list_sessions(client: ArmoniKSessions, session_filter: Filter):
    """
    List sessions with filter options

    Args:
        client (ArmoniKSessions): ArmoniKSessions instance for session management
        session_filter (Filter) : Filter for the session
    """
    page = 0
    sessions = client.list_sessions(session_filter, page=page)
    
    while len(sessions[1]) > 0:
        for session in sessions[1]:
            print(f'Session ID: {session.session_id}')
        page += 1
        sessions = client.list_sessions(session_filter, page=page)

    print(f'\nNumber of sessions: {sessions[0]}\n')


def cancel_sessions(client: ArmoniKSessions, sessions: list):
    """
    Cancel sessions with a list of session IDs or all sessions running

    Args:
        client (ArmoniKSessions): Instance of the class with cancel_session method
        sessions (list): List of session IDs to cancel
    """
    for session_id in sessions:
        try:
            client.cancel_session(session_id)
            print(f"Session {session_id} canceled successfully")
        except grpc._channel._InactiveRpcError as error:
            print(f"Error for canceling session {session_id}: {error.details()}")


def create_task_filter(session_id: str, all: bool , creating: bool, error: bool) -> Filter:
    """
    Create a task Filter based on the provided options

    Args:
        session_id (str): Session ID to filter tasks
        all (bool): List all tasks regardless of status
        creating (bool): List only tasks in creating status
        error (bool): List only tasks in error status

    Returns:
        Filter object
    """
    if all:
        tasks_filter = TaskFieldFilter.SESSION_ID == session_id
    elif creating:
        tasks_filter = (TaskFieldFilter.SESSION_ID == session_id) & (TaskFieldFilter.STATUS == TASK_STATUS_CREATING)
    elif error:
        tasks_filter = (TaskFieldFilter.SESSION_ID == session_id) & (TaskFieldFilter.STATUS == TASK_STATUS_ERROR)
    else:
            raise ValueError("SELECT ARGUMENT [--all | --creating | --error]")

    return tasks_filter
    

def list_tasks(client: ArmoniKTasks, task_filter: Filter):
    """
    List tasks associated with the specified sessions based on filter options

    Args:
        client (ArmoniKTasks): ArmoniKTasks instance for task management
        task_filter (Filter): Filter for the task
    """

    page = 0
    tasks = client.list_tasks(task_filter, page=page)
    while len(tasks[1]) > 0:
        for task in tasks[1]:
            print(f'Task ID: {task.id}')
        page += 1
        tasks = client.list_tasks(task_filter, page=page)

    print(f"\nTotal tasks: {tasks[0]}\n")

def check_task(client: ArmoniKTasks, task_ids: list):
    """
    Check the status of a task based on its ID.

    Args:
        client (ArmoniKTasks): ArmoniKTasks instance for task management.
        task_id (str): ID of the task to check.
    """
    for task_id in task_ids:
        tasks = client.list_tasks(TaskFieldFilter.TASK_ID == task_id)
        if len(tasks[1]) > 0:
            print(f"\nTask information for task ID {task_id} :\n")
            print(tasks[1])
        else:
            print(f"No task found with ID {task_id}")



def main():

    parser = argparse.ArgumentParser(description="ArmoniK Admin CLI to perform administration tasks for ArmoniK")
    parser.add_argument("-v","--version", action="version", version="ArmoniK Admin CLI 0.0.1")
    parser.add_argument("--endpoint", default="localhost:5001", help="ArmoniK control plane endpoint")
    parser.add_argument("--ca", help="CA file for mutual TLS")
    parser.add_argument("--cert", help="Certificate for mutual TLS")
    parser.add_argument("--key", help="Private key for mutual TLS")
    parser.set_defaults(func=lambda _: parser.print_help())

    subparsers = parser.add_subparsers()

    list_session_parser = subparsers.add_parser('list-session', help='List sessions with specific filters')
    group_list_session = list_session_parser.add_mutually_exclusive_group(required=True)
    group_list_session.add_argument("--all", dest="filter", action="store_const", const=(SessionFieldFilter.STATUS == SESSION_STATUS_RUNNING) | (SessionFieldFilter.STATUS == SESSION_STATUS_CANCELLED) , help="Select all sessions")
    group_list_session.add_argument("--running", dest="filter", action="store_const", const=SessionFieldFilter.STATUS == SESSION_STATUS_RUNNING, help="Select running sessions")
    group_list_session.add_argument("--cancelled", dest="filter", action="store_const", const=SessionFieldFilter.STATUS == SESSION_STATUS_CANCELLED, help="Select cancelled sessions")
    group_list_session.set_defaults(func=lambda args: list_sessions(session_client, args.filter))
 
    
    list_task_parser = subparsers.add_parser('list-task', help='List tasks with specific filters')
    list_task_parser.add_argument(dest="session_id", help="Select ID from SESSION")

    group_list_task = list_task_parser.add_mutually_exclusive_group(required=True)
    group_list_task.add_argument("--all", dest="all", action="store_true", help="Select all tasks")
    group_list_task.add_argument("--creating", dest="creating", action="store_true", help="Select creating tasks")
    group_list_task.add_argument("--error", dest="error", action="store_true", help="Select error tasks")
    group_list_task.set_defaults(func=lambda args: list_tasks(task_client, create_task_filter(args.session_id, args.all, args.creating, args.error)))

    check_task_parser = subparsers.add_parser('check-task', help='Check the status of a specific task')
    check_task_parser.add_argument(dest="task_ids", nargs="+",  help="Select ID from TASK")
    check_task_parser.set_defaults(func=lambda args: check_task(task_client, args.task_ids))


    cancel_session_parser = subparsers.add_parser('cancel-session', help='Cancel sessions')
    cancel_session_parser.add_argument(dest="session_ids", nargs="+", help="Session IDs to cancel")
    cancel_session_parser.set_defaults(func=lambda args: cancel_sessions(session_client, args.session_ids))

    args = parser.parse_args()
    grpc_channel = create_channel(args.endpoint, args.ca, args.key, args.cert)
    task_client = ArmoniKTasks(grpc_channel)
    session_client = ArmoniKSessions(grpc_channel)
    args.func(args)


if __name__ == '__main__':
    main()
