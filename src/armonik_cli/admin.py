"""ArmoniK Admin CLI to perform admnistration tasks for ArmoniK

Usage:
  armonik.py cancel-session list <session>... [--endpoint ENDPOINT] [--ca CA_FILE] [--cert CERT_FILE] [--key KEY_FILE]
  armonik.py cancel-session --running         [--endpoint ENDPOINT] [--ca CA_FILE] [--cert CERT_FILE] [--key KEY_FILE]
  armonik.py cancel-task <task>...            [--endpoint ENDPOINT] [--ca CA_FILE] [--cert CERT_FILE] [--key KEY_FILE]
  armonik.py list-task <session>...           [--endpoint ENDPOINT] [--ca CA_FILE] [--cert CERT_FILE] [--key KEY_FILE] [--all | --creating | --error] [--page PAGE] [--page_size PAGE_SIZE]
  armonik.py list-session                     [--endpoint ENDPOINT] [--ca CA_FILE] [--cert CERT_FILE] [--key KEY_FILE] [--all | --running | --cancelled]
  armonik.py (-h | --help)
  armonik.py --version
  armonik.py check-task <taskid>


Options:
  -h --help                         Show this screen.
  --version                         Show version.
  -e ENDPOINT --endpoint ENDPOINT   ArmoniK control plane endpoint. [default: localhost:5001]
  --ca CA_FILE                      CA file for mutual tls
  --cert CERT_FILE                  Certificate for mutual tls
  --key KEY_FILE                    Private key for mutual tls
  --all                             Select all sessions or tasks
  --running                         Select running sessions
  --cancelled                       Select cancelled sessions
  --session SESSION                 Select tasks from SESSION
  --creating                        Select creating tasks
  --error                           Select error tasks
  --page PAGE                       Page number [default: 0]
  --page_size PAGE_SIZE             Page size [default: 1000]
"""

from docopt import docopt
import grpc
import math
from armonik.client.sessions import ArmoniKSessions, SessionFieldFilter
from armonik.client.tasks import ArmoniKTasks, TaskFieldFilter
from armonik.common.enumwrapper import TASK_STATUS_ERROR, TASK_STATUS_COMPLETED, TASK_STATUS_CREATING , SESSION_STATUS_RUNNING, SESSION_STATUS_CANCELLED, SESSION_STATUS_UNSPECIFIED


def create_channel(arguments):
    """
    Create a gRPC channel for communication with the ArmoniK control plane

    Args:
        arguments (dict): command-line arguments from docopt

    Returns:
        grpc.Channel: gRPC channel for communication
    """
    if(arguments["--ca"] != None and arguments["--cert"] != None and arguments["--key"] != None):
        ca = open(arguments["--ca"], 'rb').read()
        cert = open(arguments["--cert"], 'rb').read()
        key = open(arguments["--key"], 'rb').read()
        credentials = grpc.ssl_channel_credentials(ca, key, cert)
        return grpc.secure_channel(arguments["--endpoint"], credentials)
    else:
        return grpc.insecure_channel(arguments["--endpoint"])


def list_sessions(client: ArmoniKSessions, all: bool, running: bool, cancelled: bool):
    """
    List sessions with filter options

    Args:
        client (ArmoniKSessions): ArmoniKSessions instance for session management
        all (bool): Show all sessions
        running (bool): Show only running sessions
        cancelled (bool): Show only cancelled sessions
    """
    session_filter = None

    if all:
        session_filter = (SessionFieldFilter.STATUS == SESSION_STATUS_RUNNING) | (SessionFieldFilter.STATUS == SESSION_STATUS_CANCELLED)
    elif running:
        session_filter = SessionFieldFilter.STATUS == SESSION_STATUS_RUNNING
    elif cancelled:
        session_filter = SessionFieldFilter.STATUS == SESSION_STATUS_CANCELLED
    else:
        print("SELECT ARGUMENT [--all | --running | --cancelled]")
        return

    number_sessions, sessions = client.list_sessions(session_filter)
    print(f'Number of sessions: {number_sessions}\nSessions: {[session.session_id for session in sessions]}')


def cancel_sessions(client: ArmoniKSessions, sessions: list):
    """
    Cancel sessions with a list of session IDs or all sessions running

    Args:
        client (ArmoniKSessions): Instance of the class with cancel_session method
        sessions (list): List of session IDs to cancel
    """
    if sessions:
        for session_id in sessions:
            client.cancel_session(session_id)


def list_tasks(client: ArmoniKTasks, session_ids: list, all: bool, creating: bool , error: bool, page: int, page_size: int):
    """
    List tasks associated with the specified sessions based on filter options

    Args:
        client (ArmoniKTasks): ArmoniKTasks instance for task management
        session_ids (list): List of session IDs to filter tasks
        all (bool): List all tasks regardless of status
        creating (bool): List only tasks in creating status
        error (bool): List only tasks in error status
        page (int): Select a specified page
        page_size (int): Display a number of tasks per pages
    """
    for session_id in session_ids:
        if all:
            nb_tasks, task_list = client.list_tasks(TaskFieldFilter.SESSION_ID == session_id, page=page, page_size=page_size)
        elif creating:
            nb_tasks, task_list = client.list_tasks((TaskFieldFilter.SESSION_ID == session_id) &
                                                          (TaskFieldFilter.STATUS == TASK_STATUS_CREATING), page=page, page_size=page_size)
        elif error:
            nb_tasks, task_list = client.list_tasks((TaskFieldFilter.SESSION_ID == session_id) &
                                                          (TaskFieldFilter.STATUS == TASK_STATUS_ERROR), page=page, page_size=page_size)
        else:
            print("SELECT ARGUMENT [--all | --creating | --error]")
            return

        for task in task_list:
            print(f'{task.id}  status: {task.status}')
        total_pages = max(1, math.ceil(nb_tasks / page_size))
        print(f"\nTasks for session {session_id}:")
        print(f"Total tasks: {nb_tasks}\nTasks: {len(task_list)}\n")
        print(f'Page {page}/{total_pages - 1}')


def check_task(client: ArmoniKTasks, task_id: str):
    """
    Check the status of a task based on its ID.

    Args:
        client (ArmoniKTasks): ArmoniKTasks instance for task management.
        task_id (str): ID of the task to check.
    """
    _, task_list = client.list_tasks(TaskFieldFilter.TASK_ID == task_id)
    if len(task_list) > 0:
        print(f"\nTask information for task ID {task_id} :\n")
        print(task_list)
    else:
        print(f"No task found with ID {task_id}")


def main():
    arguments = docopt(__doc__, version="ArmoniK Admin CLI  0.0.1")
    grpc_channel = create_channel(arguments)
    session_client = ArmoniKSessions(grpc_channel)
    task_client = ArmoniKTasks(grpc_channel)

    if arguments['list-session']:
        list_sessions(session_client, arguments["--all"], arguments["--running"], arguments["--cancelled"])

    if arguments['list-task']:
        session_ids = arguments["<session>"]
        list_tasks(task_client, session_ids, arguments["--all"], arguments["--creating"], arguments["--error"], page=int(arguments["--page"]), page_size=int(arguments["--page_size"]))

    if arguments['check-task']:
        task_id = arguments["<taskid>"]
        check_task(task_client, task_id)

    if arguments['cancel-session']:
        if arguments["list"]:
            cancel_sessions(session_client, arguments["<session>"])
        if arguments["--running"]:
            _, session_list = session_client.list_sessions(SessionFieldFilter.STATUS == SESSION_STATUS_RUNNING)
            session_ids = [session.session_id for session in session_list]
            cancel_sessions(session_client, session_ids)

if __name__ == '__main__':
    main()
