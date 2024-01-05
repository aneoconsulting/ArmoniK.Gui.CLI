import datetime
import requests
import grpc
import os
import pytest
# from .conftest import all_rpc_called, rpc_called, get_client
import armonik_cli.admin as admin
from armonik.client import ArmoniKResults, ArmoniKSubmitter, ArmoniKTasks, ArmoniKSessions
from armonik.common import Session, TaskOptions
from armonik.common.enumwrapper import SessionStatus
from armonik.common.enumwrapper import TASK_STATUS_ERROR, TASK_STATUS_CREATING , SESSION_STATUS_RUNNING, SESSION_STATUS_CANCELLED, SESSION_STATUS_UNSPECIFIED
from armonik.common.filter import Filter
from typing import List
from armonik.client.sessions import SessionFieldFilter



grpc_endpoint = "localhost:5001"
calls_endpoint = "http://localhost:5000/calls.json"
reset_endpoint = "http://localhost:5000/reset"
data_folder = os.getcwd()

@pytest.fixture(scope="session", autouse=True)
def clean_up(request):
    """
    This fixture runs at the session scope and is automatically used before and after
    running all the tests. It set up and teardown the testing environments by:
        - creating dummy files before testing begins;
        - clear files after testing;
        - resets the mocking gRPC server counters to maintain a clean testing environment.

    Yields:
        None: This fixture is used as a context manager, and the test code runs between
        the 'yield' statement and the cleanup code.

    Raises:
        requests.exceptions.HTTPError: If an error occurs when attempting to reset
        the mocking gRPC server counters.
    """
    # Write dumm payload and data dependency to files for testing purposes
    with open(os.path.join(data_folder, "payload-id"), "wb") as f:
        f.write("payload".encode())
    with open(os.path.join(data_folder, "dd-id"), "wb") as f:
        f.write("dd".encode())

    # Run all the tests
    yield

    # Remove the temporary files created for testing
    os.remove(os.path.join(data_folder, "payload-id"))
    os.remove(os.path.join(data_folder, "dd-id"))

    # Reset the mock server counters
    try:
        response = requests.post(reset_endpoint)
        response.raise_for_status()
        print("\nMock server resetted.")
    except requests.exceptions.HTTPError as e:
        print("An error occurred when resetting the server: " + str(e))

def get_client(client_name: str, endpoint: str = grpc_endpoint) -> [ArmoniKResults, ArmoniKSubmitter, ArmoniKTasks, ArmoniKSessions]:
    """
    Get the ArmoniK client instance based on the specified service name.

    Args:
        client_name (str): The name of the ArmoniK client to retrieve.
        endpoint (str, optional): The gRPC server endpoint. Defaults to grpc_endpoint.

    Returns:
        Union[ArmoniKResults, ArmoniKSubmitter, ArmoniKTasks, ArmoniKSessions, ARmoniKPartitions, AgentStub]:
            An instance of the specified ArmoniK client.

    Raises:
        ValueError: If the specified service name is not recognized.

    Example:
        >>> result_service = get_service("Results")
        >>> submitter_service = get_service("Submitter", "custom_endpoint")
    """
    channel = grpc.insecure_channel(endpoint).__enter__()
    match client_name:
        case "Results":
            return ArmoniKResults(channel)
        case "Submitter":
            return ArmoniKSubmitter(channel)
        case "Tasks":
            return ArmoniKTasks(channel)
        case "Sessions":
            return ArmoniKSessions(channel)
        case _:
            raise ValueError("Unknown service name: " + str(service_name))

def rpc_called(service_name: str, rpc_name: str, n_calls: int = 1, endpoint: str = calls_endpoint) -> bool:
    """Check if a remote procedure call (RPC) has been made a specified number of times.
    This function uses ArmoniK.Api.Mock. It just gets the '/calls.json' endpoint.

    Args:
        service_name (str): The name of the service providing the RPC.
        rpc_name (str): The name of the specific RPC to check for the number of calls.
        n_calls (int, optional): The expected number of times the RPC should have been called. Default is 1.
        endpoint (str, optional): The URL of the remote service providing RPC information. Default to
            calls_endpoint.

    Returns:
        bool: True if the specified RPC has been called the expected number of times, False otherwise.

    Raises:
        requests.exceptions.RequestException: If an error occurs when requesting ArmoniK.Api.Mock.

    Example:
    >>> rpc_called('http://localhost:5000/calls.json', 'Versions', 'ListVersionss', 0)
    True
    """
    response = requests.get(endpoint)
    response.raise_for_status()
    data = response.json()
    
    # Check if the RPC has been called n_calls times
    if data[service_name][rpc_name] == n_calls:
        return True
    return False

def all_rpc_called(service_name: str, missings: List[str] = [], endpoint: str = calls_endpoint) -> bool:
    """
    Check if all remote procedure calls (RPCs) in a service have been made at least once.
    This function uses ArmoniK.Api.Mock. It just gets the '/calls.json' endpoint.

    Args:
        service_name (str): The name of the service containing the RPC information in the response.
        endpoint (str, optional): The URL of the remote service providing RPC information. Default is
            the value of calls_endpoint.
        missings (List[str], optional): A list of RPCs known to be not implemented. Default is an empty list.

    Returns:
        bool: True if all RPCs in the specified service have been called at least once, False otherwise.

    Raises:
        requests.exceptions.RequestException: If an error occurs when requesting ArmoniK.Api.Mock.

    Example:
    >>> all_rpc_called('http://localhost:5000/calls.json', 'Versions')
    False
    """
    response = requests.get(endpoint)
    response.raise_for_status()
    data = response.json()
    
    missing_rpcs = []

    # Check if all RPCs in the service have been called at least once
    for rpc_name, rpc_num_calls in data[service_name].items():
        if rpc_num_calls == 0:
            missing_rpcs.append(rpc_name)
    if missing_rpcs:
        if missings == missing_rpcs:
            return True
        print(f"RPCs not implemented in {service_name} service: {missing_rpcs}.")
        return False
    return True

def test_list_session_no_filter():
    sessions_client: ArmoniKSessions = get_client("Sessions")
    num, sessions = sessions_client.list_sessions()
    assert rpc_called("Sessions", "ListSessions")
    # TODO: Mock must be updated to return something and so that changes the following assertions
    assert num == 0
    assert sessions == []

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

def test_list_running_sessions():
    session_client: ArmoniKSessions = get_client("Sessions")
    sessions = admin.list_sessions(session_client, SessionFieldFilter.STATUS == SESSION_STATUS_RUNNING)
    assert sessions is not None
    assert len(sessions) > 0 

# endpoint = 'http://localhost:5000/calls.json'  
# reset_endpoint = "http://localhost:5000/reset"

# response = requests.get(endpoint)
# data = response.json()

# if response.status_code == 200:

#     print(data["Sessions"]["ListSessions"])
# else:

#     print(f"fail request with code {response.status_code}")

# try:
#     response = requests.post(reset_endpoint)
#     response.raise_for_status()
#     print("\nMock server resetted.")
# except requests.exceptions.HTTPError as e:
#     print("An error occurred when resetting the server: " + str(e))