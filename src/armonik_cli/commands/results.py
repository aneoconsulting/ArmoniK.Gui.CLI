import grpc
import rich_click as click

from datetime import timedelta
from typing import List, Tuple, Union

from armonik.client.results import ArmoniKResults
from armonik.common import Result, ResultStatus

from armonik_cli.core import console, base_command, KeyValuePairParam, TimeDeltaParam


RESULT_TABLE_COLS = [("ID", "ResultId"), ("Status", "Status"), ("CreatedAt", "CreatedAt")] # These should be configurable (through Config)


@click.group(name="result")
def results() -> None:
    """Manage results."""
    pass


@results.command()
@click.argument("session-id", required=True, type=str) 
@base_command
def list(endpoint: str, output: str, session_id: str, debug: bool) -> None:
    """List the results of an ArmoniK cluster."""
    with grpc.insecure_channel(endpoint) as channel:
        results_client = ArmoniKResults(channel)
        total, results = results_client.list_results(result_filter=Result.session_id==session_id)

    if total > 0:
        results = [_clean_up_status(r) for r in results]
        console.formatted_print(results, format=output, table_cols=RESULT_TABLE_COLS)

    # TODO: Use logger to display this information
    # console.print(f"\n{total} sessions found.")

@results.command()
@click.argument("result-id", type=str)
# TODO: @click.option("--download", is_flag=True)
@base_command
def get(endpoint: str, output: str, result_id: str, debug: bool) -> None:
    with grpc.insecure_channel(endpoint) as channel:
        results_client = ArmoniKResults(channel) 
        result = results_client.get_result(result_id)
        result = _clean_up_status(result)
        console.formatted_print(results, format=output, table_cols=RESULT_TABLE_COLS)

@results.command()
@click.argument("session-id", type=str)
@click.option("--result_data_file", type=str, default=None)
@base_command
def create(endpoint: str, output: str, result_data_file :str, session_id: str, debug: bool) -> None:
    with grpc.insecure_channel(endpoint) as channel:
        results_client = ArmoniKResults(channel)
        if result_data_file: 
            result_data = "" # read from file
        else:
            result_data = "" # multiple prompts ? 
        results_client.create_results(results_data=result_data, session_id=session_id)

@results.command()
@click.argument("result-id", type=str)
@click.option("--yes", is_flag=True)
@base_command
def delete(endpoint: str, output: str, result_id: str, yes: bool, debug: bool) -> None:
    with grpc.insecure_channel(endpoint) as channel:
        results_client = ArmoniKResults(channel)
        result = results_client.get_result(result_id)
        if yes or click.confirm(f"Are you sure you want to delete the result data of task [{result.owner_task_id}] in session [{result.session_id}]", abort=False):
            results_client.delete_result_data(session_id=result.session_id) # Are results the same as result data


def _clean_up_status(result: Result) -> Result:
    #result.status = ResultStatus(result.status).name.split("_")[-1].capitalize()
    result.status = ResultStatus.name_from_value(result.status).split("_")[-1].capitalize()
    return result
