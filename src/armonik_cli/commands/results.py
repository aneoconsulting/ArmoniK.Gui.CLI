from collections import defaultdict
import grpc
import rich_click as click

from typing import List, Union

from armonik.client.results import ArmoniKResults
from armonik.common import Result, ResultStatus, Direction
from armonik.common.filter import PartitionFilter, Filter

from armonik_cli.core import console, base_command
from armonik_cli.core.options import MutuallyExclusiveOption
from armonik_cli.core.params import FieldParam, FilterParam, ResultParam


RESULT_TABLE_COLS = [
    ("ID", "ResultId"),
    ("Status", "Status"),
    ("CreatedAt", "CreatedAt"),
]  # These should be configurable (through Config)


@click.group(name="result")
def results() -> None:
    """Manage results."""
    pass


@results.command(name="list")
@click.argument("session-id", required=True, type=str)
@click.option(
    "-f",
    "--filter",
    "filter_with",
    type=FilterParam("Partition"),
    required=False,
    help="An expression to filter the listed results with.",
    metavar="FILTER EXPR",
)
@click.option(
    "--sort-by",
    type=FieldParam("Partition"),
    required=False,
    help="Attribute of result to sort with.",
)
@click.option(
    "--sort-direction",
    type=click.Choice(["asc", "desc"], case_sensitive=False),
    default="asc",
    required=False,
    help="Whether to sort by ascending or by descending order.",
)
@click.option(
    "--page", default=-1, help="Get a specific page, it defaults to -1 which gets all pages."
)
@click.option("--page-size", default=100, help="Number of elements in each page")
@base_command
def result_list(
    endpoint: str,
    output: str,
    session_id: str,
    filter_with: Union[PartitionFilter, None],
    sort_by: Filter,
    sort_direction: str,
    page: int,
    page_size: int,
    debug: bool,
) -> None:
    """List the results of an ArmoniK cluster given <SESSION-ID>."""
    with grpc.insecure_channel(endpoint) as channel:
        results_client = ArmoniKResults(channel)
        curr_page = page if page > 0 else 0
        results_list = []
        while True:
            total, results = results_client.list_results(
                result_filter=(Result.session_id == session_id) & filter_with
                if filter_with is not None
                else (Result.session_id == session_id),
                sort_field=Result.name if sort_by is None else sort_by,
                sort_direction=Direction.ASC
                if sort_direction.capitalize() == "ASC"
                else Direction.DESC,
                page=curr_page,
                page_size=page_size,
            )

            results_list += results
            if page > 0 or len(results_list) >= total:
                break
            curr_page += 1

    if total > 0:
        results = [_clean_up_status(r) for r in results]
        console.formatted_print(results, format=output, table_cols=RESULT_TABLE_COLS)


@results.command(name="get")
@click.argument("result-ids", type=str, nargs=-1, required=True)
@base_command
def result_get(endpoint: str, output: str, result_ids: List[str], debug: bool) -> None:
    """Get details about multiple results given their RESULT_IDs."""
    with grpc.insecure_channel(endpoint) as channel:
        results_client = ArmoniKResults(channel)
        results = []
        for result_id in result_ids:
            result = results_client.get_result(result_id)
            result = _clean_up_status(result)
            results.append(result)
        console.formatted_print(results, format=output, table_cols=RESULT_TABLE_COLS)


@results.command(name="create")
@click.argument("session-id", type=str, required=True)
@click.option(
    "-r",
    "--result",
    "results",
    type=ResultParam(),
    required=True,
    multiple=True,
    help=(
        "Results to create. You can pass:\n"
        "1. --result <result_name> (only metadata is created).\n"
        "2. --result '<result_name> bytes <bytes>' (data is provided in bytes).\n"
        "3. --result '<result_name> file <filepath>' (data is provided from a file)."
    ),
)
@base_command
def result_create(
    endpoint: str, output: str, results: List[ResultParam.ParamType], session_id: str, debug: bool
) -> None:
    """Create result objects in a session with id SESSION_ID."""
    results_with_data = {res[0]: res[2] for res in results if res[1] == "bytes"}  # type: ignore
    metadata_only = [res[0] for res in results if res[1] == "nodata"]
    # Read in remaining data from files
    for res in results:
        if res[1] == "file":
            result_data_filepath = res[2]  # type: ignore
            try:
                with open(result_data_filepath, "rb") as file:
                    results_with_data[res[0]] = file.read()
            except FileNotFoundError:
                raise click.FileError(
                    str(result_data_filepath),
                    f"File {str(result_data_filepath)} not found for result {res[0]}",
                )
    channel = grpc.insecure_channel(endpoint)
    results_client = ArmoniKResults(channel)
    # Create metadata-only results
    if len(metadata_only) > 0:
        _ = results_client.create_results_metadata(
            result_names=metadata_only, session_id=session_id
        )
        console.print(f"Created metadata for results: {metadata_only}")
        # TODO: Should probably print a table mapping result_name to result_id
    # Create results with data
    if len(results_with_data.keys()) > 0:
        _ = results_client.create_results(results_data=results_with_data, session_id=session_id)
        result_names = ", ".join(list(results_with_data.keys()))
        console.print(f"Created the following results along with their data: {result_names}")


@results.command(name="upload_data")
@click.argument("session-id", type=str, required=True)
@click.argument("result-id", type=str, required=True)
@click.option(
    "--from-bytes", type=str, cls=MutuallyExclusiveOption, mutual=["from_file"], require_one=True
)
@click.option(
    "--from-file", type=str, cls=MutuallyExclusiveOption, mutual=["from_bytes"], require_one=True
)
@base_command
def result_upload_data(
    endpoint: str,
    output: str,
    session_id: str,
    result_id: Union[str, None],
    from_bytes: Union[str, None],
    from_file: str,
    debug: bool,
) -> None:
    """Upload data for a result separately"""
    with grpc.insecure_channel(endpoint) as channel:
        results_client = ArmoniKResults(channel)
        if from_bytes is not None:
            result_data = bytes(from_bytes, encoding="utf-8")
        if from_file is not None:
            try:
                with open(from_file, "rb") as file:
                    result_data = file.read()
            except FileNotFoundError:
                raise click.FileError(
                    from_file, f"File {from_file} not found for result with id {result_id}"
                )

        results_client.upload_result_data(result_id, session_id, result_data)


@results.command(name="delete_data")
@click.argument("result-ids", type=str, nargs=-1, required=True)
@click.option("--yes", is_flag=True)
@base_command
def result_delete_data(
    endpoint: str, output: str, result_ids: List[str], yes: bool, debug: bool
) -> None:
    """Delete the data of multiple results given their RESULT_IDs."""
    with grpc.insecure_channel(endpoint) as channel:
        results_client = ArmoniKResults(channel)
        session_result_mapping = defaultdict(list)
        for result_id in result_ids:
            result = results_client.get_result(result_id)
            session_result_mapping[result.session_id].append(result_id)
        if yes or click.confirm(
            f"Are you sure you want to delete the result data of task [{result.owner_task_id}] in session [{result.session_id}]",
            abort=False,
        ):
            for session_id, result_ids_for_session in session_result_mapping.items():
                results_client.delete_result_data(result_ids_for_session, session_id)


def _clean_up_status(result: Result) -> Result:
    result.status = ResultStatus.name_from_value(result.status).split("_")[-1].capitalize()
    return result
