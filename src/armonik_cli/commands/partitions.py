import grpc
import rich_click as click

from datetime import timedelta
from typing import List, Tuple, Union

from armonik.client.partitions import ArmoniKPartitions
from armonik.common.filter import PartitionFilter

from armonik_cli.core import console, base_command
from armonik_cli.core.params import FilterParam

PARTITIONS_TABLE_COLS = [("ID", "Id"), ("PodReserved", "PodReserved"), ("PodMax", "PodMax")]


@click.group(name="partition")
def partitions() -> None:
    """Manage cluster partitions."""
    pass


@partitions.command()
@click.option(
    "-f",
    "--filter",
    "filter_with",
    type=FilterParam("Partition"),
    required=False,
    help="An expression to filter partitions with",
    metavar="<filter expr>",
)
# @click.option(
#     "--sort_by", type=FieldParam("Task"), required=False, help="Attribute of partition to sort with."
# )
# @click.option(
#     "--sort_order",
#     type=click.Choice(["asc", "desc"], case_sensitive=False),
#     default="asc",
#     required=False,
#     help="Whether to sort by ascending or by descending order.",
# )
@click.option("--page", default=-1)
@base_command
def list(
    endpoint: str, output: str, filter_with: Union[PartitionFilter, None], page: int, debug: bool
) -> None:
    """List the partitions in an ArmoniK cluster."""
    with grpc.insecure_channel(endpoint) as channel:
        partitions_client = ArmoniKPartitions(channel)
        total, partitions = partitions_client.list_partitions(
            partition_filter=filter_with,
            page=page if page >= 0 else 0,
            page_size=100 if page >= 0 else 10000,
        )

        if total > 0:
            console.formatted_print(partitions, format=output, table_cols=PARTITIONS_TABLE_COLS)
        else:
            pass


@partitions.command()
@click.argument("partition-id", type=str, required=True)
@base_command
def get(endpoint: str, output: str, partition_id: str, debug: bool) -> None:
    """Get a specific partition from an ArmoniK cluster given its id."""
    with grpc.insecure_channel(endpoint) as channel:
        partitions_client = ArmoniKPartitions(channel)
        partition = partitions_client.get_partition(partition_id)
        console.formatted_print(partition, format=output, table_cols=PARTITIONS_TABLE_COLS)
