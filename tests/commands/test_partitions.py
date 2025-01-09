from copy import deepcopy
from datetime import datetime, timedelta
import pytest

from armonik.client import ArmoniKPartitions
from armonik.common import Partition

from conftest import run_cmd_and_assert_exit_code, reformat_cmd_output

ENDPOINT = "172.17.119.85:5001"

raw_partition = Partition(
    id="stream",
    parent_partition_ids=[],
    pod_reserved=1,
    pod_max=100,
    pod_configuration={},
    preemption_percentage=50,
    priority=1,
)

serialized_partition = {
    "Id": "stream",
    "ParentPartitionIds": [],
    "PodReserved": 1,
    "PodMax": 100,
    "PodConfiguration": {},
    "PreemptionPercentage": 50,
    "Priority": 1,
}


@pytest.mark.parametrize("cmd", [f"partition list -e {ENDPOINT}"])
def test_partition_list(mocker, cmd):
    mocker.patch.object(ArmoniKPartitions, "list_partitions", return_value=(1,deepcopy(raw_partition)))
    result = run_cmd_and_assert_exit_code(cmd)
    assert reformat_cmd_output(result.output, deserialize=True) == serialized_partition



@pytest.mark.parametrize(
    "cmd",
    [
        f"partition get --endpoint {ENDPOINT} {serialized_partition['Id']}",
    ],
)
def test_partition_get(mocker, cmd):
    mocker.patch.object(ArmoniKPartitions, "get_partition", return_value=raw_partition)
    result = run_cmd_and_assert_exit_code(cmd)
    assert reformat_cmd_output(result.output, deserialize=True) == serialized_partition