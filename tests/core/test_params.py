import click
import pytest

from datetime import timedelta

from armonik.common import Partition, Result, Session, Task
from armonik.common.filter.filter import StringFilter

from armonik_cli.core import KeyValuePairParam, TimeDeltaParam, FilterParam
from armonik_cli.core.params import FieldParam


@pytest.mark.parametrize(
    ("input", "output"),
    [
        ("key=value", ("key", "value")),
        ("ke_y=valu_e", ("ke_y", "valu_e")),
    ],
)
def test_key_value_pair_param(input, output):
    assert KeyValuePairParam().convert(input, None, None) == output


@pytest.mark.parametrize("input", ["key value", "ke?y=value"])
def test_key_value_pair_param_fail(input):
    with pytest.raises(click.BadParameter):
        KeyValuePairParam().convert(input, None, None)


@pytest.mark.parametrize(
    ("input", "output"),
    [
        ("12:11:10.987", timedelta(hours=12, minutes=11, seconds=10, milliseconds=987)),
        ("12:11:10", timedelta(hours=12, minutes=11, seconds=10)),
        ("0:10:0", timedelta(minutes=10)),
    ],
)
def test_timedelta_parm_success(input, output):
    assert TimeDeltaParam().convert(input, None, None) == output


@pytest.mark.parametrize("input", ["1.0", "10", "00:10"])
def test_timedelta_parm_fail(input):
    with pytest.raises(click.BadParameter):
        assert TimeDeltaParam().convert(input, None, None)


@pytest.mark.parametrize(
    ("filter_type", "input", "output"),
    [
        ("Task", "output.error contains 'an error'", Task.output.error.contains("an error")),
        ("Session", "options['key'] = value", Session.options["key"] == "value"),
        ("Result", "result_id = id", Result.result_id == "id"),
        ("Partition", "id = id", Partition.id == "id"),
    ],
)
def test_filter_parm_success(filter_type, input, output):
    assert FilterParam(filter_type).convert(input, None, None).to_dict() == output.to_dict()


@pytest.mark.parametrize(
    ("filter_type", "input"),
    [
        ("Task", "id = string with space"),
        ("Result", 'size = "1"'),
    ],
)
def test_filter_parm_fail(filter_type, input):
    with pytest.raises(click.BadParameter):
        assert FilterParam(filter_type).convert(input, None, None)


@pytest.mark.parametrize(
    "base_struct, field_name",
    [
        ("Task", "task_options"),
        ("Session", "options"),
    ],
)
def test_field_param_invalid(mocker, base_struct, field_name):
    with pytest.raises(click.BadParameter):
        FieldParam(base_struct).convert(field_name, None, None)


@pytest.mark.parametrize(
    "base_struct, field_name",
    [
        ("Task", "session_id"),
        ("Session", "session_id"),
    ],
)
def test_field_param_valid(mocker, base_struct, field_name):
    res = FieldParam(base_struct).convert(field_name, None, None)
    assert type(res) is StringFilter
