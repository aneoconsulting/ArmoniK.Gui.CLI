import click
import pytest

from datetime import timedelta

from armonik_cli.core import KeyValuePairParam, TimeDeltaParam


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
