import pytest

from datetime import timedelta

from armonik_cli.utils import parse_time_delta, remove_string_delimiters


@pytest.mark.parametrize(
    ("input", "output"),
    [
        ("15.12:11:10", timedelta(days=15, hours=12, minutes=11, seconds=10)),
        ("15.12:11:10.987", timedelta(days=15, hours=12, minutes=11, seconds=10, milliseconds=987)),
        ("12:11:10.987", timedelta(hours=12, minutes=11, seconds=10, milliseconds=987)),
        ("12:11:10", timedelta(hours=12, minutes=11, seconds=10)),
        ("0:10:0", timedelta(minutes=10)),
        ("-0:10:0", -timedelta(minutes=10)),
    ],
)
def test_parse_time_delta(input, output):
    assert parse_time_delta(input) == output


@pytest.mark.parametrize(
    ("input", "output"),
    [
        ("'test string'", "test string"),
        ('"test string"', "test string"),
        ("test string", "test string"),
    ],
)
def test_remove_string_delimiters(input, output):
    remove_string_delimiters(input) == output
