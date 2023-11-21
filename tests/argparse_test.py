import pytest

import armonik_cli.admin as admin
from armonik.common.filter import Filter


def test_sum():
    assert admin.sum(3,55) == 58

