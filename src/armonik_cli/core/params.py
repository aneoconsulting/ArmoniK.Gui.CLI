import re

import rich_click as click

from datetime import timedelta
from typing import cast, Tuple, Union

from armonik.common import Filter
from lark.exceptions import VisitError, UnexpectedInput

from armonik_cli.utils import parse_time_delta
from armonik_cli.core.filters import FilterParser


class KeyValuePairParam(click.ParamType):
    """
    A custom Click parameter type that parses a key-value pair in the format "key=value".

    Attributes:
        name: The name of the parameter type, used by Click.
    """

    name = "key_value_pair"

    def convert(
        self, value: str, param: Union[click.Parameter, None], ctx: Union[click.Context, None]
    ) -> Tuple[str, str]:
        """
        Converts the input value into a tuple of (key, value) if it matches the required format.

        Args:
            value: The input value to be converted.
            param: The parameter object passed by Click.
            ctx: The context in which the parameter is being used.

        Returns:
            A tuple (key, value) if the input matches the format "key=value".

        Raises:
            click.BadParameter: If the input does not match the expected format.
        """
        pattern = r"^([a-zA-Z0-9_-]+)=([a-zA-Z0-9_-]+)$"
        match_result = re.match(pattern, value)
        if match_result:
            return cast(Tuple[str, str], match_result.groups())
        self.fail(
            f"{value} is not a valid key value pair. Use key=value where both key and value contain only alphanumeric characters, dashes (-), and underscores (_).",
            param,
            ctx,
        )


class TimeDeltaParam(click.ParamType):
    """
    A custom Click parameter type that parses a time duration string in the format "HH:MM:SS.MS".

    Attributes:
        name: The name of the parameter type, used by Click.
    """

    name = "timedelta"

    def convert(
        self, value: str, param: Union[click.Parameter, None], ctx: Union[click.Context, None]
    ) -> timedelta:
        """
        Converts the input value into a timedelta object if it matches the required time format.

        Args:
            value: The input value to be converted.
            param: The parameter object passed by Click.
            ctx: The context in which the parameter is being used.

        Returns:
            A timedelta object representing the parsed time duration.

        Raises:
            click.BadParameter: If the input does not match the expected time format.
        """
        try:
            return parse_time_delta(value)
        except ValueError:
            self.fail(f"{value} is not a valid time delta. Use HH:MM:SS.MS.", param, ctx)


class FilterParam(click.ParamType):
    """
    A custom Click parameter type that parses a string expression into a valid ArmoniK API filter.

    Attributes:
        name: The name of the parameter type, used by Click.
    """

    name = "filter"

    def __init__(self, filter_type: str) -> None:
        super().__init__()
        try:
            filter_type = filter_type.capitalize()
            from armonik import common

            self.parser = FilterParser(
                obj=getattr(common, filter_type),
                filter=getattr(common.filter, f"{filter_type}Filter"),
                status_enum=getattr(common, f"{filter_type}Status")
                if filter_type != "Partition"
                else None,
                options_fields=(filter_type == "Task" or filter_type == "Session"),
                output_fields=filter_type == "Task",
            )
        except AttributeError:
            msg = f"'{filter_type}' is not a valid filter type."
            raise ValueError(msg)

    def convert(
        self, value: str, param: Union[click.Parameter, None], ctx: Union[click.Context, None]
    ) -> Filter:
        """
        Converts the input value into a valid ArmoniK API filter.

        Args:
            value: The input value to be converted.
            param: The parameter object passed by Click.
            ctx: The context in which the parameter is being used.

        Returns:
            A filter object.

        Raises:
            click.BadParameter: If the input contains a syntax error.
        """
        try:
            return self.parser.parse(value)
        except UnexpectedInput as error:
            self.fail(f"Filter syntax error: {error.get_context(value, span=40)}.", param, ctx)
        except VisitError as error:
            self.fail(str(error.orig_exc), param, ctx)
