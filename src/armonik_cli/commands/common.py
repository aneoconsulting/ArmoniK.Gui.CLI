import re

import rich_click as click

from datetime import timedelta
from typing import cast, Tuple, Union


endpoint_option = click.option(
    "-e",
    "--endpoint",
    type=str,
    required=True,
    help="Endpoint of the cluster to connect to.",
    metavar="ENDPOINT",
)
output_option = click.option(
    "-o",
    "--output",
    type=click.Choice(["yaml", "json", "table"], case_sensitive=False),
    default="json",
    show_default=True,
    help="Commands output format.",
    metavar="FORMAT",
)
debug_option = click.option(
    "--debug", is_flag=True, default=False, help="Print debug logs and internal errors."
)


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
            return self._parse_time_delta(value)
        except ValueError:
            self.fail(f"{value} is not a valid time delta. Use HH:MM:SS.MS.", param, ctx)

    @staticmethod
    def _parse_time_delta(time_str: str) -> timedelta:
        """
        Parses a time string in the format "HH:MM:SS.MS" into a datetime.timedelta object.

        Args:
            time_str (str): A string representing a time duration in hours, minutes,
                            seconds, and milliseconds (e.g., "12:34:56.789").

        Returns:
            timedelta: A datetime.timedelta object representing the parsed time duration.

        Raises:
            ValueError: If the input string is not in the correct format.
        """
        hours, minutes, seconds = time_str.split(":")
        sec, microseconds = (seconds.split(".") + ["0"])[:2]  # Handle missing milliseconds
        return timedelta(
            hours=int(hours),
            minutes=int(minutes),
            seconds=int(sec),
            milliseconds=int(microseconds.ljust(3, "0")),  # Ensure 3 digits for milliseconds
        )
