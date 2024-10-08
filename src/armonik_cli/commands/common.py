import re

import rich_click as click

from datetime import timedelta

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
    help="Endpoint of the cluster to connect to.",
    metavar="FORMAT",
)
debug_option = click.option(
    "--debug", is_flag=True, default=False, help="Print debug logs and internal errors."
)


class KeyValuePairParamType(click.ParamType):
    name = "key_value_pair"

    def convert(self, value, param, ctx):
        pattern = r"^([a-zA-Z0-9_-]+)=([a-zA-Z0-9_-]+)$"
        match_result = re.match(pattern, value)
        if match_result:
            return match_result.groups()
        self.fail(
            f"{value} is not a valid key value pair. Use key=value where both key and value contain only alphanumeric characters, dashes (-), and underscores (_).",
            param,
            ctx,
        )


class TimeDeltaParamType(click.ParamType):
    name = "timedelta"

    def convert(self, value, param, ctx):
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
