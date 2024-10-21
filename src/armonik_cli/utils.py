from datetime import timedelta


def parse_time_delta(time_str: str) -> timedelta:
    """
    Parses a time string in the format "D.HH:MM:SS.FRAC" into a datetime.timedelta object.

    Args:
        time_str: A string representing a time duration in days, hours, minutes,
                        seconds, and fractional seconds (e.g., "15.12:34:56.789183").

    Returns:
        A datetime.timedelta object representing the parsed time duration.

    Raises:
        ValueError: If the input string is not in the correct format.
    """
    negative = False
    if time_str[0] == "-":
        negative = True
        time_str = time_str[0:]
    hours, minutes, seconds = time_str.split(":")
    days, hours = (["0"] + hours.split("."))[-2:]
    sec, fractional_sec = (seconds.split(".") + ["0"])[:2]
    return (-1 if negative else 1) * timedelta(
        days=int(days),
        hours=int(hours),
        minutes=int(minutes),
        seconds=int(sec),
        microseconds=int(float(f"0.{fractional_sec}") * 10**6),
    )


def remove_string_delimiters(s: str) -> str:
    """Remove delimiters ' or " from a string.

    Args:
        s: A string.

    Returns:
        The same string without delimiters if it has some.
    """
    if s[0] == s[-1] == '"' or s[0] == s[-1] == "'":
        return s[1:-1]
    return s
