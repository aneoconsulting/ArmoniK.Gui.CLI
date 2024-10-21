from datetime import timedelta


def parse_time_delta(time_str: str) -> timedelta:
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
