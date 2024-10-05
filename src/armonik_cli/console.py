from functools import lru_cache

from rich import console


@lru_cache(maxsize=None)
def get_console() -> console.Console:
    """Create console instance to be imported and used across CLI."""
    return console.Console()
