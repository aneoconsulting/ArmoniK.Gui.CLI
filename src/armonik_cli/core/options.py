from typing import Any, List, Tuple, Mapping

import rich_click as click


class MutuallyExclusiveOption(click.Option):
    """
    A custom Click option class that enforces mutual exclusivity between specified options.

    This class allows defining options that cannot be used together with other specific options.
    If any of the mutually exclusive options are used simultaneously, a usage error is raised.

    Attributes:
        mutual: A list of option names that cannot be used together with this option.
    """

    def __init__(self, *args, **kwargs):
        self.mutual = set(kwargs.pop("mutual", []))
        if self.mutual:
            kwargs["help"] = (
                f"{kwargs.get('help', '')} This option cannot be used together with {' or '.join(self.mutual)}."
            )
        super().__init__(*args, **kwargs)

    def handle_parse_result(
        self, ctx: click.Context, opts: Mapping[str, Any], args: List[str]
    ) -> Tuple[Any, List[str]]:
        """
        Handle the parsing of command-line options, enforcing mutual exclusivity.

        Args:
            ctx: The Click context, which provides information about the command being executed.
            opts: A dictionary of the parsed command-line options.
            args: The remaining command-line arguments.

        Returns:
            The result of the superclass's `handle_parse_result` method, if no mutual exclusivity violation occurs.

        Raises:
            click.UsageError: If this option and any of the mutually exclusive options are used together.
        """
        mutex = self.mutual.intersection(opts)
        if mutex and self.name in opts:
            raise click.UsageError(
                f"Illegal usage: `{self.name}` cannot be used together with '{''.join(mutex)}'."
            )

        return super().handle_parse_result(ctx, opts, args)
