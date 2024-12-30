from typing import Any, List, Tuple, Mapping

import rich_click as click


class MutuallyExclusiveOption(click.Option):
    """
    A custom Click option class that enforces mutual exclusivity between specified options
    and optionally requires at least one of the mutual options to be passed.

    Attributes:
        mutual: A list of option names that cannot be used together with this option.
        require_one: Whether at least one of the mutually exclusive options must be provided.
    """

    def __init__(self, *args, **kwargs):
        self.mutual = set(kwargs.pop("mutual", []))
        self.require_one = kwargs.pop("require_one", False)

        if self.mutual:
            mutual_text = f" This option cannot be used together with {' or '.join(self.mutual)}."
            kwargs["help"] = f"{kwargs.get('help', '')}{mutual_text}"

        if self.require_one:
            kwargs["help"] = (
                f"{kwargs.get('help', '')} At least one of these options must be provided."
            )

        super().__init__(*args, **kwargs)

    def handle_parse_result(
        self, ctx: click.Context, opts: Mapping[str, Any], args: List[str]
    ) -> Tuple[Any, List[str]]:
        """
        Handle the parsing of command-line options, enforcing mutual exclusivity
        and the requirement of at least one mutual option if specified.

        Args:
            ctx: The Click context.
            opts: A dictionary of the parsed command-line options.
            args: The remaining command-line arguments.

        Returns:
            The result of the superclass's `handle_parse_result` method.

        Raises:
            click.UsageError: If mutual exclusivity is violated or if none of the required options are provided.
        """
        mutex = self.mutual.intersection(opts)

        # Enforce mutual exclusivity
        if mutex and self.name in opts:
            raise click.UsageError(
                f"Illegal usage: `{self.name}` cannot be used together with '{', '.join(mutex)}'."
            )

        # Enforce that at least one mutual option is provided
        if self.require_one and not (mutex or self.name in opts):
            raise click.UsageError(
                f"At least one of the following options must be provided: {', '.join(self.mutual | {self.name})}."
            )

        return super().handle_parse_result(ctx, opts, args)
