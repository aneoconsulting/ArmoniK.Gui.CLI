import rich_click as click

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
