# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
import sys

import click


@click.command()
@click.argument("slices", nargs=-1)
@click.option(
    "--config",
    "-c",
    metavar="CONFIG",
    multiple=True,
    help=(
        "Configuration JSON or YAML file."
        " If multiple are passed, subsequent configurations"
        " are incremental to the previous ones."
    ),
)
@click.option(
    "--target",
    "-t",
    metavar="TARGET",
    help=(
        "Target Zarr dataset path or URI."
        " Overrides the 'target_dir' configuration field."
    ),
)
@click.option(
    "--force-new",
    is_flag=True,
    help=(
        "Force creation of a new target dataset."
        " An existing target dataset (and its lock) will be"
        " permanently deleted before appending of slice datasets"
        " begins. WARNING: the deletion cannot be rolled back."
    ),
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Run the tool without creating, changing, or deleting any files.",
)
@click.option(
    "--traceback",
    is_flag=True,
    help="Show Python traceback on error.",
)
@click.option(
    "--version",
    is_flag=True,
    help="Show version and exit.",
)
@click.option(
    "--help-config",
    metavar="json|md",
    type=click.Choice(["json", "md"]),
    help="Show configuration help and exit.",
)
def zappend(
    slices: tuple[str, ...],
    config: tuple[str, ...],
    target: str | None,
    force_new: bool,
    dry_run: bool,
    traceback: bool,
    version: bool,
    help_config: str | None,
):
    """Create or update a Zarr datacube TARGET from slice datasets SLICES.

    The zappend command concatenates the dataset SLICES along a given append dimension,
    e.g., "time" (the default) for geospatial satellite observations.
    Each append step is atomic, that is, the append operation is a transaction that can
    be rolled back, in case the append operation fails. This ensures integrity of the
    target data cube given by TARGET or in CONFIG.
    """
    if version:
        from zappend import __version__

        return click.echo(f"{__version__}")

    if help_config:
        return _show_config_help(help_config)

    if not slices:
        click.echo("No slice datasets given.")
        return

    from zappend.api import zappend

    # noinspection PyBroadException
    try:
        zappend(
            slices,
            config=config,
            target_dir=target,
            force_new=force_new,
            dry_run=dry_run,
        )
    except BaseException as e:
        if traceback:
            import traceback as tb

            tb.print_exc(file=sys.stderr)
        raise click.ClickException(f"{e}") from e


def _show_config_help(config_help_format):
    from zappend.config import get_config_schema

    text = get_config_schema(format=config_help_format)
    click.echo(text + "\n")


if __name__ == "__main__":
    zappend()
