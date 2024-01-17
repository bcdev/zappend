# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import click


@click.command()
@click.argument("slices", nargs=-1)
@click.option(
    "--config",
    "-c",
    metavar="CONFIG",
    multiple=True,
    help="Configuration JSON or YAML file."
    " If multiple are passed, subsequent configurations"
    " are incremental to the previous ones.",
)
@click.option(
    "--target",
    "-t",
    metavar="TARGET",
    help="Target Zarr dataset path or URI."
    " Overrides the 'target_dir' configuration field.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Run the tool without creating, changing," " or deleting any files.",
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
    dry_run: bool,
    help_config: str | None,
):
    """Create or update a Zarr datacube TARGET from slice datasets SLICES."""

    if help_config:
        return _show_config_help(help_config)

    if not slices:
        click.echo("No slice datasets given.")
        return

    from zappend.api import zappend

    # noinspection PyBroadException
    try:
        zappend(slices, config=config, target_dir=target, dry_run=dry_run)
    except BaseException as e:
        raise click.ClickException(f"{e}") from e


def _show_config_help(config_help_format):
    from zappend.config import get_config_schema

    text = get_config_schema(format=config_help_format)
    print(text + "\n")


if __name__ == "__main__":
    zappend()
