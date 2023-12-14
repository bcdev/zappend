# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import click


@click.command()
@click.argument('target', nargs=1)
@click.argument('subsets', nargs=-1)
@click.option('--config', '-c', help='Configuration file.')
def zappend(target: str, subsets: tuple[str, ...], config: str | None):
    """Tool to create or update a Zarr dataset from subsets."""
    if config:
        click.echo(f"Reading {config}")
    click.echo(f"Creating {target}")
    for subset in subsets:
        click.echo(f"Appending {subset} to {target}")
    click.echo(f"Done.")


if __name__ == '__main__':
    zappend()
