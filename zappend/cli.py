# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import click


@click.command(options_metavar='<options>')
@click.argument('target',
                metavar='<target-path>', nargs=1)
@click.argument('slices',
                metavar='<slice-paths>',
                nargs=-1)
@click.option('--config', '-c',
              metavar='<config-path>',
              multiple=True, help='Configuration file.')
def zappend(target: str,
            slices: tuple[str, ...],
            config: tuple[str, ...]):
    """Tool to create or update a Zarr dataset from slices."""
    from zappend.api import zappend as zappend_api

    if not slices:
        raise click.ClickException("No slice paths given.")

    zappend_api(target, slices, config=config)


if __name__ == '__main__':
    zappend()
