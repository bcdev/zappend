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
@click.option('--config', '-c', 'config_paths',
              metavar='<config-path>',
              multiple=True, help='Configuration file.')
def zappend(target_path: str,
            slice_paths: tuple[str, ...],
            config_paths: tuple[str, ...]):
    """Tool to create or update a Zarr dataset from slices."""
    from zappend.config import load_configs
    from zappend.context import Context
    from zappend.processor import Processor

    if not slice_paths:
        raise click.ClickException("No slice paths given.")

    config = load_configs(config_paths)
    ctx = Context(target_path, config)
    processor = Processor(ctx)
    processor.process_slices(iter(slice_paths))


if __name__ == '__main__':
    zappend()
