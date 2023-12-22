# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import click


@click.command()
@click.argument("slices", nargs=-1)
@click.option("--config", "-c",
              metavar="CONFIG",
              multiple=True,
              help="Configuration JSON or YAML file."
                   " If multiple are passed,"
                   " they will be deeply merged into one.")
@click.option("--target", "-t",
              metavar="TARGET",
              help="Target Zarr dataset path or URI."
                   " Overrides the 'target_uri' configuration field.")
@click.option("--dry-run", is_flag=True,
              help="Run the tool without creating, changing,"
                   " or deleting any files.")
@click.option("--help-config", is_flag=True,
              help="Show configuration help and exit.")
def zappend(slices: tuple[str, ...],
            config: tuple[str, ...],
            target: str | None,
            dry_run: bool,
            help_config: bool):
    """Create or update a Zarr dataset TARGET from slice datasets SLICES.
    """

    if help_config:
        return _show_config_help()

    if not slices:
        click.echo("No slice datasets given.")
        return

    from zappend.api import zappend
    zappend(slices, config=config, target_uri=target, dry_run=dry_run)


def _show_config_help():
    import json
    from zappend.config import CONFIG_V1_SCHEMA
    config_schema_json = json.dumps(CONFIG_V1_SCHEMA, indent=2)
    print(f"Configuration JSON schema:\n")
    print(config_schema_json)


if __name__ == '__main__':
    zappend()
