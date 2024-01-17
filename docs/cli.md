# Command Line Interface Reference



```
Usage: zappend [OPTIONS] [SLICES]...

  Create or update a Zarr dataset TARGET from slice datasets SLICES.

Options:
  -c, --config CONFIG    Configuration JSON or YAML file. If multiple are
                         passed, subsequent configurations are incremental to
                         the previous ones.
  -t, --target TARGET    Target Zarr dataset path or URI. Overrides the
                         'target_dir' configuration field.
  --dry-run              Run the tool without creating, changing, or deleting
                         any files.
  --help-config json|md  Show configuration help and exit.
  --help                 Show this message and exit.
```
