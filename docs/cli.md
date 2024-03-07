# Command Line Interface Reference

After installation, the `zappend` command can be used from the terminal. 
The following are the command's options and arguments:

```
Usage: zappend [OPTIONS] [SLICES]...

  Create or update a Zarr datacube TARGET from slice datasets SLICES.

  The zappend command concatenates the dataset SLICES along a given append
  dimension, e.g., "time" (the default) for geospatial satellite observations.
  Each append step is atomic, that is, the append operation is a transaction
  that can be rolled back, in case the append operation fails. This ensures
  integrity of the target data cube given by TARGET or in CONFIG.

Options:
  -c, --config CONFIG    Configuration JSON or YAML file. If multiple are
                         passed, subsequent configurations are incremental to
                         the previous ones.
  -t, --target TARGET    Target Zarr dataset path or URI. Overrides the
                         'target_dir' configuration field.
  --force-new            Force creation of a new target dataset. An existing
                         target dataset (and its lock) will be permanently
                         deleted before appending of slice datasets begins.
                         WARNING: the deletion cannot be rolled back.
  --dry-run              Run the tool without creating, changing, or deleting
                         any files.
  --traceback            Show Python traceback on error.
  --version              Show version and exit.
  --help-config json|md  Show configuration help and exit.
  --help                 Show this message and exit.
```
