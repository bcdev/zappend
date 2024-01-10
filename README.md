[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# zappend

Tool for creating and updating a Zarr dataset from smaller slices

The objective of **zappend** is to address recurring memory issues when 
generating large geospatial data cubes using the 
[Zarr format](https://zarr.readthedocs.io/) by subsequently concatenating data
slices along an append dimension, usually `time`. Each append step is atomic, 
that is, the append operation is a transaction that can be rolled back, 
in case the append operation fails. This ensures integrity of the target 
data cube. 

## How it works

### CLI

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

### API

The API is defined in module `zappend.api`:

```python
def zappend(slices: Iterable[str | xr.Dataset], config: ConfigLike = None, **kwargs):
    """
    Create or update a Zarr dataset from dataset slices.

    :param slices: The slice datasets. An iterable that yields either
        ``str`` or ``xarray.Dataset`` objects. If ``str`` is used,
        it is interpreted as local dataset path or dataset URI.
        If a URI is used, protocol-specific parameters apply, given by
        configuration parameter ``slice_storage_options``.
    :param config: Processor configuration.
        May be a file path or URI, a ``dict``, ``None``, or a sequence of
        the aforementioned. If a sequence is used, subsequent configurations
        are incremental to the previous ones.
    :param kwargs: Additional configuration parameters.
        Can be used to pass or override configuration values in *config*.
    """
    processor = Processor(config, **kwargs)
    processor.process_slices(slices)
```

### Configuration

The configuration is described in a
[dedicated document](https://github.com/bcdev/zappend/blob/main/CONFIG.md).


## Tool Requirements

### Core Requirements

* Create a target Zarr dataset by appending Zarr dataset slices along a 
  given *append dimension*, usually `time`.   
* The target and slice datasets may also be xcube multi-level datasets. 
* The tool takes care of modifying the target dataset using the slices,
  but doesn't care how the slice datasets are created.
* Slice datasets may be given as URIs with storage options or as 
  in-memory datasets of type 
  [xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html)
  or 
  [xcube.core.mldataset.MultiLevelDataset](https://xcube.readthedocs.io/en/latest/mldatasets.html).
* Target and slices are allowed to live in different filesystems.
* The tool is configurable. The configuration defines 
  - the append dimension;
  - optional target encoding for all or individual target variables;
  - the target path into the target filesystem;
  - optional target storage options;
  - optional slice storage options.
* The target chunking of the append dimension equals the size of the append 
  dimension in each slice and vice versa. 
* The target encoding should allow for specifying the target storage chunking, 
  data type, and compression. 
* The target encoding should also allow for packing floating point data into 
  integer data with fewer bits using scaling factor and offset.
* Detect coordinate variables and allow them to stay un-chunked.
  This is important for coordinate variables containing or corresponding 
  to the append-dimension.
* If the target does not exist, it will be created from a copy of the first 
  slice. This first slice will specify any not-yet-configured properties
  of the target dataset, e.g., the append dimension chunking.
* If the target exists, the slice will be appended. Check if the slice to be 
  appended is last. If not, refuse to append (alternative: insert but this is 
  probably difficult or error prone).
* Slices are appended in the order they are provided.
* If a slice is not yet available, wait and retry until it 
  - exists, and
  - is complete.
* Check for each slice that it is valid. A valid slice
  - is self-consistent, 
  - has the same structure as target, and
  - has an append dimension whose size is equal to the target chunking of
    this dimension.
* Before appending a slice, lock the target so that another tool invocation 
  can recognize it, e.g., write a lock file.
* If the target is locked, either wait until it becomes available or exit 
  with an error. The behaviour is controlled by a tool option.
* After successfully appending a slice, remove the lock from the target.
* Appending a slice shall be an atomic operation to ensure target dataset 
  integrity. That is, in case a former append step failed, a rollback must
  be performed to restore the last valid state of the target. Rolling back  
  shall take place after an append failed, or before a new slice is appended,
  or to sanitize a target to make it usable again. Rolling back shall 
  include restoring all changed files, removing all added files, 
  and removing any locks. 
* The tool shall allow for continuing appending slices at the point
  it failed.
* The tool shall offer a CLI and a Python API.
  - Using the CLI, slices are given as a variadic argument that provides the 
    file paths into the slice filesystem.
  - Using the Python API, it shall be possible to provide the slices by 
    specifying a function that generates the slice datasets and an
    iterable providing the arguments for the function.
    This is similar how the Python `map()` built-in works.

### To be considered

* Add xcube server API: Add endpoint to xcube server that works similar 
  to the CLI and also uses a similar request parameters.
* Allow for **inserting and deleting** slices.
* Allow the slice to have an append dimension size > 1. 
* Allow specifying a constant delta between coordinates of the
  append dimension.
* Verify append dimension coordinates increase or decrease monotonically. 
* Verify coordinate deltas of append dimension to be constant. 
* Try getting along without using `xarray`, use `zarr` only,
  but honor the xarray `_ARRAY_DIMENSIONS` attribute. 
  This avoids extra magic and complexity.
* Use it in xcube data stores for the `write_data()` method, as a parameter 
  to enforce sequential writing of Zarr datasets as a robust option when a 
  plain write fails.

