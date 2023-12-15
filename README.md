# zappend

Tool to create and update a Zarr dataset from smaller slices

## Requirements

### Core

* Create a target Zarr dataset by appending Zarr dataset slice along a 
  given *append dimension*, usually `time`.
* The target and slice datasets may also be xcube multi-level datasets. 
* The tool takes care of modifying the target dataset using the slices,
  but doesn't care how the slice datasets are created.
* Target and slices are allowed live in different filesystems.
* The tool is configurable. The configuration defines 
  - the append dimension;
  - optional target encoding for all or individual target variables;
  - the target path into the target filesystem;
  - optional target filesystem options;
  - optional slice filesystem options.
* The target chunking of the append dimension equals the size of the append 
  dimension in each slice and vice versa. 
* The target encoding should allow specifying the target storage chunking, 
  data type, and compression. 
* The target encoding should also allow for packing floating point data into 
  integer data with fewer bits using scaling factor and offset.
* Detect coordinate variables and allow them to stay un-chunked.
  This is important for coordinate variables containing or corresponding 
  to the append-dimension.
* If the target does not exist, it will be created from a copy of the first 
  slice. This first slice will specify any not-yet-configured properties
  of the target dataset, e.g., the append dimension chunking.
* Slices are appended in the order they are provided.
* If a slice is not yet available, wait until it 
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

* Allow for **inserting and deleting** slices.
* Allow the slice to have an append dimension size > 1. 
* Allow specifying a constant delta between coordinates of the
  append dimension.
* Verify append dimension coordinates increase or decrease monotonically. 
* Verify coordinate deltas of append dimension to be constant. 
* Try getting along without using `xarray`, use `zarr` only,
  but honor the xarray `__ARRAY_DIMENSIONS__` attribute. 
  This avoids extra magic and complexity. 

## How it works

CLI: zappend_cli --config *config_path* *target_path* *slice_paths* ...

```
def zappend_cli(target_path, slice_paths, config_path):
  config = read_config(config_path)
  zappend_api(target_path, open_slice, slice_paths, config=config)
```

API: zappend_api(*target_path*, *slice_fn*, *slice_args*, config=*config*)

```
def zappend_api(target_path, slice_fn, slice_args, config=None):
  slice_iter = iter(slice_fn(*args, config=config) for args in slice_args)
  process(target_path, slice_iter, config)
```

with

```
def process(target_path, slice_iter, config):
  target_fs = get_target_fs(config)
  
  if not target_fs.exist(target_path, config):
    slice_ds = slice_iter.next()
    target_ds = create_target(target_fs, target_path, slice_ds, config)
  else:
    target_ds = open_target(target_fs, target_path)
     
  while (slice_ds = slice_iter.next()):
    target_ds.append(slice_ds)
```

