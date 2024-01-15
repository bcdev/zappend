# User Guide

Both the `zappend` [CLI command](cli.md) and the [Python function](api.md) can be run 
without any further [configuration](config.md) except the target dataset path and the 
slice dataset paths that contribute to the datacube to be generated. The target dataset 
path must point to a directory that will contain a Zarr group to be created and 
updated. The slice dataset paths may be provided as Zarr as well or in other data 
formats supported by the  
[xarray.open_dataset()](https://docs.xarray.dev/en/stable/generated/xarray.open_dataset.html)
function. The target and slice dataset are allowed to live in different filesystems. 
Additional filesystem storage options may be specified via the tool's configuration.

The tool takes care of generating the target dataset from slice datasets, but doesn't 
care how the slice datasets are created. Hence, when using the Python `zappend()` 
function, the slice datasets can be provided in various forms. More on this below.

> [!NOTE]
> We use the term _Dataset_ in the same way `xarray` does: A dataset
> comprises any number of multidimensional _Data Variables_, and 
> usually 1-dimensional _Coordinate Variables_ that provide the labels for 
> the dimensions used by the data variables. A variable comprises the actual 
> data array as well as metadata describing the data dimensions, 
> units, and encoding, such as chunking and compression.

## Dataset Outline

If no further configuration is supplied, then the target dataset's outline
and data encoding is fully prescribed by the first slice dataset provided.
By default, the dimension along subsequent slice datasets are concatenated
is `time`. If you use a different append dimension, the `append_dim` 
setting can be used to specify its name:

```json
{
    "append_dim": "depth"
}
```

All other non-variadic dimensions can and should be specified using the
`fixed_dims` setting which is a mapping from dimension name to the 
fixed dimension sizes, e.g.:

```json
{
    "fixed_dims": {
        "x": 16384,
        "y": 8192
    }
}
```

By default, without further configuration, all data variables seen in the first
dataset slice will be included in the target dataset. If only a subset of 
variables shall be used from the slice dataset, they can be specified using the
`included_variables` setting, which is a list of names of variables that will 
be included:

```json
{
    "included_variables": ["chl", "tsm"]
}
```

Often, it is easier to tell which variables should be excluded:

```json
{
    "excluded_variables": ["GridCellId"]
}
```

## Variable Metadata 

Without any additional configuration, `zappend` uses the outline, attributes, 
and encoding information of data variables for the target dataset from the 
data variables of the first slice dataset. 
Encoding information is used only to the extent applicable to the Zarr format.
Non-applicable encoding information will be reported by a warning log record 
but is otherwise ignored. 

Variable metadata can be specified by the `variables` setting, which is a 
mapping from variable name to a mapping that provides the dimensions, attributes, 
and encoding information of data variables for the target dataset. All such 
information is optional. The provided settings will be merged with the
information retrieved from the data variables with same name included in the
first dataset slice.

A special "variable name" is the wildcard `*` that can be used to define default
values for all variables:

```json
{
    "variables": {
        "*": { }
    }
}
```

If `*` is specified, the effective variable metadata applied is gained by merging a 
given specific metadata, into the common metadata given by `*`, which is eventually 
merged into metadata of the variable in the first dataset slice.

> [!NOTE]
> The metadata of variables from subsequent slice datasets is ignored!

### Variable Outline

To ensure a slice variable has the expected dimensionality, the `dims` 
setting is used. The following example defines the dimensions of the data variable
named `chlorophyll`

```json
{
    "variables": {
        "chl": { 
            "dims": ["time", "y", "x"]
        }
    }
}
```

An error will be raised if a variable from a subsequent slice has different 
dimensions.

### Variable Attributes

Extra variable attributes can be provided using the `attrs` setting:

```json
{
    "variables": {
        "chl": { 
            "attrs": {
                "units": "mg/m^3",
                "long_name": "chlorophyll_concentration"
            }
        }
    }
}
```

### Variable Encoding

_This section is a work in progress._

* The target chunking of the append dimension equals the size of the append 
  dimension in each slice and vice versa. 
* The target encoding should allow for specifying the target storage chunking, 
  data type, and compression. 
* Detect coordinate variables and allow them to stay un-chunked.
  This is important for coordinate variables containing or corresponding 
  to the append-dimension.

#### Chunking

#### Missing Values

_This section is a work in progress._

#### Compression

_This section is a work in progress._

#### Data Packing

_This section is a work in progress._

* The target encoding should also allow for packing floating point data into 
  integer data with fewer bits using scaling factor and offset.

* If the target exists, the slice will be appended. Check if the slice to be 
  appended is last. If not, refuse to append (alternative: insert but this is 
  probably difficult or error prone).
* Slices are appended in the order they are provided.


## Data I/O

_This section is a work in progress._

* `dry_run`

* `target_dir`
* `target_storage_options`
* `zarr_version`

* `slice_engine`
* `slice_storage_options`
* `persist_mem_slices`

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

### Slice Polling

_This section is a work in progress._

* `slice_polling`

### Transactions

_This section is a work in progress._

* `temp_dir`
* `temp_storage_options`
* `disable_rollback`

## Slice Data Types

_This section is a work in progress._


## Logging

The `zappend` logging configuration follows exactly the 
Python [dictionary schema](https://docs.python.org/3/library/logging.config.html#logging-config-dictschema) of the Python module `logging.config`.

The logger used by the `zappend` tool is named `zappend`.
Note that you can also configure the logger of other Python modules, e.g.,
`xarray` or `dask` here.

Given here is an example that logs `zappend`'s output 
to the console using the INFO level:

```json
{
    "logging": {
        "version": 1,
        "formatters": {
            "normal": {
                "format": "%(asctime)s %(levelname)s %(message)s",
                "style": "%"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "normal"
            }
        },
        "loggers": {
            "zappend": {
                "level": "INFO",
                "handlers": ["console"]
            }
        }
    }
}
```

