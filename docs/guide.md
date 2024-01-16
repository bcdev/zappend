# User Guide

After [installation](start.md), you can either use the `zappend` [CLI command](cli.md)

```shell
zappend -t output/mycube.zarr inputs/*.nc
```

or the `zappend` [Python function](api.md)

```bash
from zappend.api import zappend

zappend(os.listdir("inputs"), target_dir="output/mycube.zarr")
```

Both the CLI command and the Python function can be run without any further 
configuration provided the paths of the target dataset and the source slice datasets 
are given. The target dataset path must point to a directory that will contain a Zarr 
group to be created and updated. The slice dataset paths may be provided as Zarr as 
well or in other data formats supported by the [xarray.open_dataset()](https://docs.xarray.dev/en/stable/generated/xarray.open_dataset.html) 
function. The target and slice dataset are allowed to live in different filesystems. 
Additional filesystem storage options may be specified via the tool's configuration.

The tool takes care of generating the target dataset from slice datasets, but doesn't 
care how the slice datasets are created. Hence, when using the Python `zappend()` 
function, the slice datasets can be provided in various forms. More on this below.

To run the `zappend` tool with [configuration](config.md) you can pass one or more
configuration files using JSON or YAML format

```shell
zappend -t output/mycube.zarr -c config.yaml inputs/*.nc
```

If multiple configuration files are passed, they will be merged into one by
incrementally updating the first by subsequent ones. 

You can pass configuration settings to the `zappend` Python function by the optional
`config` keyword argument. Other keyword arguments are interpreted as individual 
configuration settings and will be merged into the one given by `config` argument,
if any. The `config` keyword argument can be given as local file path or URL 
(type `str`) pointing to a JSON or YAML file. It can also be given as dictionary,
or as a sequence of the aforementioned types. Configuration sequences are again merged
into one.

```python
import os
from zappend.api import zappend

zappend(os.listdir("inputs"), 
        config=["configs/base.yaml",
                "configs/mycube.yaml"], 
        target_dir="outputs/mycube.zarr",
        dry_run=True)
```

This remainder of this guide explains the how to use the various `zappend` 
[configuration](config.md) settings.

!!! note
    We use the term _Dataset_ in the same way `xarray` does: A dataset comprises any 
    number of multidimensional _Data Variables_, and usually 1-dimensional 
    _Coordinate Variables_ that provide the labels for the dimensions used by the data 
    variables. A variable comprises the actual data array as well as metadata describing 
    the data dimensions, units, and encoding, such as chunking and compression.

## Dataset Outline

If no further configuration is supplied, then the target dataset's outline and data 
encoding is fully prescribed by the first slice dataset provided. By default, the 
dimension along subsequent slice datasets are concatenated is `time`. If you use a 
different append dimension, the `append_dim` setting can be used to specify its name:

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
    "included_variables": [
        "time", "y", "x",
        "chl", 
        "tsm"
    ]
}
```

Often, it is easier to tell which variables should be excluded:

```json
{
    "excluded_variables": ["GridCellId"]
}
```

## Variable Metadata 

Without any additional configuration, `zappend` uses the dimensions, attributes, 
and encoding information from the data variables of the first slice dataset. 
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
        "*": { 
        }
    }
}
```

If `*` is specified, the effective variable metadata applied is gained by merging a 
given specific metadata, into the common metadata given by `*`, which is eventually 
merged into metadata of the variable in the first dataset slice.

!!! note
    Only metadata from the first slice dataset is used, metadata of variables from 
    subsequent slice datasets is ignored entirely.

### Variable Dimensions

To ensure a slice variable has the expected dimensionality and shape, the `dims` 
setting is used. The following example defines the dimensions of a data variable 
named `chl` (Chlorophyll):

```json
{
    "variables": {
        "chl": { 
            "dims": ["time", "y", "x"]
        }
    }
}
```

An error will be raised if a variable from a subsequent slice has different dimensions.

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

Encoding metadata specifies how array data is stored in the target dataset and includes 
storage data type, packing, chunking, and compression. Encoding metadata for a given 
variable is provided by the `encoding` setting. Since the encoding is often shared by 
multiple variables the wildcard variable name `*` can often be of help.

!!! tip "Verify encoding is as expected"
    To verify that `zappend` uses the expected encoding for your variables create a 
    target dataset for testing from your first slice dataset and open it using 
    `ds = xarray.open_zarr(target_dir, decode_cf=False)`. Then inspect dataset `ds` 
    using the Python console or Jupyter Notebook (attribute `ds.<var>.encoding`).
    You can also inspect the Zarr directly by opening the `<target_dir>/<var>/.zarray`
    or `<target_dir>/.zmetadata` metadata JSON files.    
    

#### Chunking

By default, the chunking of the coordinate variable corresponding to the append 
dimension will be its dimension in the first slice dataset. Often, this will be one or 
a small number. Since `xarray` loads coordinates eagerly when opening a dataset, this 
can lead to performance issues if the target dataset is served from object storage such 
as S3. This is because, a separate HTTP request is required for every single chunk. It 
is therefore very advisable to set the chunks of that variable to a larger number using 
the `chunks` setting. For other variables, the chunking within the append dimension may 
stay small if desired:

```json
{
    "variables": {
        "time": { 
            "dims": ["time"],
            "encoding": {
                "chunks": [1024]
            }
        },
        "chl": { 
            "dims": ["time", "y", "x"],
            "encoding": {
                "chunks": [1, 2048, 2048]
            }
        }
    }
}
```

#### Missing Data

To indicate missing data in a variable data array, a dedicated no-data or missing value 
can be specified by the `fill_value` setting. The value is given in a variable's storage 
type and storage units, see next section _Data Packing_.

```json
{
    "variables": {
        "chl": { 
            "encoding": {
                "fill_value": -999
            }
        }
    }
}
```

If the `fill_value` is not specified, the default is `NaN` (given as string `"NaN"` 
in JSON) if the storage data type is floating point; it is `None` (`null` in JSON) 
if the storage data types is integer, which effectively means, no fill value is used. 
You can also explicitly set `fill_value` to `null` (`None` in Python) to not use one.
                  
Setting the `fill_value` for a variable can be important for saving storage space and 
improving data I/O performance in many cases, because `zappend` does not write empty 
array chunks - chunks that comprise missing data only, i.e., 
`slice.to_zarr(target_dir, write_empty_chunks=False, ...)`.

#### Data Packing

_Data packing_ refers to a simple lossy data compression method where 32- or 64-bit 
floating point values are linearly scaled so that their value range can be fully or 
partially represented by a lower precision integer data type. Packed values usually
also give higher compression rates when using a `compressor`, see next section.

Data packing is specified using the `scale_factor` and `add_offset` settings together
with the storage data type setting `dtype`. The settings should be given as a triple:

```json
{
    "variables": {
        "chl": { 
            "encoding": {
                "dtype": "int16",
                "scale_factor": 0.005,
                "add_offset": 0.0
            }
        }
    }
}
```

The in-memory value in its physical units for a given encoded value in storage is 
computed according to 

```python
memory_value = scale_factor * storage_value + add_offset
```

Hence, the encoded value is computed from an in-memory value in physical units as

```python
storage_value = (memory_value - add_offset) / scale_factor
```

You can compute `scale_factor` and `add_offset` from given data range in physical units
according to

```python
  add_offset = memory_value_min
  scale_factor = (memory_value_max - memory_value_min) / (2 ** num_bits - 1)
```

with `num_bits` being the number of bits for the integer type to be used.

#### Compression

Data compression is specified by the `compressor` setting, optionally paired with the
`filters` setting: 

```json
{
    "variables": {
        "chl": { 
            "encoding": {
                "compressor": {},
                "filters": []
            }
        }
    }
}
```

By default, `zappend` uses default the default `blosc` compressor of Zarr, if not 
specified. To explicitly disable compression you must set the `compressor` to `None` 
(`null` in JSON).

The usage of compressors and filters is best explained in dedicated sections of the 
[Zarr Tutorial](https://zarr.readthedocs.io/en/stable/tutorial.html), namely 
[_Compressors_](https://zarr.readthedocs.io/en/stable/tutorial.html#compressors) and 
[_Filters_](https://zarr.readthedocs.io/en/stable/tutorial.html#filters).

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

The `zappend` logging configuration follows exactly the [dictionary schema](https://docs.python.org/3/library/logging.config.html#logging-config-dictschema) of the 
Python module `logging.config`. The logger used by the `zappend` tool is named 
`zappend`. Note that you can also configure the logger of other Python modules, e.g.,
`xarray` or `dask` here.

Given here is an example that logs `zappend`'s output to the console using 
the INFO level:

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

