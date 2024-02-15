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

Both invocations will create the Zarr dataset `output/mycube.zarr` by concatenating
the "slice" datasets provided in the `inputs` directory along their `time` dimension. 
`target_dir` must specify a directory for the Zarr dataset. (Its parent directory must 
exist.) Both the CLI command and the Python function can be run without any further 
configuration provided the paths of the target dataset and the source slice datasets 
are given. The target dataset path must point to a directory that will contain a Zarr 
group to be created and updated. The slice dataset paths may be provided as Zarr as 
well or in other data formats supported by the [xarray.open_dataset()](https://docs.xarray.dev/en/stable/generated/xarray.open_dataset.html) 
function. Because we provided no additional configuration, the default append dimension
`time` is used above.

The target and slice datasets are allowed to live in filesystems other than the local
one, if their paths are given as URIs prefixed with a filesystem protocol such as 
`s3://` or `memory://`. Additional filesystem storage options may be specified via 
dedicated configuration settings. More on this is given
in section [_Data I/O_](#data-io) below.

!!! note "Zarr Format v2"
    By default, `zappend` uses the [Zarr storage specification 2](https://zarr.readthedocs.io/en/stable/spec/v2.html)
    and has only been tested with this version. The `zarr_version` setting can be used 
    to change it, e,g, to `3`, but any other value than `2` is currently unsupported.

The tool takes care of generating the target dataset from slice datasets, but doesn't 
care how the slice datasets are created. Hence, when using the Python `zappend()` 
function, the slice datasets can be provided in various forms. More on this is given
in section [_Slice Sources_](#slice-sources) below.

To run the `zappend` tool with [configuration](config.md) you can pass one or more
configuration files using JSON or YAML format:

```shell
zappend -t output/mycube.zarr -c config.yaml inputs/*.nc
```

If multiple configuration files are passed, they will be merged into one by
incrementally updating the first by subsequent ones. 

!!! info "Environment Variables"
    It is possible to include the values of environment variables in JSON or YAML 
    configuration files using the syntax `${ENV_VAR}` or just `$ENV_VAR`.

You can pass configuration settings to the `zappend` Python function with 
the optional `config` keyword argument. Other keyword arguments are 
interpreted as individual configuration settings and will be merged into the 
one given by `config` argument, if any. The `config` keyword argument can be 
given as local file path or URL (type `str`) pointing to a JSON or YAML file.
It can also be given as dictionary, or as a sequence of the aforementioned 
types. Configuration sequences are again merged into one.

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

## Dataset Metadata

### Outline

If no further configuration is supplied, then the target dataset's outline and data 
encoding is fully prescribed by the first slice dataset provided. By default, the
dimension along which subsequent slice datasets are concatenated is `time`. If
you use a different append dimension, the `append_dim` setting can be used to
specify its name:

```json
{
    "append_dim": "depth"
}
```

The configuration setting `append_step` can be used to validate the step sizes 
between the labels of a coordinate variable associated with the append dimension. 
Its value can be a number for numerical labels or a timedelta value of the form
`<count><unit>` for date/time labels. In the latter case `<count>` is an integer 
and `<units>` is one of the possible
[numpy datetime units](https://numpy.org/doc/stable/reference/arrays.datetime.html#datetime-units), 
for example, `8h` (8 hours) or `2D` (two days). Numerical and timedelta values
may be negative. `append_step` can also take the two special values `"+"` and 
`"-"`. In this case it is just verified that the append labels are monotonically 
increasing and decreasing, respectively.

```json
{
    "append_dim": "time",
    "append_step": "2D"
}
```

Other, non-variadic dimensions besides the append dimension can and should 
be specified using the `fixed_dims` setting which is a mapping from dimension 
name to the fixed dimension size, e.g.:

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
variables should be used from the slice dataset, they can be specified using
the `included_variables` setting, which is a list of names of variables that
will be included:

```json
{
    "included_variables": [
        "time", "y", "x",
        "chl", 
        "tsm"
    ]
}
```

Often, it is easier to specify which variables should be excluded:

```json
{
    "excluded_variables": ["GridCellId"]
}
```
### Attributes

The target dataset should exploit information about itself using global 
metadata attributes.
There are three choices to update the global attributes of the target 
dataset from slices. The configuration setting `attrs_update_mode` 
controls how this is done:

* `"keep"` - use attributes from first slice dataset and keep them (default);
* `"replace"` - replace existing attributes by attributes of last slice dataset;
* `"update"` - update existing attributes by attributes of last slice dataset;
* `"ignore"` - ignore attributes from slice datasets.

Extra attributes can be added using the optional configuration setting `attrs`:

```json
{
    "attrs_update_mode": "keep",
    "attrs": {
        "Conventions": "CF-1.10",
        "title": "SMOS Level 2C Soil Moisture 2-Days Composite"
    }
}
```

Independently of the `attrs_update_mode` setting, extra attributes configured 
by the `attrs` setting will always be used to update the resulting target 
attributes. 

Attribute values in the `attrs` setting may also be computed dynamically using 
the syntax `{{ expression }}`, where `expression` is an arbitrary Python 
expression. For this to work, the setting `permit_eval` must be explicitly 
set for security reasons:

```json
{
    "permit_eval": true,
    "attrs_update_mode": "keep",
    "attrs": {
        "time_coverage_start": "{{ ds.time[0] }}", 
        "time_coverage_end": "{{ ds.time[-1] }}"
    }
}
```

Currently, the only variable accessible from expressions is `ds` which is 
a reference to the current state of the target dataset after the last slice 
append. It is of type 
[xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html).

!!! danger "Evil eval()"
    The expressions in `{{ expression }}` are evaluated using the Python 
    [eval() function](https://docs.python.org/3/library/functions.html#eval).
    This can pose a threat to your application and environment. 
    Although `zappend` does not allow you to directly access Python built-in 
    functions via expressions, it should be used judiciously and with extreme 
    caution if used as part of a web service where configuration is injected 
    from the outside of your network.

The following utility functions can be used as well and are handy if you need 
to store the upper and lower bounds of coordinates as attribute values:

* `lower_bound(array, ref: "lower"|"upper"|"center" = "lower")`:
  Return the lower bound of a one-dimensional (coordinate) array `array`.
* `upper_bound(array, ref: "lower"|"upper"|"center" = "lower")`:
  Return the upper bound of a one-dimensional (coordinate) array `array`.
 
The `ref` value specifies the reference within an array element that is used 
as a basis for the boundary computation. E.g., if coordinate labels refer to 
array element centers, pass `ref="center"`.
    
```json
{
    "attrs": {
        "time_coverage_start": "{{ lower_bound(ds.time, 'center') }}", 
        "time_coverage_end": "{{ upper_bound(ds.time, 'center') }}"
    }
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

If `*` is specified, the metadata for a particular variable is generated by
merging the specific metadata for that variable into the common metadata given
by `*`, which is eventually merged into metadata of the variable in the first
dataset slice.

!!! note
    Only metadata from the first slice dataset is used. Metadata of variables
    from subsequent slice datasets is ignored entirely.

### Dimensions

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

### Attributes

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

### Encoding

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

This section describes the configuration of how data is read and written.

All input and output can be configured to take place in different filesystem. To 
specify a filesystem other than the local one, you can use URIs and URLs for path 
configuration settings such `target_dir` and `temp_dir` as well as for the slice 
dataset paths. The filesystem is given by an URI's protocol prefix, such as `s3://`,
which specifies the S3 filesystem. Additional storage parameters may be required to 
access the data which can be provided by the settings `target_storage_options`, 
`temp_storage_options`, and `slice_storage_options` which must be given as dictionaries 
or JSON objects. The supported filesystems and their storage options are given by the 
[fsspec](https://filesystem-spec.readthedocs.io/) package.

!!! tip
    You can use the `dry_run` setting to supress creation or modification of any files
    in the filesystem. This is useful for testing, e.g., make sure configuration is 
    valid and slice datasets can be read without errors. 


While the target dataset is being modified, a file lock is created used to effectively 
prevent concurrent dataset modifications. After successfully appending a complete slice
dataset, the lock is removed from the target. The lock file is written next to the target
dataset, using the same filesystem and parent directory path. Its filename is the 
filename of the target dataset suffixed by the extension `.lock`.

### Transactions

Appending a slice dataset is an atomic operation to ensure the target dataset's 
integrity. That is, in case a former append step failed, a rollback is performed to 
restore the last valid state of the target dataset. The rollback takes place 
immediately after a target dataset modification failed. The rollback include restoring 
all changed files and removing added files. After the rollback you can analyse what
went wrong and try to continue appending slices at the point it failed.

To allow for rollbacks, a slice append operation is treated as a transaction,
hence temporary files must be written, e.g., to record required rollback actions and to
save backup files with the original data. The location of the temporary transaction 
files can be configured using the optional `temp_dir` and `temp_storage_options`
settings:

```json
{
    "temp_dir": "memory://temp"
}
```
The default value for `temp_dir` is your operating system's location for temporary 
data (Python `tempfile.gettempdir()`). 

You can disable transaction management by specifying

```json
{
    "disable_rollback": true
}
```

### Target Dataset

The `target_dir` setting is mandatory. If it is not specified in the configuration,
it must be passed either as `--target` or `-t` option to the `zappend` command or as 
`target_dir` keyword argument when using the `zappend` Python function.
Note, the parent directory of `target_dir` must already exist.

If the target path is given for another filesystem, additional storage options may be 
passed using the optional `target_storage_options` setting. 

```json
{
    "target_dir": "s3://wqservices/cubes/chl-2023.zarr",
    "target_storage_options": {
        "anon": false,
        "key": "...",
        "secret": "...",
        "endpoint_url": "https://s3.acme.org"
    }
}
```

### Slice Datasets

If the slice paths passed to the `zappend` tool are given as URIs,
additional storage options may be provided for the filesystem given by the 
URI's protocol. They may be specified using the `slice_storage_options` setting.

Sometimes, the slice dataset to be processed are not yet available, e.g., 
because another process is currently generating them. For such cases, the 
`slice_polling` setting can be used. It provides the poll interval and the timeout 
values in seconds. If this setting is used, and the slice dataset does not yet exist or 
fails to open, the tool will retry to open it after the given interval. It will stop 
doing so and exit with an error if the total time for opening the slice dataset exceeds
the given timeout:

```json
{
    "slice_polling": {
        "interval": 2,
        "timeout": 600
    } 
}
```

Or use default polling:

```json
{
    "slice_polling": true 
}
```


### Slice Sources

A _slice source_ is an object that provides a slice dataset of type `xarray.Dataset` 
for given parameters of any type.

The optional `slice_source` configuration setting is used to specify a custom 
slice source. If not specified, `zappend` selects the slice source based on the type 
of a given slice object. These types are described in following subsections.

If given, the value of the `slice_source` setting is a class derived from
`zappend.api.SliceSource`, or a function that creates an instance of 
`zappend.api.SliceSource`, or the fully qualified name of the aforementioned. 
In the case `slice_source` is given, the _slices_ argument passed to the CLI 
command and Python function become parameters to the specified class constructor 
or factory function.
The individual slice items in the `SLICES` arguments of the `zappend` CLI 
command are of type `str`, typically interpreted as file paths or URIs.
The individual slice items passed in the `slices` argument of the
`zappend.api.zappend()` function can be of any type, but the `tuple`, `list`, 
and `dict` types have a special meaning:

* `tuple`: a pair of the form `(args, kwargs)`, where `args` is a list
  or tuple of positional arguments and `kwargs` is a dictionary of keyword
  arguments;
  * `list`: positional arguments only;
* `dict`: keyword arguments only;
* Any other type is interpreted as single positional argument.

In addition, your class constructor or factory function specified by `slice_source` 
may specify a positional or keyword argument named `ctx`, which will receive the 
current processing context of type `zappend.api.Context`. 

If the `slice_source` setting is _not_ specified, the slice items passed as `slices`
argument to the [`zappend`](api.md) Python function can be one of the types described
in the following subsections.

#### `str` and `zappend.api.FileObj`

A slice object of type `str` is interpreted as local file path or URI, in the case 
the path has a protocol prefix, such as `s3://`.

An alternative to providing the slice dataset as path or URI is using the `FileObj` 
class, which combines a URI with dedicated filesystem storage options.

```python
from zappend.api import FileObj

slice_obj = FileObj(slice_uri, storage_options=dict(...)) 
```

#### `xarray.Dataset`

In-memory slice objects can be passed as [xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html) objects. 
Such objects may originate from opening datasets from some storage 

```python
import xarray as xr

slice_obj = xr.open_dataset(slice_store, ...) 
```

or by composing, aggregating, resampling slice datasets from other datasets and 
data variables. To allow for out-of-core computation of large datasets [Dask arrays](https://docs.dask.org/en/stable/array.html)
are used by both `xarray` and `zarr`. As a dask array may represent complex and/or 
expensive processing graphs, high CPU loads and memory consumption are common issues
for computed slice datasets, especially if the specified target dataset chunking is 
different from the slice dataset chunking. This may cause Dask graphs to be 
computed multiple times if the source chunking overlaps multiple target chunks, 
potentially causing large resource overheads while recomputing and/or reloading the same
source chunks multiple times.

In such cases it can help to "terminate" such computations for each slice by 
persisting the computed dataset first and then to reopen it. This can be specified 
using the `persist_mem_slice` setting: 

```json
{
    "persist_mem_slice": true
}
```

If the flag is set, in-memory slices will be persisted to a temporary Zarr before 
appending them to the target dataset. It may prevent expensive re-computation of chunks 
at the cost of additional i/o. It therefore defaults to `false`.

#### `zappend.api.SliceSource`

Often you want to perform some custom cleanup after a slice has been processed and
appended to the target dataset. In this case you can write your own 
`zappend.api.SliceSource` by implementing its `get_dataset()` and `dispose()`
methods. 

Slice source instances are supposed to be created by _slice factories_, see 
subsection below.

#### `zappend.api.SliceFactory`

A slice factory is a 1-argument function that receives a processing context of type
`zappend.api.Context` and yields a slice dataset object of one of the types
described above. Since a slice factory cannot have additional arguments, it is 
normally defined as a [closure](https://en.wikipedia.org/wiki/Closure_(computer_programming)) 
to capture slice-specific information.

In the following example, the actual slice dataset is computed from averaging another 
dataset. A `SliceSource` is used to close the datasets after the slice has been 
processed. Slice factories are created from the custom slice source and the slice paths
using the utility function [to_slice_factories()][zappend.slice.factory.to_slice_factories]:

```python
import numpy as np
import xarray as xr
from zappend.api import SliceSource
from zappend.api import to_slice_factories
from zappend.api import zappend

def get_mean_time(slice_ds: xr.Dataset) -> xr.DataArray:
    time = slice_ds.time
    t0 = time[0]
    dt = time[-1] - t0
    return xr.DataArray(np.array([t0 + dt / 2], 
                                 dtype=slice_ds.time.dtype), 
                        dims="time")

def get_mean_slice(slice_ds: xr.Dataset) -> xr.Dataset: 
    mean_slice_ds = slice_ds.mean("time")
    mean_slice_ds = mean_slice_ds.expand_dims("time", axis=0)
    mean_slice_ds.coords["time"] = get_mean_time(slice_ds)
    return mean_slice_ds 
    
class MySliceSource(SliceSource):
    def __init__(self, ctx, slice_path):
        super().__init__(ctx)
        self.slice_path = slice_path
        self.ds = None
        self.mean_ds = None

    def get_dataset(self):
        self.ds = xr.open_dataset(self.slice_path)
        self.mean_ds = get_mean_slice(self.ds)
        return self.mean_ds

    def dispose(self):
        if self.ds is not None:
            self.ds.close()
            self.ds = None
        if self.mean_ds is not None:
            self.mean_ds.close()
            self.mean_ds = None
        
zappend(to_slice_factories(MySliceSource, ["slice-1.nc", "slice-2.nc", "slice-3.nc"]),
        target_dir="target.zarr")
```

Note, the above example can be simplified by using the `slice_source` setting directly:

```python
zappend(["slice-1.nc", "slice-2.nc", "slice-3.nc"],
        target_dir="target.zarr",
        slice_source=MySliceSource)
```

## Profiling

Runtime profiling is very important for understanding program runtime behavior 
and performance. The configuration setting `profiling` can be used to
analyse and improve the runtime performance of `zappend` itself as well as 
the runtime performance of the computation of in-memory slices passed to the
`zappend()` function.

To log the output of the profiling with level `INFO` (see next section 
[Logging](#logging)), you can use the value `true`:

```json
{
    "profiling": true
}
```

If you like to see the output in a file too, then set `profiling` to the 
desired local file path:

```json
{
    "profiling": "perf.out"
}
```

You can also set `profiling` to an object that allows for fine-grained 
control of the runtime logging: 

```json
{
    "profiling": {
        "log_level": "WARNING",
        "path": "perf.out",
        "keys": ["tottime", "time", "ncalls"]
    }
}
```

Please refer to section [`profiling`](config.md#profiling) in 
the [Configuration Reference](config.md) for more details.


## Logging

The `zappend` logging output is configured using the `logging` setting.
Its configuration follows exactly the 
[dictionary schema](https://docs.python.org/3/library/logging.config.html#logging-config-dictschema)
of the Python module `logging.config`. The logger used by the `zappend` tool is named 
`zappend`. Note that you can also configure the logger of other Python modules, e.g.,
`xarray` or `dask` using an entry in the `loggers` setting.

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

