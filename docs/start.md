# Getting Started

## Installation

`zappend` requires a Python v3.10+ environment. To install the latest released
version from PyPI:

```shell
pip install zappend
```

To install the latest version for development, clone the
[repository](https://github.com/bcdev/zappend), and with the repository’s root
directory as the current working directory execute:

```shell
pip install --editable .
```


## Using the CLI

Get usage help:

```shell
zappend --help
```

Get [configuration](config.md) help in Markdown format (json also available):

```shell
zappend --help-config md
```

Process list of local slice paths:

```shell
zappend --target target.zarr slice-1.nc slice-2.nc slice-3.nc
```

Process list of local slice paths with [configuration](config.md) in
`config.yaml`:

```shell
zappend --config config.yaml slice-1.nc slice-2.nc slice-3.nc
```

## Using the Python API

Process list of local slice paths:


```python
from zappend.api import zappend

zappend(["slice-1.nc", "slice-2.nc", "slice-3.nc"], target_dir="target.zarr")
```

Process list of slices stored in S3 [configuration](config.md) in `config`:

```python
from zappend.api import zappend

config = { 
    "target_dir": "target.zarr",
    "slice_storage_options": {
        "key": "...",               
        "secret": "...",               
    } 
}

zappend((f"s3:/mybucket/data/{name}" 
         for name in ["slice-1.nc", "slice-2.nc", "slice-3.nc"]), 
        config=config)
```

Slice items can also be arguments passed to your custom _slice source_, 
a function or class that provides the actual slice to be appended:

```python
import xarray as xr
from zappend.api import zappend


def get_dataset(path: str):
    ds = xr.open_dataset(path)
    return ds.drop_vars(["ndvi_min", "ndvi_max"])

zappend(["slice-1.nc", "slice-2.nc", "slice-3.nc"], 
        slice_source=get_dataset,
        target_dir="target.zarr")
```

For the details, please refer to the section [_Slice Sources_](guide.md#slice-sources) in the 
[User Guide](guide.md).


