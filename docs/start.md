# Getting Started

## Installation

Installing `zappend` into a Python v3.10+ environment:

```shell
pip install zappend
```

## Using the CLI

Get usage help:

```shell
zappend --help
```

Get [configuration](config.md) help: 

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

Process slice paths in S3 with slice generator and [configuration](config.md):

```python
import numpy as np
import xarray as xr
from zappend.api import zappend

config = { "target_dir": "target.zarr" }

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
    
def get_slices(slice_paths: list[str]):
    for slice_path in slice_paths:
        ds = xr.open_dataset("s3://mybucket/eodata/" + slice_path)
        yield get_mean_slice(ds) 
        
zappend(get_slices(["slice-1.nc", "slice-2.nc", "slice-3.nc"]),
        config=config)
```
