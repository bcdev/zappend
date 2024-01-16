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

Slice datasets can be passed in a number of ways, please refer to the section 
[_Slice Sources_](guide.md#slice-sources) in the [User Guide](guide.md).
