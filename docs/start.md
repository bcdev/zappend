# Getting Started

## Installation

```bash
pip install zappend
```

## Using the CLI

```bash
zappend --help
```

```bash
zappend --help-config md
```

```bash
zappend --target target.zarr slice-1.nc slice-2.nc slice-3.nc
```

```bash
zappend --config config.yaml slice-1.nc slice-2.nc slice-3.nc
```

## Using the Python API

```python
from zappend.api import zappend

zappend(["slice-1.nc", "slice-2.nc", "slice-3.nc"], target_dir="target.zarr")

config = { "target_dir": "target.zarr" }
zappend(["slice-1.nc", "slice-2.nc", "slice-3.nc"], config=config)
```

