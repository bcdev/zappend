# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import xarray as xr
from typing import Any, Iterable

from .config import ConfigLike
from .processor import Processor


def zappend(slices: Iterable[str | xr.Dataset],
            config: ConfigLike = None,
            **kwargs):
    """Create or update a Zarr dataset from slices."""
    processor = Processor(config, **kwargs)
    processor.process_slices(slices)
