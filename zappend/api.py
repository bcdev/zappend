# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import xarray as xr
from typing import Any, Iterable

from .config import normalize_config
from .context import Context
from .processor import Processor


def zappend(target_path: str,
            slice_paths: Iterable[str | xr.Dataset],
            config: tuple[str, ...] | str | dict[str, Any] | None = None):
    """Tool to create or update a Zarr dataset from slices."""
    if not slice_paths:
        raise ValueError("slice_paths must be given")

    config = normalize_config(config)

    ctx = Context(target_path, config)
    processor = Processor(ctx)
    processor.process_slices(slice_paths)
