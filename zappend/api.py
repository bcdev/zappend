# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import xarray as xr
from typing import Any, Iterable

from .config import normalize_config
from .config import validate_config
from .context import Context
from .processor import Processor


def zappend(slices: Iterable[str | xr.Dataset],
            config: tuple[str, ...] | str | dict[str, Any] | None = None,
            **kwargs):
    """Create or update a Zarr dataset from slices."""

    if not slices:
        return

    config = normalize_config(config)
    config.update({k: v for k, v in kwargs.items() if v is not None})
    validate_config(config)

    ctx = Context(config)
    processor = Processor(ctx)
    processor.process_slices(slices)
