# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import xarray as xr
from typing import Any, Iterable

from .config import normalize_config
from .config import validate_config
from .context import Context
from .processor import Processor


def zappend(slice_uris: Iterable[str | xr.Dataset],
            config: tuple[str, ...] | str | dict[str, Any] | None = None,
            **kwargs):
    """Create or update a Zarr dataset from slices."""

    if not slice_uris:
        return

    config = normalize_config(config)
    config.update({k: v for k, v in kwargs if v is not None})
    validate_config(config)

    ctx = Context(config)
    processor = Processor(ctx)
    processor.process_slices(slice_uris)
