# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import xarray as xr

from zappend.log import logger
from ..source import SliceSource


class MemorySliceSource(SliceSource):
    """A slice source that uses the in-memory dataset passed in.

    Args:
        slice_ds: The slice dataset
        slice_index: An index for slice identification (logging only)
    """

    def __init__(self, slice_ds: xr.Dataset, slice_index: int):
        self._slice_ds = slice_ds
        self._slice_index = slice_index

    def get_dataset(self) -> xr.Dataset:
        logger.info(f"Processing in-memory slice dataset #{self._slice_index}")
        return self._slice_ds

    def dispose(self):
        self._slice_ds = None
        logger.info(f"Slice dataset #{self._slice_index} processed")
