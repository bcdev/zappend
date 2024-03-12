# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import xarray as xr

from zappend.context import Context
from zappend.fsutil.fileobj import FileObj
from zappend.log import logger
from .memory import MemorySliceSource


class TemporarySliceSource(MemorySliceSource):
    """A slice source that persists the in-memory dataset and returns
     the re-opened dataset instance.

    Args:
        ctx: Processing context
        slice_ds: The slice dataset
        slice_index: An index for slice identification
    """

    def __init__(self, ctx: Context, slice_ds: xr.Dataset, slice_index: int):
        super().__init__(slice_ds, slice_index)
        self._config = ctx.config
        self._temp_slice_dir: FileObj | None = None
        self._temp_slice_ds: xr.Dataset | None = None

    def get_dataset(self) -> xr.Dataset:
        slice_index = self._slice_index
        temp_slice_dir = self._config.temp_dir / f"slice-{slice_index}.zarr"
        self._temp_slice_dir = temp_slice_dir
        temp_slice_store = temp_slice_dir.fs.get_mapper(
            temp_slice_dir.path, create=True
        )
        logger.info(
            f"Persisting in-memory slice dataset #{slice_index}"
            f" to {temp_slice_dir.uri}"
        )
        self._slice_ds.to_zarr(temp_slice_store)
        self._slice_ds = None
        self._temp_slice_ds = xr.open_zarr(temp_slice_store)
        return self._temp_slice_ds

    def dispose(self):
        if self._temp_slice_ds is not None:
            self._temp_slice_ds.close()
            self._temp_slice_ds = None

        temp_slice_dir = self._temp_slice_dir
        if temp_slice_dir is not None:
            self._temp_slice_dir = None
            if temp_slice_dir.exists():
                logger.info(
                    f"Removing temporary dataset {temp_slice_dir.uri}"
                    f" for slice #{self._slice_index}"
                )
                temp_slice_dir.delete(recursive=True)

        super().dispose()
