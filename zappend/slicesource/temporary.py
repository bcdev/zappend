# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import xarray as xr

from .memory import MemorySliceSource
from ..context import Context
from ..fsutil.fileobj import FileObj
from ..log import logger


class TemporarySliceSource(MemorySliceSource):
    """A slice source that persists the in-memory dataset and returns
     the re-opened dataset instance.

    :param ctx: Processing context
    :param slice_ds: The slice dataset
    :param slice_index: An index for slice identification
    """

    def __init__(self, ctx: Context, slice_ds: xr.Dataset, slice_index: int):
        super().__init__(ctx, slice_ds, slice_index)
        self._temp_slice_dir: FileObj | None = None
        self._temp_slice_ds: xr.Dataset | None = None

    def open(self) -> xr.Dataset:
        slice_index = self._slice_index
        temp_slice_dir = self.ctx.temp_dir / f"slice-{self._slice_index}.zarr"
        self._temp_slice_dir = temp_slice_dir
        temp_slice_store = temp_slice_dir.fs.get_mapper(
            temp_slice_dir.path, create=True
        )
        logger.info(
            f"Persisting in-memory slice dataset #{self._slice_index}"
            f" to {temp_slice_dir.uri}"
        )
        self._slice_ds.to_zarr(temp_slice_store)
        self._slice_ds = None
        self._temp_slice_ds = xr.open_zarr(temp_slice_store)
        return self._temp_slice_ds

    def close(self):
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

        super().close()
