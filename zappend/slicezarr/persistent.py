# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import time

import xarray as xr

from ..config import SLICE_ACCESS_MODE_SOURCE
from ..context import Context
from ..fileobj import FileObj
from ..log import logger
from ..outline import DatasetOutline
from ..outline import check_compliance
from .abc import SliceZarr
from .inmemory import InMemorySliceZarr


class PersistentSliceZarr(SliceZarr):
    """
    A slice Zarr that is persisted in some filesystem.

    :param ctx: Processing context
    :param slice_file: Slice file object
    """

    def __init__(self, ctx: Context, slice_file: FileObj):
        super().__init__(ctx)
        self._slice_file = slice_file
        self._slice_ds: xr.Dataset | None = None
        self._slice_outline: DatasetOutline | None = None
        self._mem_slice_zarr: InMemorySliceZarr | None = None

    def prepare(self) -> FileObj:
        logger.info(f"Opening slice from {self._slice_file.uri}")

        slice_ds = self._wait_for_slice_dataset()

        slice_access_mode = self._ctx.slice_access_mode
        if slice_access_mode == SLICE_ACCESS_MODE_SOURCE:
            slice_outline = DatasetOutline.from_dataset(slice_ds)
            compliant = check_compliance(self._ctx.target_outline,
                                         slice_outline,
                                         self._slice_file.uri,
                                         error=False)
            if compliant:
                logger.info("Using slice source directly")
                # No longer the dataset
                slice_ds.close()
                return self._slice_file

        self._slice_ds = slice_ds  # Save instance so we can close it later
        self._mem_slice_zarr = InMemorySliceZarr(self._ctx, slice_ds)
        return self._mem_slice_zarr.prepare()

    def dispose(self):
        if hasattr(self, "slice_zarr") and self._mem_slice_zarr is not None:
            self._mem_slice_zarr.dispose()
            self._mem_slice_zarr = None
            del self._mem_slice_zarr
        if hasattr(self, "slice_ds") and self._slice_ds is not None:
            self._slice_ds.close()
            self._slice_ds = None
            del self._slice_ds
        super().dispose()

    def _wait_for_slice_dataset(self) -> xr.Dataset:
        slice_ds: xr.Dataset | None = None
        interval, timeout = self._ctx.slice_polling
        if timeout is not None:
            t0 = time.monotonic()
            while (time.monotonic() - t0) < timeout:
                try:
                    slice_ds = self._open_slice_dataset()
                except OSError:
                    logger.debug(
                        f"Slice not ready or corrupt,"
                        f" retrying after {interval} seconds"
                    )
                    time.sleep(interval)
        else:
            slice_ds = self._open_slice_dataset()

        if not slice_ds:
            raise FileNotFoundError(self._slice_file.uri)
        return slice_ds

    def _open_slice_dataset(self) -> xr.Dataset:
        return xr.open_dataset(self._slice_file.uri,
                               storage_options=self._ctx.slice_storage_options,
                               decode_cf=False)
