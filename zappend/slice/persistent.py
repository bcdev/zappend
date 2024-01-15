# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import time

import xarray as xr

from ..context import Context
from ..fsutil.fileobj import FileObj
from ..log import logger
from .abc import SliceSource


class PersistentSliceSource(SliceSource):
    """
    A slice source that is persisted in some filesystem.

    Args:
        ctx: Processing context
        slice_file: Slice file object
    """

    def __init__(self, ctx: Context, slice_file: FileObj):
        super().__init__(ctx)
        self._slice_file = slice_file
        self._slice_ds: xr.Dataset | None = None

    def get_dataset(self) -> xr.Dataset:
        logger.info(f"Opening slice dataset from {self._slice_file.uri}")
        self._slice_ds = self._wait_for_slice_dataset()
        return self._slice_ds

    def dispose(self):
        if self._slice_ds is not None:
            self._slice_ds.close()
            self._slice_ds = None
        logger.info(f"Slice dataset {self._slice_file.uri} closed")
        super().dispose()

    def _wait_for_slice_dataset(self) -> xr.Dataset:
        slice_ds: xr.Dataset | None = None
        interval, timeout = self.ctx.slice_polling
        if timeout is not None:
            # t0 = time.monotonic()
            # while (time.monotonic() - t0) < timeout:
            t0 = time.monotonic()
            while True:
                delta = time.monotonic() - t0
                if delta >= timeout:
                    break
                try:
                    slice_ds = self._open_slice_dataset()
                except OSError:
                    logger.debug(
                        f"Slice not ready or corrupt, retrying after {interval} seconds"
                    )
                    time.sleep(interval)
        else:
            slice_ds = self._open_slice_dataset()

        if not slice_ds:
            raise FileNotFoundError(self._slice_file.uri)
        return slice_ds

    def _open_slice_dataset(self) -> xr.Dataset:
        engine = self.ctx.slice_engine
        if engine is None and (
            self._slice_file.path.endswith(".zarr")
            or self._slice_file.path.endswith(".zarr.zip")
        ):
            engine = "zarr"
        if engine == "zarr":
            storage_options = self.ctx.slice_storage_options
            return xr.open_zarr(self._slice_file.uri, storage_options=storage_options)

        with self._slice_file.fs.open(self._slice_file.path, "rb") as f:
            return xr.open_dataset(f, engine=engine)
