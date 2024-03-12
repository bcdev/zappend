# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import time

import fsspec.implementations.local
import xarray as xr

from zappend.context import Context
from zappend.fsutil.fileobj import FileObj
from zappend.log import logger
from ..source import SliceSource


class PersistentSliceSource(SliceSource):
    """
    A slice source that is persisted in some filesystem.

    Args:
        ctx: Processing context
        slice_file: Slice file object
    """

    def __init__(self, ctx: Context, slice_file: FileObj):
        self._config = ctx.config
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
        interval, timeout = self._config.slice_polling
        if timeout is None:
            return self._open_slice_dataset()

        # t0 = time.monotonic()
        # while (time.monotonic() - t0) < timeout:
        t0 = time.monotonic()
        while True:
            delta = time.monotonic() - t0
            if delta >= timeout:
                raise FileNotFoundError(self._slice_file.uri)
            try:
                return self._open_slice_dataset()
            except OSError:
                logger.debug(
                    f"Slice not ready or corrupt, retrying after {interval} seconds"
                )
                time.sleep(interval)

    def _open_slice_dataset(self) -> xr.Dataset:
        engine = self._config.slice_engine
        if engine is None and (
            self._slice_file.path.endswith(".zarr")
            or self._slice_file.path.endswith(".zarr.zip")
        ):
            engine = "zarr"
        if engine == "zarr":
            storage_options = self._config.slice_storage_options
            return xr.open_zarr(self._slice_file.uri, storage_options=storage_options)

        fs = self._slice_file.fs
        if isinstance(fs, fsspec.implementations.local.LocalFileSystem):
            return xr.open_dataset(self._slice_file.path, engine=engine)

        fo = fs.open(self._slice_file.path, "rb")
        return xr.open_dataset(fo, engine=engine)
