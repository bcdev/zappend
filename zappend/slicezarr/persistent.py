# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import time

import xarray as xr

from ..config import SLICE_ACCESS_MODE_SOURCE
from ..config import SLICE_ACCESS_MODE_SOURCE_SAFE
from ..context import Context
from .abc import SliceZarr
from .inmemory import InMemorySliceZarr
from ..outline import DatasetOutline
from ..log import logger


class PersistentSliceZarr(SliceZarr):
    """
    A slice Zarr that is persisted in some filesystem.

    :param ctx: Processing context
    :param slice_path: Path of the persistent slice
    """

    def __init__(self, ctx: Context, slice_path: str):
        super().__init__(ctx)
        self._slice_path = slice_path
        self._slice_fs, self._slice_fs_path = \
            self.ctx.get_slice_fs(self._slice_path)
        self._slice_ds: xr.Dataset | None = None
        self._slice_outline: DatasetOutline | None = None
        self._slice_zarr: SliceZarr | None = None

    def prepare(self):
        logger.info(f"Opening slice dataset {self._slice_path}")
        slice_ds: xr.Dataset | None = None
        interval, timeout = self.ctx.slice_polling
        if timeout is not None:
            t0 = time.monotonic()
            while (time.monotonic() - t0) < timeout:
                try:
                    slice_ds = self.open_dataset()
                except OSError:
                    time.sleep(interval)
        else:
            slice_ds = self.open_dataset()

        if not slice_ds:
            raise FileNotFoundError(self._slice_path)

        self._slice_outline = DatasetOutline.from_dataset(slice_ds)

        slice_access_mode = self.ctx.slice_access_mode
        if (slice_access_mode == SLICE_ACCESS_MODE_SOURCE
                or (slice_access_mode == SLICE_ACCESS_MODE_SOURCE_SAFE
                    and self.check_compliance())):
            slice_ds.close()  # No longer need this since we have the outline
            return self._slice_fs, self._slice_fs_path

        logger.info("Writing temporary slice")
        self._slice_ds = slice_ds  # Save instance so we can close it later
        self._slice_zarr = InMemorySliceZarr(self.ctx, slice_ds)
        return self._slice_zarr.prepare()

    def dispose(self):
        if hasattr(self, "slice_zarr") and self._slice_zarr is not None:
            self._slice_zarr.dispose()
            self._slice_zarr = None
            del self._slice_zarr
        if hasattr(self, "slice_ds") and self._slice_ds is not None:
            self._slice_ds.close()
            self._slice_ds = None
            del self._slice_ds
        super().dispose()

    def check_compliance(self) -> bool:
        messages = self.ctx.target_outline.get_noncompliance(
            self._slice_outline
        )
        if not messages:
            return True
        logger.warning(f"Incompatible slice dataset {self._slice_path}")
        for message in messages:
            logger.warning(message)
        return False

    def open_dataset(self) -> xr.Dataset:
        return xr.open_dataset(self._slice_path,
                               storage_options=self.ctx.slice_fs_options,
                               decode_cf=False)
