# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import tempfile
from typing import Any, Dict

import fsspec
import xarray as xr

from .config import DEFAULT_SLICE_POLLING_INTERVAL
from .config import DEFAULT_SLICE_POLLING_TIMEOUT
from .config import DEFAULT_SLICE_ACCESS_MODE
from .config import DEFAULT_ZARR_VERSION
from .outline import DatasetOutline
from .log import logger


class Context:
    """Provides access to configuration values and values derived from it."""

    def __init__(self, target_path: str, config: Dict[str, Any]):
        self._config = config

        target_fs_options = config.get("target_fs_options", {})
        self._target_fs, self._target_path = fsspec.core.url_to_fs(
            target_path,
            **target_fs_options
        )

        try:
            with xr.open_zarr(
                    target_path,
                    storage_option=target_fs_options,
                    decode_cf=False
            ) as target_ds:
                logger.info(f"Target dataset f{target_path} found,"
                         " using its outline")
                self._target_outline = DatasetOutline.from_dataset(
                    target_ds
                )
        except FileNotFoundError:
            logger.info(f"Target dataset {target_path} not found,"
                     " using outline from configuration")
            self._target_outline = DatasetOutline.from_config(self._config)

        temp_path = config.get("temp_path", tempfile.gettempdir())
        temp_fs_options = config.get("temp_fs_options", {})
        self._temp_fs, self._temp_path = fsspec.core.url_to_fs(
            temp_path,
            **temp_fs_options
        )

    @property
    def zarr_version(self):
        return self._config.get("zarr_version", DEFAULT_ZARR_VERSION)

    @property
    def variables(self) -> dict[str, dict[str, Any]]:
        return self._config.get("variables", {})

    @property
    def target_outline(self) -> DatasetOutline:
        return self._target_outline

    @property
    def target_fs(self) -> fsspec.AbstractFileSystem:
        return self._target_path

    def target_fs_options(self) -> dict[str, Any]:
        return self._config.get("target_fs_options", {})

    @property
    def target_path(self) -> str:
        return self._target_path

    @property
    def slice_fs_options(self) -> dict[str, Any]:
        return self._config.get("slice_fs_options", {})

    def get_slice_fs(self, slice_path: str) \
            -> tuple[fsspec.AbstractFileSystem, str]:
        return fsspec.core.url_to_fs(
            slice_path,
            **self.slice_fs_options
        )

    @property
    def slice_polling(self) -> tuple[float, float] | tuple[None, None]:
        """If slice polling is enabled, return tuple (interval, timeout) in seconds,
        otherwise, return (None, None).
        """
        slice_polling = self._config.get("slice_polling", False)
        if slice_polling is False:
            return None, None
        if slice_polling is True:
            slice_polling = {}
        return (
            slice_polling.get("interval", DEFAULT_SLICE_POLLING_INTERVAL),
            slice_polling.get("timeout", DEFAULT_SLICE_POLLING_TIMEOUT)
        )

    @property
    def slice_access_mode(self) -> str:
        return self._config.get("slice_access_mode",
                                DEFAULT_SLICE_ACCESS_MODE)

    @property
    def temp_fs(self) -> fsspec.AbstractFileSystem:
        return self._temp_fs

    @property
    def temp_path(self) -> str:
        return self._temp_path
