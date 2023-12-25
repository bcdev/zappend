# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import tempfile
from typing import Any, Dict

import xarray as xr

from .config import DEFAULT_SLICE_POLLING_INTERVAL
from .config import DEFAULT_SLICE_POLLING_TIMEOUT
from .config import DEFAULT_ZARR_VERSION
from .config import DEFAULT_APPEND_DIM
from .fsutil.fileobj import FileObj
from .log import logger
from .outline import DatasetOutline


class Context:
    """Provides access to configuration values and values derived from it."""

    def __init__(self, config: Dict[str, Any]):
        self._config = config

        target_uri = config.get("target_uri")
        if not target_uri:
            raise ValueError("Missing 'target_uri' in configuration")

        target_storage_options = config.get("target_storage_options")
        self._target_dir = FileObj(target_uri,
                                   storage_options=target_storage_options)

        try:
            with xr.open_zarr(
                    target_uri,
                    storage_options=target_storage_options,
                    decode_cf=False
            ) as target_ds:
                logger.info(f"Target dataset f{target_uri} found,"
                            " using its outline")
                self._target_outline = DatasetOutline.from_dataset(
                    target_ds
                )
        except FileNotFoundError:
            logger.info(f"Target dataset {target_uri} not found,"
                        " using outline from configuration")
            self._target_outline = DatasetOutline.from_config(self._config)

        temp_dir_uri = config.get("temp_dir", tempfile.gettempdir())
        temp_storage_options = config.get("temp_storage_options")
        self._temp_dir = FileObj(temp_dir_uri,
                                 storage_options=temp_storage_options)

    @property
    def zarr_version(self) -> int:
        return self._config.get("zarr_version", DEFAULT_ZARR_VERSION)

    @property
    def append_dim(self) -> str:
        return self._config.get("append_dim", DEFAULT_APPEND_DIM)

    @property
    def variables(self) -> dict[str, dict[str, Any]]:
        return self._config.get("variables", {})

    @property
    def target_outline(self) -> DatasetOutline:
        return self._target_outline

    @property
    def target_dir(self) -> FileObj:
        return self._target_dir

    @property
    def slice_engine(self) -> str | None:
        return self._config.get("slice_engine")

    @property
    def slice_storage_options(self) -> dict[str, Any] | None:
        return self._config.get("slice_storage_options")

    @property
    def slice_polling(self) -> tuple[float, float] | tuple[None, None]:
        """If slice polling is enabled, return tuple (interval, timeout)
        in seconds, otherwise, return (None, None).
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
    def temp_dir(self) -> FileObj:
        return self._temp_dir
