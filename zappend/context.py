# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import tempfile
from typing import Any, Dict

import xarray as xr

from .config import DEFAULT_APPEND_DIM
from .config import DEFAULT_SLICE_POLLING_INTERVAL
from .config import DEFAULT_SLICE_POLLING_TIMEOUT
from .config import DEFAULT_ZARR_VERSION
from .metadata import get_effective_target_dims
from .metadata import get_effective_variables
from .fsutil.fileobj import FileObj
from .log import logger


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

        append_dim_name = config.get("append_dim") or DEFAULT_APPEND_DIM
        target_dim_sizes = dict(config.get("fixed_dims") or {})
        target_variables = dict(config.get("variables") or {})
        try:
            with xr.open_zarr(
                target_uri,
                storage_options=target_storage_options,
                decode_cf=False
            ) as target_ds:
                logger.info(f"Target dataset f{target_uri} found")
                target_dim_sizes = get_effective_target_dims(target_dim_sizes,
                                                             append_dim_name,
                                                             target_ds)
                target_variables = get_effective_variables(target_variables,
                                                           target_ds)
        except FileNotFoundError:
            logger.info(f"Target dataset {target_uri} not found")
        self._append_dim_name: str = append_dim_name
        self._target_dim_sizes: dict[str, int] | None = target_dim_sizes
        self._target_variables: dict[str, dict[str, Any]] = target_variables

        temp_dir_uri = config.get("temp_dir", tempfile.gettempdir())
        temp_storage_options = config.get("temp_storage_options")
        self._temp_dir = FileObj(temp_dir_uri,
                                 storage_options=temp_storage_options)

    @property
    def zarr_version(self) -> int:
        return self._config.get("zarr_version", DEFAULT_ZARR_VERSION)

    @property
    def append_dim_name(self) -> str:
        return self._append_dim_name

    @property
    def target_variables(self) -> dict[str, dict[str, Any]]:
        return self._target_variables

    @property
    def target_attrs(self) -> dict[str, Any]:
        return self._config.get("attrs") or {}

    @property
    def target_dim_sizes(self) -> dict[str, int]:
        return self._target_dim_sizes

    @property
    def included_var_names(self) -> set[str]:
        return set(self._config.get("included_var_names", []))

    @property
    def excluded_var_names(self) -> set[str]:
        return set(self._config.get("excluded_var_names", []))

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
    def temp_dir(self) -> FileObj:
        return self._temp_dir
