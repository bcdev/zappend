# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import tempfile
from typing import Any, Dict, Literal, Callable

from .defaults import DEFAULT_APPEND_DIM
from .defaults import DEFAULT_APPEND_STEP
from .defaults import DEFAULT_ATTRS_UPDATE_MODE
from .defaults import DEFAULT_SLICE_POLLING_INTERVAL
from .defaults import DEFAULT_SLICE_POLLING_TIMEOUT
from .defaults import DEFAULT_ZARR_VERSION
from ..fsutil.fileobj import FileObj


class Config:
    """Provides access to configuration values and values derived from it.

    Args:
        config_dict: A validated configuration dictionary.

    Raises:
        ValueError: If `target_dir` is missing in the configuration.
    """

    def __init__(self, config_dict: Dict[str, Any]):
        self._config = config_dict

        target_uri = config_dict.get("target_dir")
        if not target_uri:
            raise ValueError("Missing 'target_dir' in configuration")
        target_storage_options = config_dict.get("target_storage_options")
        self._target_dir = FileObj(target_uri, storage_options=target_storage_options)

        temp_dir_uri = config_dict.get("temp_dir", tempfile.gettempdir())
        temp_storage_options = config_dict.get("temp_storage_options")
        self._temp_dir = FileObj(temp_dir_uri, storage_options=temp_storage_options)

        # avoid cyclic import
        from ..slice.callable import to_slice_callable

        slice_source = config_dict.get("slice_source")
        self._slice_source = to_slice_callable(slice_source)

    @property
    def zarr_version(self) -> int:
        """The configured Zarr version for the target dataset."""
        return self._config.get("zarr_version", DEFAULT_ZARR_VERSION)

    @property
    def fixed_dims(self) -> dict[str, int] | None:
        return self._config.get("fixed_dims") or None

    @property
    def append_dim(self) -> str:
        """The name of the append dimension along which slice datasets will be
        concatenated. Defaults to `"time"`.
        """
        return self._config.get("append_dim") or DEFAULT_APPEND_DIM

    @property
    def append_step(self) -> int | float | str | None:
        """The enforced step size in the append dimension between two slices.
        Defaults to `None`.
        """
        return self._config.get("append_step") or DEFAULT_APPEND_STEP

    @property
    def included_variables(self) -> list[str]:
        """Names of included variables."""
        return self._config.get("included_variables") or []

    @property
    def excluded_variables(self) -> list[str]:
        """Names of excluded variables."""
        return self._config.get("excluded_variables") or []

    @property
    def variables(self) -> dict[str, Any]:
        """Variable definitions."""
        return self._config.get("variables") or {}

    @property
    def attrs(self) -> dict[str, Any]:
        """Global dataset attributes. May include dynamically computed
        placeholders if the form `{{ expression }}`.
        """
        return self._config.get("attrs") or {}

    @property
    def attrs_update_mode(
        self,
    ) -> Literal["keep"] | Literal["replace"] | Literal["update"]:
        """The mode used to deal with global slice dataset attributes.
        One of `"keep"`, `"replace"`, `"update"`.
        """
        return self._config.get("attrs_update_mode") or DEFAULT_ATTRS_UPDATE_MODE

    @property
    def permit_eval(self) -> bool:
        """Check if dynamically computed values in dataset attributes `attrs`
        using the syntax `{{ expression }}` is permitted. Executing arbitrary
        Python expressions is a security risk, therefore this must be explicitly
        enabled.
        """
        return bool(self._config.get("permit_eval"))

    @property
    def target_dir(self) -> FileObj:
        """The configured directory that represents the target datacube
        in Zarr format."""
        return self._target_dir

    @property
    def slice_engine(self) -> str | None:
        """The configured slice engine to be used if a slice path or URI does not
        point to a dataset in Zarr format.
        If defined, it will be passed to the `xarray.open_dataset()` function.
        """
        return self._config.get("slice_engine")

    @property
    def slice_source(self) -> Callable[[...], Any] | None:
        """The configured slice source type. If given, it must be
        a callable that returns a value of type `SliceItem` or a class that is
        derived from `SliceSource` abstract base class.
        """
        return self._slice_source

    @property
    def slice_storage_options(self) -> dict[str, Any] | None:
        """The configured slice storage options to be used
        if a slice item is a URI.
        """
        return self._config.get("slice_storage_options")

    @property
    def slice_polling(self) -> tuple[float, float] | tuple[None, None]:
        """The configured slice dataset polling.
        If slice polling is enabled, returns tuple (interval, timeout)
        in seconds, otherwise, return (None, None).
        """
        slice_polling = self._config.get("slice_polling", False)
        if slice_polling is False:
            return None, None
        if slice_polling is True:
            slice_polling = {}
        return (
            slice_polling.get("interval", DEFAULT_SLICE_POLLING_INTERVAL),
            slice_polling.get("timeout", DEFAULT_SLICE_POLLING_TIMEOUT),
        )

    @property
    def temp_dir(self) -> FileObj:
        """The configured directory used for temporary files such as rollback data."""
        return self._temp_dir

    @property
    def persist_mem_slices(self) -> bool:
        """Whether to persist in-memory slice datasets."""
        return bool(self._config.get("persist_mem_slices"))

    @property
    def force_new(self) -> bool:
        """If set, an existing target dataset will be deleted."""
        return bool(self._config.get("force_new"))

    @property
    def disable_rollback(self) -> bool:
        """Whether to disable transaction rollbacks."""
        return bool(self._config.get("disable_rollback"))

    @property
    def dry_run(self) -> bool:
        """Whether to run in dry mode."""
        return bool(self._config.get("dry_run"))

    @property
    def logging(self) -> dict[str, Any] | str | bool | None:
        """Logging configuration."""
        return self._config.get("logging")

    @property
    def profiling(self) -> dict[str, Any] | str | bool | None:
        """Profiling configuration."""
        return self._config.get("profiling")
