# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import tempfile
from typing import Any, Dict

import xarray as xr

from .config import DEFAULT_APPEND_DIM
from .config import DEFAULT_SLICE_POLLING_INTERVAL
from .config import DEFAULT_SLICE_POLLING_TIMEOUT
from .config import DEFAULT_ZARR_VERSION
from .fsutil.fileobj import FileObj
from .metadata import DatasetMetadata


class Context:
    """Provides access to configuration values and values derived from it.

    Args:
        config: A validated configuration.

    Raises:
        ValueError: If `target_dir` is missing in the configuration.
    """

    def __init__(self, config: Dict[str, Any]):
        self._config = config

        target_uri = config.get("target_dir")
        if not target_uri:
            raise ValueError("Missing 'target_dir' in configuration")

        target_storage_options = config.get("target_storage_options")
        self._target_dir = FileObj(target_uri, storage_options=target_storage_options)

        try:
            with xr.open_zarr(
                target_uri, storage_options=target_storage_options
            ) as target_dataset:
                target_metadata = DatasetMetadata.from_dataset(target_dataset, config)
        except FileNotFoundError:
            target_metadata = None

        self._target_metadata = target_metadata

        temp_dir_uri = config.get("temp_dir", tempfile.gettempdir())
        temp_storage_options = config.get("temp_storage_options")
        self._temp_dir = FileObj(temp_dir_uri, storage_options=temp_storage_options)

    def get_dataset_metadata(self, dataset: xr.Dataset) -> DatasetMetadata:
        """Get the dataset metadata from configuration and the given dataset.

        Args:
            dataset: The dataset

        Returns:
            The dataset metadata
        """
        return DatasetMetadata.from_dataset(dataset, self._config)

    @property
    def zarr_version(self) -> int:
        """The configured Zarr version for the target dataset."""
        return self._config.get("zarr_version", DEFAULT_ZARR_VERSION)

    @property
    def append_dim_name(self) -> str:
        """The configured append dimension."""
        return self._config.get("append_dim") or DEFAULT_APPEND_DIM

    @property
    def target_metadata(self) -> DatasetMetadata | None:
        """The metadata for the target dataset. May be `None` while the
        target dataset hasn't been created yet. Will be set, once the
        target dataset has been created from the first slice dataset."""
        return self._target_metadata

    @target_metadata.setter
    def target_metadata(self, value: DatasetMetadata):
        self._target_metadata = value

    @property
    def target_dir(self) -> FileObj:
        """The configured target directory."""
        return self._target_dir

    @property
    def slice_engine(self) -> str | None:
        """The configured slice engine to be used if a slice object is not a Zarr.
        If defined, it will be passed to the `xarray.open_dataset()` function.
        """
        return self._config.get("slice_engine")

    @property
    def slice_storage_options(self) -> dict[str, Any] | None:
        """The configured slice storage options to be used if a slice object is a Zarr."""
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
        return self._config.get("persist_mem_slices", False)

    @property
    def disable_rollback(self) -> bool:
        """Whether to disable transaction rollbacks."""
        return self._config.get("disable_rollback", False)

    @property
    def dry_run(self) -> bool:
        """Whether to run in dry mode."""
        return self._config.get("dry_run", False)
