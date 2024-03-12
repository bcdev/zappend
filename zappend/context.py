# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Any, Dict

import xarray as xr

from .config import Config
from .metadata import DatasetMetadata


class Context:
    """Provides access to configuration values and values derived from it.

    Args:
        config: A validated configuration dictionary or a `Config` instance.

    Raises:
        ValueError: If `target_dir` is missing in the configuration.
    """

    def __init__(self, config: Dict[str, Any] | Config):
        _config: Config = config if isinstance(config, Config) else Config(config)
        last_append_label = None
        try:
            with xr.open_zarr(
                _config.target_dir.uri,
                storage_options=_config.target_dir.storage_options,
            ) as target_dataset:
                if _config.append_step is not None:
                    append_var = target_dataset.get(_config.append_dim)
                    if append_var is not None and append_var.size > 0:
                        last_append_label = append_var[-1]
                target_metadata = DatasetMetadata.from_dataset(target_dataset, _config)
        except FileNotFoundError:
            target_metadata = None

        self._config = _config
        self._last_append_label = last_append_label
        self._target_metadata = target_metadata

    @property
    def config(self) -> Config:
        """The processor configuration."""
        return self._config

    @property
    def last_append_label(self) -> Any | None:
        """The last label found in the coordinate variable that corresponds to
        the append dimension. Its value is `None` if no such variable exists or the
        variable is empty or if `config.append_step` is `None`.
        """
        return self._last_append_label

    @property
    def target_metadata(self) -> DatasetMetadata | None:
        """The metadata for the target dataset. May be `None` while the
        target dataset hasn't been created yet. Will be set, once the
        target dataset has been created from the first slice dataset."""
        return self._target_metadata

    @target_metadata.setter
    def target_metadata(self, value: DatasetMetadata):
        self._target_metadata = value

    def get_dataset_metadata(self, dataset: xr.Dataset) -> DatasetMetadata:
        """Get the dataset metadata from configuration and the given dataset.

        Args:
            dataset: The dataset

        Returns:
            The dataset metadata
        """
        return DatasetMetadata.from_dataset(dataset, self._config)
