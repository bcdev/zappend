# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import tempfile
from typing import Any, Dict

import dask.array
import numpy as np
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
    def target_dim_sizes(self) -> dict[str, int]:
        return self._target_dim_sizes

    @property
    def included_var_names(self) -> set[str]:
        return set(self._config.get("included_var_names", []))

    @property
    def excluded_var_names(self) -> set[str]:
        return set(self._config.get("excluded_var_names", []))

    # TODO: extract function and test
    def _strip_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        dataset_var_names = set(map(str, dataset.variables.keys()))
        included_var_names = self.included_var_names
        excluded_var_names = self.excluded_var_names
        if not included_var_names:
            included_var_names = dataset_var_names
        if excluded_var_names:
            included_var_names -= excluded_var_names
        drop_var_names = dataset_var_names - included_var_names
        return dataset.drop_vars(drop_var_names)

    # TODO: extract function and test
    def _complete_dataset(self,
                          dataset: xr.Dataset,
                          variables: dict[str, dict[str, Any]]) -> xr.Dataset:
        for var_name, var_config in variables.items():
            var = dataset.variables.get(var_name)
            if var is None:
                logger.warning(
                    f"Variable {var_name!r} not found in slice dataset;"
                    f" creating it."
                )
                var_dims = var_config["dims"]
                assert var_dims is not None

                def get_dim_size(dim_name: str) -> int:
                    return dataset.dims[dim_name]

                try:
                    shape = tuple(map(get_dim_size, var_dims))
                except KeyError:
                    raise ValueError(f"Cannot create variable {var_name!r}"
                                     f" because at least one of its dimensions"
                                     f" {var_dims!r} does not exist in the"
                                     f" slice dataset")

                var_encoding = var_config.get("encoding", {})
                chunks = var_encoding.get("chunks")
                if chunks is None:
                    chunks = shape

                if ("_FillValue" in var_encoding
                    and var_encoding["_FillValue"] is not None):
                    memory_dtype = "float64"
                    memory_fill_value = float("NaN")
                else:
                    memory_dtype = var_encoding.get("dtype", "float32")
                    memory_fill_value = var_encoding.get("_FillValue")
                    if memory_fill_value is None:
                        if memory_dtype in ("float32", "float64"):
                            memory_fill_value = float("NaN")
                        else:
                            memory_fill_value = 0
                var = xr.DataArray(
                    dask.array.full(shape,
                                    memory_fill_value,
                                    chunks=chunks,
                                    dtype=np.dtype(memory_dtype)),
                    dims=var_dims
                )
                dataset[var_name] = var
        return dataset

    def configure_target_ds(self, dataset: xr.Dataset) -> xr.Dataset:
        dataset = self._strip_dataset(dataset)
        variables = get_effective_variables(self.target_variables, dataset)
        dataset = self._complete_dataset(dataset, variables)

        # Complement dataset attributes and set
        # variable encoding and attributes
        dataset.attrs.update(self._config.get("attrs", {}))
        for var_name, var_config in variables.items():
            variable = dataset.variables[var_name]
            variable.encoding = var_config["encoding"]
            variable.attrs = var_config["attrs"]
        return dataset

    def configure_slice_ds(self, dataset: xr.Dataset) -> xr.Dataset:
        dataset = self._strip_dataset(dataset)
        variables = get_effective_variables(self.target_variables, dataset)
        dataset = self._complete_dataset(dataset, variables)

        # Remove any encoding and attributes from slice,
        # since both are prescribed by target
        dataset.attrs.clear()
        for variable in dataset.variables.values():
            variable.encoding = {}
            variable.attrs = {}
        return dataset

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
