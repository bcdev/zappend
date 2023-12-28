# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import tempfile
from typing import Any, Dict, Callable

import dask.array
import numcodecs
import numpy as np
import xarray as xr

from .config import DEFAULT_APPEND_DIM
from .config import DEFAULT_SLICE_POLLING_INTERVAL
from .config import DEFAULT_SLICE_POLLING_TIMEOUT
from .config import DEFAULT_ZARR_VERSION
from .config import merge_configs
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

        fixed_dims = config.get("fixed_dims") or None
        variables = config.get("variables") or None
        append_dim = config.get("append_dim", DEFAULT_APPEND_DIM)
        try:
            with xr.open_zarr(
                target_uri,
                storage_options=target_storage_options,
                decode_cf=False
            ) as target_ds:
                logger.info(f"Target dataset f{target_uri} found")
                fixed_dims = get_effective_fixed_dims(fixed_dims, append_dim,
                                                      target_ds)
                variables = get_effective_variables(variables, target_ds)
        except FileNotFoundError:
            logger.info(f"Target dataset {target_uri} not found")
        self._fixed_dims: dict[str, int] | None = fixed_dims
        self._variables: dict[str, dict[str, Any]] | None = variables
        self._append_dim: str = append_dim

        temp_dir_uri = config.get("temp_dir", tempfile.gettempdir())
        temp_storage_options = config.get("temp_storage_options")
        self._temp_dir = FileObj(temp_dir_uri,
                                 storage_options=temp_storage_options)

    @property
    def zarr_version(self) -> int:
        return self._config.get("zarr_version", DEFAULT_ZARR_VERSION)

    @property
    def fixed_dims(self) -> dict[str, int] | None:
        return self._fixed_dims

    @property
    def append_dim(self) -> str:
        return self._append_dim

    @property
    def included_var_names(self) -> set[str]:
        return set(self._config.get("included_var_names", []))

    @property
    def excluded_var_names(self) -> set[str]:
        return set(self._config.get("excluded_var_names", []))

    @property
    def variables(self) -> dict[str, dict[str, Any]]:
        return self._config.get("variables", {})

    def strip_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        dataset_var_names = set(map(str, dataset.variables.keys()))
        included_var_names = self.included_var_names
        excluded_var_names = self.excluded_var_names
        if not included_var_names:
            included_var_names = dataset_var_names
        if excluded_var_names:
            included_var_names -= excluded_var_names
        drop_var_names = dataset_var_names - included_var_names
        return dataset.drop_vars(drop_var_names)

    def complete_dataset(self,
                         dataset: xr.Dataset,
                         variables: dict[str, dict[str, Any]]) -> xr.Dataset:
        for var_name, var_config in variables.items():
            var = dataset.variables.get(var_name)
            if var is None:
                logger.warning(
                    f"Variable {var_name!r} not found in slice dataset;"
                    f" creating it."
                )
                dims = var_config.get("dims")
                if not dims:
                    raise ValueError(f"Cannot create variable {var_name!r}"
                                     f" because its dimensions are unspecified")

                def get_dim_size(dim_name: str) -> int:
                    return dataset.dims[dim_name]

                try:
                    shape = tuple(map(get_dim_size, dims))
                except KeyError:
                    raise ValueError(f"Cannot create variable {var_name!r}"
                                     f" because at least one of its dimensions"
                                     f" {dims!r} does not exist in the"
                                     f" slice dataset")

                var_encoding = var_config.get("encoding", {})
                chunks = var_encoding.get("chunks")
                if chunks is None:
                    chunks = shape

                if ("fill_value" in var_encoding
                    and var_encoding["fill_value"] is not None):
                    memory_dtype = "float64"
                    memory_fill_value = float("NaN")
                else:
                    memory_dtype = var_encoding.get("dtype", "float32")
                    memory_fill_value = var_encoding.get("fill_value")
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
                    dims=dims
                )
                dataset[var_name] = var
        return dataset

    def configure_target_ds(self, dataset: xr.Dataset) -> xr.Dataset:
        dataset = self.strip_dataset(dataset)
        variables = get_effective_variables(self.variables, dataset)
        dataset = self.complete_dataset(dataset, variables)

        # Complement dataset attributes as well as
        # variable encoding and attributes
        dataset.attrs.update(self._config.get("attrs", {}))
        for var_name, var_config in variables.items():
            var = dataset.variables.get(var_name)
            # TODO: not correct:
            var_encoding = {k: normalize(var_config[k])
                            for k, normalize in _ENCODING_PROPS.items()
                            if k in var_config}
            var.encoding.update(var_encoding)
            var_attrs = var_config.get("attrs", {})
            var.attrs.update(var_attrs)
        return dataset

    def configure_slice_ds(self, dataset: xr.Dataset) -> xr.Dataset:
        dataset = self.strip_dataset(dataset)
        variables = get_effective_variables(self.variables, dataset)
        dataset = self.complete_dataset(dataset, variables)

        # Remove any encoding and attributes from slice,
        # since both are prescribed by target
        dataset.attrs.clear()
        for variable in dataset.variables.values():
            variable.encoding.clear()
            variable.attrs.clear()
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


def _normalize_dtype(dtype: Any) -> Any:
    if isinstance(dtype, str):
        return np.dtype(dtype)
    return dtype


def _normalize_fill_value(fill_value: Any) -> Any:
    if fill_value == "NaN":
        return float("NaN")
    return fill_value


def _normalize_number(scale_factor: Any) -> Any:
    return scale_factor


def _normalize_compressor(compressor: Any) -> Any:
    return _normalize_codec(compressor)


def _normalize_filters(filters: Any) -> Any:
    if not filters:
        return None
    return [_normalize_codec(f) for f in filters]


def _normalize_codec(codec: Any) -> Any:
    if isinstance(codec, dict):
        return numcodecs.get_codec(codec)
    return codec


_ENCODING_PROPS: dict[str, Callable[[Any], Any]] = {
    "dtype": _normalize_dtype,
    "fill_value": _normalize_fill_value,
    "scale_factor": _normalize_number,
    "add_offset": _normalize_number,
    "compressor": _normalize_compressor,
    "filters": _normalize_filters
}


def get_effective_fixed_dims(config_fixed_dims: dict[str, int] | None,
                             config_append_dim: str,
                             dataset: xr.Dataset) -> dict[str, int]:
    if config_append_dim not in dataset.dims:
        raise ValueError(f"Append dimension"
                         f" {config_append_dim!r} not found in dataset")
    ds_fixed_dims = {str(k): v
                     for k, v in dataset.dims.items()
                     if k != config_append_dim}
    if not config_fixed_dims:
        return ds_fixed_dims

    for k, v in config_fixed_dims.items():
        if k not in ds_fixed_dims:
            raise ValueError(f"Dimension {k!r}"
                             f" not found in dataset")
        v2 = ds_fixed_dims[k]
        if v != v2:
            raise ValueError(f"Illegal size of dimension {k!r},"
                             f" expected {v}, got {v2}")
    return config_fixed_dims


def get_effective_variables(config_variables: dict[str, dict[str, Any]] | None,
                            dataset: xr.Dataset) -> dict[str, dict[str, Any]]:
    ds_vars = {
        str(var_name): dict(
            dims=list(map(str, var.dims)),
            # TODO: normalize encoding, handle _FillValue, handle dask chunks
            encoding=dict(var.encoding),
            attrs=dict(var.attrs)
        )
        for var_name, var in dataset.variables.items()
    }
    if not config_variables:
        return ds_vars

    defaults = config_variables.get("*", {})
    config_variables = {k: merge_configs(defaults, v)
                        for k, v in config_variables.items()
                        if k != "*"}
    for var_name, ds_var in ds_vars.items():
        ds_dims = ds_var["dims"]
        config_var = config_variables.get(var_name)
        if config_var is None:
            config_variables[var_name] = ds_var
        else:
            config_dims = config_var.get("dims")
            if config_dims is None:
                config_var["dims"] = ds_dims
            elif config_dims != ds_dims:
                raise ValueError(f"Dimension mismatch for"
                                 f" variable {var_name!r},"
                                 f" expected {config_dims},"
                                 f" got {ds_dims}")
    return config_variables
