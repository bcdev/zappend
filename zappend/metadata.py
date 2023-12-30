# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Any, Callable

import numcodecs
import numpy as np
import xarray as xr

from .config import merge_configs


def get_effective_target_dims(config_fixed_dims: dict[str, int] | None,
                              config_append_dim: str,
                              dataset: xr.Dataset) -> dict[str, int]:
    if config_fixed_dims:
        if config_fixed_dims.get(config_append_dim) is not None:
            raise ValueError(f"Size of append dimension"
                             f" {config_append_dim!r} must not be fixed")
        for dim_name, fixed_dim_size in config_fixed_dims.items():
            if dim_name not in dataset.dims:
                raise ValueError(f"Fixed dimension {dim_name!r}"
                                 f" not found in dataset")
            ds_dim_size = dataset.dims[dim_name]
            if fixed_dim_size != ds_dim_size:
                raise ValueError(f"Wrong size for fixed dimension {dim_name!r}"
                                 f" in dataset: expected {fixed_dim_size},"
                                 f" found {ds_dim_size}")
    if config_append_dim not in dataset.dims:
        raise ValueError(f"Append dimension"
                         f" {config_append_dim!r} not found in dataset")

    return {str(k): v for k, v in dataset.dims.items()}


# TODO: write test
def get_effective_variables(config_variables: dict[str, dict[str, Any]] | None,
                            dataset: xr.Dataset) -> dict[str, dict[str, Any]]:
    config_variables = config_variables or {}
    defaults = config_variables.get("*", {})
    config_variables = {k: merge_configs(defaults, v)
                        for k, v in config_variables.items()
                        if k != "*"}

    # Complement configured variables by dataset variables
    for var_name, variable in dataset.variables.items():
        var_name = str(var_name)
        ds_var_def = dict(dims=list(map(str, variable.dims)),
                          encoding=dict(variable.encoding),
                          attrs=dict(variable.attrs))
        config_var_def = config_variables.get(var_name)
        if config_var_def is None:
            config_var_def = ds_var_def
        else:
            ds_var_dims = ds_var_def.get("dims")
            config_var_dims = config_var_def.get("dims")
            if config_var_dims is not None and config_var_dims != ds_var_dims:
                raise ValueError(f"Dimension mismatch for"
                                 f" variable {var_name!r}:"
                                 f" expected {config_var_dims},"
                                 f" got {ds_var_dims}")
            config_var_def = merge_configs(ds_var_def, config_var_def)
        config_variables[var_name] = config_var_def

    # Normalize effective variables
    for var_name, config_var_def in config_variables.items():
        encoding = config_var_def.get("encoding") or {}
        attrs = config_var_def.get("attrs") or {}
        for prop_name, normalize_value in _ENCODING_PROPS.items():
            if prop_name in attrs:
                if prop_name not in encoding:
                    encoding[prop_name] = attrs.pop(prop_name)
            if prop_name in encoding:
                encoding[prop_name] = normalize_value(encoding[prop_name])
        config_var_def["encoding"] = encoding
        config_var_def["attrs"] = attrs

    return config_variables


def _normalize_dtype(dtype: Any) -> np.dtype | None:
    if isinstance(dtype, str):
        return np.dtype(dtype)
    return dtype


def _normalize_chunks(chunks: Any) -> tuple[int, ...] | None:
    if not chunks:
        return None
    return tuple((v if isinstance(v, int) else v[0])
                 for v in chunks)


def _normalize_number(value: Any) -> int | float | None:
    if isinstance(value, str):
        return float(value)
    return value


def _normalize_compressor(compressor: Any) -> Any | None:
    return _normalize_codec(compressor)


def _normalize_filters(filters: Any) -> list[Any] | None:
    if not filters:
        return None
    return [_normalize_codec(f) for f in filters]


def _normalize_codec(codec: Any) -> Any | None:
    if not codec:
        return None
    if isinstance(codec, dict):
        return numcodecs.get_codec(codec)
    return codec


_ENCODING_PROPS: dict[str, Callable[[Any], Any]] = {
    "dtype": _normalize_dtype,
    "chunks": _normalize_chunks,
    "fill_value": _normalize_number,
    "_FillValue": _normalize_number,
    "scale_factor": _normalize_number,
    "add_offset": _normalize_number,
    "compressor": _normalize_compressor,
    "filters": _normalize_filters
}
