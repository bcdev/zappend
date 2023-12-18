# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import math
from typing import Any

import xarray as xr
from .config import DEFAULT_APPEND_DIM


ZARR_V2_DEFAULT_COMPRESSOR = {
    "id": "blosc",
    "cname": "lz4",
    "clevel": 5,
    "shuffle": 1,
    "blocksize": 0,
}


class DatasetSchema:
    def __init__(self,
                 dims: dict[str, int],
                 variables: dict[str, "VariableSchema"]):
        self.dims = dims
        self.variables = variables

    @classmethod
    def from_dataset(cls, ds: xr.Dataset) -> "DatasetSchema":
        return DatasetSchema(
            {str(k): v for k, v in ds.dims.items()},
            {str(k): VariableSchema.from_dataset(v)
             for k, v in ds.variables.items()}
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DatasetSchema":
        return DatasetSchema(
            dict(**config.get("fixed_dims", {}),
                 **{config.get("append_dim", "time"): -1}),
            {k: VariableSchema.from_config(v)
             for k, v in config.get("variables", {}).items()}
        )

    def get_noncompliance(self,
                          other: "DatasetSchema",
                          append_dim: str = DEFAULT_APPEND_DIM) -> list[str]:
        messages: list[str] = []
        if append_dim not in other.dims:
            messages.append(f"Append dimension {append_dim!r} not found")
        elif other.dims[append_dim] <= 0:
            messages.append(f"Non-positive size of"
                            f" append dimension {append_dim!r}:"
                            f" {other.dims[append_dim]}")
        for dim_name, dim_size in self.dims.items():
            if dim_name != append_dim:
                if dim_name not in self.dims:
                    messages.append(f"Missing dimension {dim_name!r}")
                elif self.dims[dim_name] != dim_size:
                    messages.append(f"Wrong size for dimension {dim_name!r},"
                                    f" expected {dim_size},"
                                    f" got {self.dims[dim_name]}")
        for var_name, var_schema in self.variables.items():
            if var_name not in other.variables:
                messages.append(f"Missing variable {var_name!r}")
            else:
                other_variable = other.variables[var_name]
                var_noncompliance = var_schema.get_noncompliance(
                    other_variable)
                if var_noncompliance:
                    messages.append(f"Non-compliant variable {var_name!r}:")
                    for m in var_noncompliance:
                        messages.append("  " + m)
        return messages


class VariableSchema:
    def __init__(self,
                 dtype: str,
                 dims: tuple[str, ...],
                 shape: tuple[int, ...],
                 chunks: tuple[int, ...],
                 fill_value: int | float | None = None,
                 scale_factor: int | float | None = None,
                 add_offset: int | float | None = None,
                 compressor: dict[str, Any] | None = None,
                 filters: tuple[Any, ...] | None = None):
        self.dtype = dtype
        self.dims = dims
        self.shape = shape
        self.chunks = chunks
        self.fill_value = fill_value
        self.scaling_factor = scale_factor
        self.add_offset = add_offset
        self.compressor = compressor
        self.filters = filters

    @classmethod
    def from_dataset(cls, var: xr.Variable) -> "VariableSchema":
        return VariableSchema(
            dtype=str(var.dtype),
            dims=_to_tuple(map(str, var.dims)),
            shape=_to_tuple(var.shape),
            chunks=_to_zarr_chunks(var.chunks, var.shape),
            fill_value=var.encoding.get("fill_value",
                                        var.attrs.get("_FillValue")),
            scale_factor=var.encoding.get("scale_factor",
                                          var.attrs.get("scale_factor")),
            add_offset=var.encoding.get("add_offset",
                                        var.attrs.get("add_offset")),
            compressor=_to_compressor_config(var.encoding.get("compressor")),
            filters=_to_filters_config(var.encoding.get("filters"))
        )

    @classmethod
    def from_config(cls, var: dict[str, Any]) -> "VariableSchema":
        dtype = var.get("dtype")
        return VariableSchema(
            dtype=dtype,
            dims=_to_tuple(var.get("dims")),
            shape=_to_tuple(var.get("shape")),
            chunks=_to_tuple(var.get("chunks", var.get("shape"))),
            fill_value=_to_fill_value(var.get("fill_value"), dtype),
            scale_factor=var.get("scale_factor"),
            add_offset=var.get("add_offset"),
            compressor=(_to_compressor_config(var.get("compressor"))
                        if "compressor" in var
                        else ZARR_V2_DEFAULT_COMPRESSOR),
            filters=_to_filters_config(var.get("filters"))
        )

    def get_noncompliance(self, other: "VariableSchema") -> list[str]:
        messages: list[str] = []
        for attr_name, v in self.__dict__.items():
            if attr_name.startswith("_"):
                continue
            this_v = _to_comparable_value(v)
            other_v = _to_comparable_value(getattr(other, attr_name))
            if this_v != other_v:
                messages.append(f"Incompatible {attr_name}, expected {v},"
                                f" got {other_v}")
        return messages


def _to_fill_value(fill_value: Any, dtype: str):
    if fill_value is None and dtype in ("float32", "float64"):
        return float("NaN")
    return fill_value


def _to_compressor_config(compressor: Any) -> dict[str, Any] | None:
    return _to_codec_config(compressor)


def _to_filters_config(filters: Any) -> list[dict[str, Any]] | None:
    if filters is None:
        return filters
    if _is_iterable(filters):
        return [_to_codec_config(f) for f in filters]
    raise TypeError()


def _to_codec_config(codec: Any) -> dict[str, Any] | None:
    if codec is None or codec is isinstance(codec, dict):
        return codec
    if hasattr(codec, "get_config"):
        return getattr(codec, "get_config")()
    raise TypeError()


def _to_zarr_chunks(xr_chunks: tuple[tuple[int, ...]] | None,
                    xr_shape: tuple[int, ...]) \
        -> tuple[int, ...] | None:
    if xr_chunks is None:
        return xr_shape
    zarr_chunks: list[int] = []
    for sizes in xr_chunks:
        first_size = sizes[0]
        zarr_compatible_chunking = True
        if len(sizes) > 1:
            last_size = sizes[-1]
            zarr_compatible_chunking = last_size <= first_size
            if zarr_compatible_chunking:
                zarr_compatible_chunking = all(size == first_size
                                               for size in sizes[1:-1])
        # append negative size as marker for varying,
        # hence invalid chunk size
        zarr_chunks.append(
            first_size if zarr_compatible_chunking else -first_size
        )
    return tuple(zarr_chunks)


def _to_tuple(v: Any) -> tuple[Any, ...] | None:
    if v is None or isinstance(v, tuple):
        return v
    if not isinstance(v, str) and _is_iterable(v):
        return tuple(v)
    return (v,)


def _is_iterable(value: Any) -> bool:
    try:
        iter(value)
        return True
    except TypeError:
        return False


def _to_comparable_value(value: Any):
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    return value
