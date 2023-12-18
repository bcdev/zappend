# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Any

import xarray as xr

from ..config import ZARR_V2_DEFAULT_COMPRESSOR
from ._helpers import to_tuple
from ._helpers import to_zarr_chunks
from ._helpers import to_compressor_config
from ._helpers import to_filters_config
from ._helpers import to_fill_value
from ._helpers import to_comparable_value


class VariableOutline:
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
    def from_dataset(cls, var: xr.Variable) -> "VariableOutline":
        return VariableOutline(
            dtype=str(var.dtype),
            dims=to_tuple(map(str, var.dims)),
            shape=to_tuple(var.shape),
            chunks=to_zarr_chunks(var.chunks, var.shape),
            fill_value=var.encoding.get("fill_value",
                                        var.attrs.get("_FillValue")),
            scale_factor=var.encoding.get("scale_factor",
                                          var.attrs.get("scale_factor")),
            add_offset=var.encoding.get("add_offset",
                                        var.attrs.get("add_offset")),
            compressor=to_compressor_config(var.encoding.get("compressor")),
            filters=to_filters_config(var.encoding.get("filters"))
        )

    @classmethod
    def from_config(cls, var: dict[str, Any]) -> "VariableOutline":
        dtype = var.get("dtype")
        return VariableOutline(
            dtype=dtype,
            dims=to_tuple(var.get("dims")),
            shape=to_tuple(var.get("shape")),
            chunks=to_tuple(var.get("chunks", var.get("shape"))),
            fill_value=to_fill_value(var.get("fill_value"), dtype),
            scale_factor=var.get("scale_factor"),
            add_offset=var.get("add_offset"),
            compressor=(to_compressor_config(var.get("compressor"))
                        if "compressor" in var
                        else ZARR_V2_DEFAULT_COMPRESSOR),
            filters=to_filters_config(var.get("filters"))
        )

    def get_noncompliance(self, other: "VariableOutline") -> list[str]:
        messages: list[str] = []
        for attr_name, v in self.__dict__.items():
            if attr_name.startswith("_"):
                continue
            this_v = to_comparable_value(v)
            other_v = to_comparable_value(getattr(other, attr_name))
            if this_v != other_v:
                messages.append(f"Incompatible {attr_name}, expected {v},"
                                f" got {other_v}")
        return messages
