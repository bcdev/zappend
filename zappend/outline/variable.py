# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Any

import xarray as xr


class VariableOutline:
    def __init__(self,
                 dtype: str,
                 dims: tuple[str, ...],
                 shape: tuple[int, ...]):
        self.dtype = dtype
        self.dims = dims
        self.shape = shape

    @classmethod
    def from_dataset(cls, var: xr.Variable) -> "VariableOutline":
        return VariableOutline(
            dtype=str(var.dtype),
            dims=_to_tuple(map(str, var.dims)),
            shape=_to_tuple(var.shape)
        )

    @classmethod
    def from_config(cls, var: dict[str, Any]) -> "VariableOutline":
        dtype = var.get("dtype")
        return VariableOutline(
            dtype=dtype,
            dims=_to_tuple(var.get("dims")),
            shape=_to_tuple(var.get("shape")),
        )

    def get_noncompliance(self, other: "VariableOutline") -> list[str]:
        messages: list[str] = []
        for attr_name in ("dtype", "dims", "shape"):
            this_v = getattr(self, attr_name)
            other_v = getattr(other, attr_name)
            if this_v != other_v:
                messages.append(f"Incompatible {attr_name}, expected {this_v},"
                                f" got {other_v}")
        return messages


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
