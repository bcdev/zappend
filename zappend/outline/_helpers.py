# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import math
from typing import Any


def to_fill_value(fill_value: Any, dtype: str):
    if fill_value is None and dtype in ("float32", "float64"):
        return float("NaN")
    return fill_value


def to_compressor_config(compressor: Any) -> dict[str, Any] | None:
    return to_codec_config(compressor)


def to_filters_config(filters: Any) -> list[dict[str, Any]] | None:
    if filters is None:
        return filters
    if is_iterable(filters):
        return [to_codec_config(f) for f in filters]
    raise TypeError()


def to_codec_config(codec: Any) -> dict[str, Any] | None:
    if codec is None or codec is isinstance(codec, dict):
        return codec
    if hasattr(codec, "get_config"):
        return getattr(codec, "get_config")()
    raise TypeError()


def to_zarr_chunks(xr_chunks: tuple[tuple[int, ...]] | None,
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


def to_tuple(v: Any) -> tuple[Any, ...] | None:
    if v is None or isinstance(v, tuple):
        return v
    if not isinstance(v, str) and is_iterable(v):
        return tuple(v)
    return (v,)


def is_iterable(value: Any) -> bool:
    try:
        iter(value)
        return True
    except TypeError:
        return False


def to_comparable_value(value: Any):
    if isinstance(value, float) and math.isnan(value):
        return "NaN"
    return value
