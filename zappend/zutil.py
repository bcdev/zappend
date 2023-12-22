# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import itertools
import math
from typing import Iterator

import zarr
import zarr.convenience
import zarr.storage

from .fileobj import FileObj


def open_zarr_group(directory: FileObj) -> zarr.Group:
    return zarr.open_group(get_zarr_store(directory))


def get_zarr_store(directory: FileObj) -> zarr.storage.BaseStore:
    return zarr.storage.FSStore(directory.uri, fs=directory.fs)


def get_zarr_arrays_for_dim(target_group: zarr.Group,
                            append_dim: str) -> dict[str, (zarr.Array, int)]:
    result: dict[str, (zarr.Array, int)] = {}

    for array_name, _array in target_group.arrays():
        target_array: zarr.Array = _array

        target_dims = target_array.attrs.get("_ARRAY_DIMENSIONS")
        if target_dims is None:
            # Should actually not come here
            raise ValueError("Array array dimensions"
                             " for variable {var_name!r}")

        try:
            append_axis = target_dims.index(append_dim)
        except ValueError:
            # append dimension does not exist in variable,
            # so we cannot append data
            continue

        result[array_name] = (target_array, append_axis)

    return result


def get_chunk_update_range(size: int,
                           chunk_size: int,
                           append_size: int) -> tuple[bool, tuple[int, int]]:
    """Return the range of indexes of affected chunks if a
    given *size* with chunking *chunk_size* is extended by
    *append_size*. The first chunk may be updated or created,
    subsequent chunks would always need to be created.

    :param size: the size of the append dimension
    :param append_size: the size to be appended
    :param chunk_size: the chunk size of the append dimension
    :return: a tuple of the form
        (*first_is_update*, *chunk_index_range*).
    """
    start = size // chunk_size
    pixel = start * chunk_size
    first_is_update = pixel < size <= pixel + chunk_size
    end = math.ceil((size + append_size) / chunk_size)
    return first_is_update, (start, end)


def get_chunk_indices(shape: tuple[int, ...],
                      chunks: tuple[int, ...],
                      append_axis: int,
                      append_dim_range: tuple[int, int]) \
        -> Iterator[tuple[int, ...]]:
    dim_ranges = [range(0, math.ceil(s / c)) for s, c in zip(shape, chunks)]
    dim_ranges[append_axis] = range(*append_dim_range)
    return itertools.product(*dim_ranges)

