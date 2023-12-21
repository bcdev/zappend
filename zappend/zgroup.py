# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
import math

import zarr
import zarr.convenience
import zarr.storage

from .fileobj import FileObj

ZarrUpdate = tuple[
    int,  # append_axis
    list[tuple[int, ...]]  # chunks
]


def open_zarr_group(directory: FileObj) -> zarr.Group:
    return zarr.open_group(get_zarr_store(directory))


def get_zarr_store(directory: FileObj) -> zarr.storage.BaseStore:
    return zarr.storage.FSStore(directory.uri, fs=directory.fs)


def get_zarr_updates(target_group: zarr.Group,
                     slice_group: zarr.Group,
                     append_dim: str) -> dict[str, ZarrUpdate]:
    updates: dict[str, ZarrUpdate] = {}

    for var_name, value in target_group.arrays():
        target_array: zarr.Array = value

        target_dims = target_array.attrs.get("_ARRAY_DIMENSIONS")
        if target_dims is None:
            # Should actually not come here
            raise ValueError("Missing array dimensions"
                             " for variable {var_name!r}")

        try:
            append_axis = target_dims.index(append_dim)
        except ValueError:
            # append dimension does not exist in variable,
            # so we cannot append data
            continue

        if var_name not in slice_group or not hasattr(value, "shape"):
            raise ValueError(f"Variable {var_name!r} not found in slice")
        slice_array: zarr.Array = slice_group[var_name]

        slice_dims = slice_array.attrs.get("_ARRAY_DIMENSIONS")
        if target_dims != slice_dims:
            raise ValueError(f"Variable dimensions"
                             f" for {var_name!r} do not match:"
                             f" expected {target_dims},"
                             f" but got {slice_dims}")

        # TODO: compute new files and updated files and

        updates[var_name] = (append_axis, [])

    return updates


def get_chunks_update_range(size: int,
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
