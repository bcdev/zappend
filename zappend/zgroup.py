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


def get_chunk_actions(size: int,
                      append_size: int,
                      chunk_size: int) -> list[tuple[str, int]]:
    num_chunks = math.ceil((size + append_size) / chunk_size)
    actions = []
    for chunk_index in range(size // chunk_size, num_chunks):
        pixel_index = chunk_index * chunk_size
        if pixel_index < size <= pixel_index + chunk_size:
            actions.append(("update", chunk_index))
        else:
            actions.append(("create", chunk_index))
    return actions
