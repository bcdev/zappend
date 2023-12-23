# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from contextlib import contextmanager
from typing import Any

import fsspec
import numpy as np
import xarray as xr

default_dims = ("time", "y", "x")
default_shape = (3, 50, 100)
default_chunks = (1, 30, 50)


def clear_memory_fs():
    fs = fsspec.filesystem("memory")
    fs.rm("/", recursive=True)


def make_test_config(
    dims: tuple[str, str, str] = default_dims,
    shape: tuple[int, int, int] = default_shape,
    chunks: tuple[int, int, int] = default_chunks
) -> dict[str, Any]:
    return dict(
        fixed_dims={dims[1]: shape[1], dims[2]: shape[2]},
        append_dim="time",
        variables={
            "chl": dict(
                dtype="uint16",
                dims=list(dims),
                shape=list(shape),
                chunks=list(chunks),
                scale_factor=0.2,
                add_offset=0,
                fill_value=9999
            ),
            "tsm": dict(
                dtype="int16",
                dims=list(dims),
                shape=list(shape),
                chunks=list(chunks),
                scale_factor=0.01,
                add_offset=-200,
                fill_value=-9999
            ),
            dims[0]: dict(
                dtype="uint64",
                dims=dims[0],
                shape=shape[0],
            ),
            dims[1]: dict(
                dtype="float64",
                dims=dims[1],
                shape=shape[1],
            ),
            dims[2]: dict(
                dtype="float64",
                dims=dims[2],
                shape=shape[2],
            ),
        }
    )


def make_test_dataset(
    dims: tuple[str, str, str] = default_dims,
    shape: tuple[int, int, int] = default_shape,
    chunks: tuple[int, int, int] = default_chunks,
    uri: str | None = None,
    storage_options: dict[str, Any] | None = None
) -> xr.Dataset:
    """Make a test dataset and return a xarray.Dataset instance.

    If *uri* is given, the dataset will be written to Zarr using optional
    *storage_options* and the dataset returned is the one reopened from that
    location using *storage_options* and ``decode_cf=False``.
    """
    ds = xr.Dataset(
        data_vars=dict(
            chl=xr.DataArray(np.zeros(shape, dtype="uint16"),
                             dims=dims,
                             attrs=dict(scale_factor=0.2,
                                        add_offset=0,
                                        _FillValue=9999)),
            tsm=xr.DataArray(np.zeros(shape, dtype="int16"),
                             dims=dims,
                             attrs=dict(scale_factor=0.01,
                                        add_offset=-200,
                                        _FillValue=-9999)),
        ),
        coords={
            dims[0]: xr.DataArray(np.arange(shape[0],
                                            dtype="uint64"),
                                  dims=dims[0]),
            dims[1]: xr.DataArray(np.linspace(0, 1, shape[1],
                                              dtype="float64"),
                                  dims=dims[1]),
            dims[2]: xr.DataArray(np.linspace(0, 1, shape[2],
                                              dtype="float64"),
                                  dims=dims[2]),
        }
    )

    ds = ds.chunk(dict(tuple(zip(dims, chunks))))

    if not uri:
        return ds

    fs, path = fsspec.core.url_to_fs(uri, **(storage_options or {}))
    if fs.exists(path):
        fs.rm(path, recursive=True)
    ds.to_zarr(uri,
               storage_options=storage_options,
               zarr_version=2,
               write_empty_chunks=False)
    return xr.open_zarr(uri,
                        storage_options=storage_options,
                        decode_cf=False)


@contextmanager
def file_tree(fs: fsspec.AbstractFileSystem, tree_data: dict, root: str = ""):
    try:
        yield create_tree(fs, tree_data, root=root)
    finally:
        delete_tree(fs, tree_data, root=root)


def create_tree(fs: fsspec.AbstractFileSystem,
                tree_data: dict,
                root: str = ""):
    if root and not fs.exists(root):
        fs.mkdir(root)
    for k, v in tree_data.items():
        path = f"{root}/k" if root else k
        if isinstance(v, dict):
            create_tree(fs, v, root=path)
        else:
            with fs.open(path, "w") as f:
                f.write(str(v))


def delete_tree(fs: fsspec.AbstractFileSystem,
                tree_data: dict,
                root: str = ""):
    if root and fs.exists(root):
        fs.rm(root, recursive=True)
        return
    for k, v in tree_data.items():
        path = f"{root}/k" if root else k
        if isinstance(v, dict):
            delete_tree(fs, v, root=path)
        elif fs.exists(path):
            fs.rm(path)
