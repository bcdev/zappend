# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import pytest
import xarray as xr

from zappend.context import Context
from zappend.fsutil.fileobj import FileObj
from zappend.slicesource.common import open_slice_source
from zappend.slicesource.identity import IdentitySliceSource
from zappend.slicesource.persistent import PersistentSliceSource
from .helpers import clear_memory_fs
from .helpers import make_test_dataset


class SliceSourceTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_in_memory(self):
        slice_dir = FileObj("memory://slice.zarr")
        dataset = make_test_dataset(uri=slice_dir.uri)
        ctx = Context(dict(target_dir="memory://target.zarr"))
        slice_zarr = open_slice_source(ctx, dataset)
        self.assertIsInstance(slice_zarr, IdentitySliceSource)
        with slice_zarr as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    def test_persistent_zarr(self):
        slice_dir = FileObj("memory://slice.zarr")
        make_test_dataset(uri=slice_dir.uri)
        ctx = Context(dict(target_dir="memory://target.zarr"))
        slice_zarr = open_slice_source(ctx, slice_dir.uri)
        self.assertIsInstance(slice_zarr, PersistentSliceSource)
        with slice_zarr as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    # def test_persistent_nc(self):
    #     slice_ds = make_test_dataset()
    #     slice_file = FileObj("memory:///slice.nc")
    #     with slice_file.fs.open(slice_file.path, "wb") as f:
    #         slice_ds.to_netcdf(f)
    #     ctx = Context(dict(target_dir="memory://target.zarr",
    #                        slice_engine="scipy"))
    #     slice_nc = open_slice_source(ctx, slice_file.uri)
    #     self.assertIsInstance(slice_nc, PersistentSliceSource)
    #     with slice_nc as slice_ds:
    #         self.assertIsInstance(slice_ds, xr.Dataset)

    def test_persistent_wait_success(self):
        slice_dir = FileObj("memory://slice.zarr")
        make_test_dataset(uri=slice_dir.uri)
        ctx = Context(
            dict(
                target_dir="memory://target.zarr",
                slice_polling=dict(timeout=0.1, interval=0.02),
            )
        )
        slice_zarr = open_slice_source(ctx, slice_dir.uri)
        self.assertIsInstance(slice_zarr, PersistentSliceSource)
        with slice_zarr as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    # noinspection PyMethodMayBeStatic
    def test_persistent_wait_fail(self):
        slice_dir = FileObj("memory://slice.zarr")
        ctx = Context(
            dict(
                target_dir="memory://target.zarr",
                slice_polling=dict(timeout=0.1, interval=0.02),
            )
        )
        slice_zarr = open_slice_source(ctx, slice_dir.uri)
        with pytest.raises(FileNotFoundError, match=slice_dir.uri):
            with slice_zarr:
                pass

    # noinspection PyMethodMayBeStatic
    def test_invalid_type(self):
        dataset_dir = FileObj("memory://slice.zarr")
        make_test_dataset(uri=dataset_dir.uri)
        ctx = Context(dict(target_dir="memory://target.zarr"))
        with pytest.raises(
            TypeError, match="slice_obj must be a str or xarray.Dataset"
        ):
            # noinspection PyTypeChecker
            open_slice_source(ctx, dataset_dir)
