# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import pytest
import xarray as xr

from zappend.context import Context
from zappend.fsutil.fileobj import FileObj
from zappend.slice.common import get_slice_dataset
from zappend.slice.memory import MemorySliceSource
from zappend.slice.persistent import PersistentSliceSource
from zappend.slice.temporary import TemporarySliceSource
from .helpers import clear_memory_fs
from .helpers import make_test_dataset


class OpenSliceDatasetTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_slice_source_slice_source(self):
        dataset = make_test_dataset()
        ctx = Context(dict(target_dir="memory://target.zarr"))
        slice_obj = MemorySliceSource(ctx, dataset, 0)
        slice_source = get_slice_dataset(ctx, slice_obj)
        self.assertIs(slice_obj, slice_source)

    def test_factory_slice_source(self):
        dataset = make_test_dataset()
        ctx = Context(dict(target_dir="memory://target.zarr"))

        def factory(_ctx):
            self.assertIs(ctx, _ctx)
            return dataset

        slice_source = get_slice_dataset(ctx, factory)
        self.assertIsInstance(slice_source, MemorySliceSource)
        with slice_source as slice_ds:
            self.assertIs(dataset, slice_ds)

    def test_memory_slice_source(self):
        dataset = make_test_dataset()
        ctx = Context(dict(target_dir="memory://target.zarr"))
        slice_source = get_slice_dataset(ctx, dataset)
        self.assertIsInstance(slice_source, MemorySliceSource)
        with slice_source as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    def test_temporary_slice_source(self):
        dataset = make_test_dataset()
        ctx = Context(dict(target_dir="memory://target.zarr", persist_mem_slices=True))
        slice_source = get_slice_dataset(ctx, dataset)
        self.assertIsInstance(slice_source, TemporarySliceSource)
        with slice_source as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    def test_file_obj_slice_source(self):
        slice_dir = FileObj("memory://slice.zarr")
        make_test_dataset(uri=slice_dir.uri)
        ctx = Context(dict(target_dir="memory://target.zarr"))
        slice_source = get_slice_dataset(ctx, slice_dir)
        self.assertIsInstance(slice_source, PersistentSliceSource)
        with slice_source as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    def test_persistent_slice_source_for_zarr(self):
        slice_dir = FileObj("memory://slice.zarr")
        make_test_dataset(uri=slice_dir.uri)
        ctx = Context(dict(target_dir="memory://target.zarr"))
        slice_source = get_slice_dataset(ctx, slice_dir.uri)
        self.assertIsInstance(slice_source, PersistentSliceSource)
        with slice_source as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    # def test_persistent_slice_source_for_nc(self):
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
        slice_source = get_slice_dataset(ctx, slice_dir.uri)
        self.assertIsInstance(slice_source, PersistentSliceSource)
        with slice_source as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_persistent_wait_fail(self):
        slice_dir = FileObj("memory://slice.zarr")
        ctx = Context(
            dict(
                target_dir="memory://target.zarr",
                slice_polling=dict(timeout=0.1, interval=0.02),
            )
        )
        slice_source = get_slice_dataset(ctx, slice_dir.uri)
        with pytest.raises(FileNotFoundError, match=slice_dir.uri):
            with slice_source:
                pass

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_invalid_type(self):
        ctx = Context(dict(target_dir="memory://target.zarr"))
        with pytest.raises(TypeError, match="slice_obj must be a str, "):
            # noinspection PyTypeChecker
            get_slice_dataset(ctx, 42)