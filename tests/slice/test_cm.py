# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import shutil
import unittest
import warnings

import pytest
import xarray as xr

from zappend.context import Context
from zappend.fsutil.fileobj import FileObj
from zappend.slice.cm import SliceSourceContextManager
from zappend.slice.cm import open_slice_dataset
from zappend.slice.sources.memory import MemorySliceSource
from zappend.slice.sources.persistent import PersistentSliceSource
from zappend.slice.sources.temporary import TemporarySliceSource
from tests.helpers import clear_memory_fs
from tests.helpers import make_test_dataset


# noinspection PyUnusedLocal


# noinspection PyShadowingBuiltins
class OpenSliceDatasetTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    # noinspection PyMethodMayBeStatic
    def test_slice_item_is_slice_source(self):
        dataset = make_test_dataset()
        ctx = Context(dict(target_dir="memory://target.zarr"))
        slice_item = MemorySliceSource(dataset, 0)
        slice_cm = open_slice_dataset(ctx, slice_item)
        self.assertIs(slice_item, slice_cm.slice_source)

    def test_slice_item_is_dataset(self):
        dataset = make_test_dataset()
        ctx = Context(dict(target_dir="memory://target.zarr"))
        slice_cm = open_slice_dataset(ctx, dataset)
        self.assertIsInstance(slice_cm, SliceSourceContextManager)
        self.assertIsInstance(slice_cm.slice_source, MemorySliceSource)
        with slice_cm as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    def test_slice_item_is_persisted_dataset(self):
        dataset = make_test_dataset()
        ctx = Context(dict(target_dir="memory://target.zarr", persist_mem_slices=True))
        slice_cm = open_slice_dataset(ctx, dataset)
        self.assertIsInstance(slice_cm, SliceSourceContextManager)
        self.assertIsInstance(slice_cm.slice_source, TemporarySliceSource)
        with slice_cm as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    def test_slice_item_is_file_obj(self):
        slice_dir = FileObj("memory://slice.zarr")
        make_test_dataset(uri=slice_dir.uri)
        ctx = Context(dict(target_dir="memory://target.zarr"))
        slice_cm = open_slice_dataset(ctx, slice_dir)
        self.assertIsInstance(slice_cm, SliceSourceContextManager)
        with slice_cm as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    def test_slice_item_is_memory_uri(self):
        slice_dir = FileObj("memory://slice.zarr")
        make_test_dataset(uri=slice_dir.uri)
        ctx = Context(dict(target_dir="memory://target.zarr"))
        slice_cm = open_slice_dataset(ctx, slice_dir.uri)
        self.assertIsInstance(slice_cm, SliceSourceContextManager)
        with slice_cm as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    def test_slice_item_is_uri_of_memory_nc(self):
        engine = "scipy"
        format = "NETCDF3_CLASSIC"
        slice_ds = make_test_dataset()
        slice_file = FileObj("memory:///slice.nc")
        with slice_file.fs.open(slice_file.path, "wb") as stream:
            # noinspection PyTypeChecker
            slice_ds.to_netcdf(stream, engine=engine, format=format)
        ctx = Context(dict(target_dir="memory://target.zarr", slice_engine=engine))
        slice_cm = open_slice_dataset(ctx, slice_file.uri)
        self.assertIsInstance(slice_cm, SliceSourceContextManager)
        self.assertIsInstance(slice_cm.slice_source, PersistentSliceSource)
        try:
            with slice_cm as slice_ds:
                self.assertIsInstance(slice_ds, xr.Dataset)
        except KeyError as e:
            # This is unexpected! xarray cannot open the NetCDF file it just
            # created. Maybe report a xarray issue once we can isolate the
            # root cause. But it may be related to just the memory FS.
            warnings.warn(f"received known exception from to_netcdf(): {e}")

    def test_slice_item_is_uri_of_local_fs_nc(self):
        engine = "h5netcdf"
        format = "NETCDF4"
        target_dir = FileObj("./target.zarr")
        ctx = Context(dict(target_dir=target_dir.path, slice_engine=engine))
        slice_ds = make_test_dataset()
        slice_file = FileObj("./slice.nc")
        # noinspection PyTypeChecker
        slice_ds.to_netcdf(slice_file.path, engine=engine, format=format)
        try:
            slice_cm = open_slice_dataset(ctx, slice_file.uri)
            self.assertIsInstance(slice_cm, SliceSourceContextManager)
            self.assertIsInstance(slice_cm.slice_source, PersistentSliceSource)
            with slice_cm as slice_ds:
                self.assertIsInstance(slice_ds, xr.Dataset)
        finally:
            shutil.rmtree(target_dir.path, ignore_errors=True)
            slice_file.delete()

    def test_slice_item_is_uri_with_polling_ok(self):
        slice_dir = FileObj("memory://slice.zarr")
        make_test_dataset(uri=slice_dir.uri)
        ctx = Context(
            dict(
                target_dir="memory://target.zarr",
                slice_polling=dict(timeout=0.1, interval=0.02),
            )
        )
        slice_cm = open_slice_dataset(ctx, slice_dir.uri)
        self.assertIsInstance(slice_cm, SliceSourceContextManager)
        self.assertIsInstance(slice_cm.slice_source, PersistentSliceSource)
        with slice_cm as slice_ds:
            self.assertIsInstance(slice_ds, xr.Dataset)

    # noinspection PyMethodMayBeStatic
    def test_slice_item_is_uri_with_polling_fail(self):
        slice_dir = FileObj("memory://slice.zarr")
        ctx = Context(
            dict(
                target_dir="memory://target.zarr",
                slice_polling=dict(timeout=0.1, interval=0.02),
            )
        )
        slice_cm = open_slice_dataset(ctx, slice_dir.uri)
        with pytest.raises(FileNotFoundError, match=slice_dir.uri):
            with slice_cm:
                pass
