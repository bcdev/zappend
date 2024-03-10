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
from zappend.slice.source import to_slice_source, SliceSource
from zappend.slice.sources.memory import MemorySliceSource
from zappend.slice.sources.persistent import PersistentSliceSource
from zappend.slice.sources.temporary import TemporarySliceSource
from tests.helpers import clear_memory_fs
from tests.helpers import make_test_dataset
from tests.config.test_config import CustomSliceSource


# noinspection PyUnusedLocal
def false_slice_source_function(ctx: Context, path: str):
    return 17


def make_ctx(**config):
    return Context(dict(target_dir="memory://target.zarr", **config))


# noinspection PyShadowingBuiltins
class ToSliceSourceTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_slice_item_is_slice_source(self):
        ctx = make_ctx()
        slice_item = MemorySliceSource(xr.Dataset(), 0)
        slice_source = to_slice_source(ctx, slice_item, 0)
        self.assertIs(slice_item, slice_source)

    def test_slice_item_is_str(self):
        ctx = make_ctx()
        slice_source = to_slice_source(ctx, "memory://slice-1.zarr", 0)
        self.assertIsInstance(slice_source, PersistentSliceSource)

    def test_slice_item_is_file_obj(self):
        ctx = make_ctx()
        slice_source = to_slice_source(ctx, FileObj("memory://slice-1.zarr"), 0)
        self.assertIsInstance(slice_source, PersistentSliceSource)

    def test_slice_item_is_dataset(self):
        ctx = make_ctx()
        slice_source = to_slice_source(ctx, xr.Dataset(), 0)
        self.assertIsInstance(slice_source, MemorySliceSource)

    def test_slice_item_is_persisted_dataset(self):
        ctx = make_ctx(persist_mem_slices=True)
        slice_source = to_slice_source(ctx, xr.Dataset(), 0)
        self.assertIsInstance(slice_source, TemporarySliceSource)

    def test_slice_item_is_dataset_with_slice_source_class(self):
        class MySliceSource(SliceSource):
            def __init__(self, ctx, arg):
                self.ctx = ctx
                self.arg = arg

            def get_dataset(self) -> xr.Dataset:
                return xr.Dataset()

            def dispose(self):
                pass

        ctx = make_ctx(slice_source=MySliceSource)
        arg = xr.Dataset()
        slice_source = to_slice_source(ctx, arg, 0)
        self.assertIsInstance(slice_source, MySliceSource)
        self.assertIs(ctx, slice_source.ctx)
        self.assertIs(arg, slice_source.arg)

    def test_slice_item_is_dataset_with_slice_source_function(self):
        def my_slice_source(arg1, arg2=None, ctx=None):
            return xr.Dataset(attrs=dict(arg1=arg1, arg2=arg2, ctx=ctx))

        ctx = make_ctx(slice_source=my_slice_source)
        arg = xr.Dataset()
        slice_source = to_slice_source(ctx, ([13], {"arg2": True}), 0)
        self.assertIsInstance(slice_source, MemorySliceSource)
        ds = slice_source.get_dataset()
        self.assertEqual(13, ds.attrs.get("arg1"))
        self.assertEqual(True, ds.attrs.get("arg2"))
        self.assertIs(ctx, ds.attrs.get("ctx"))

    # noinspection PyMethodMayBeStatic
    def test_raises_if_slice_item_is_int(self):
        ctx = make_ctx(persist_mem_slices=True)
        with pytest.raises(
            TypeError,
            match=(
                "slice_item must have type str, xarray.Dataset,"
                " zappend.api.FileObj, zappend.api.SliceSource, but was type int"
            ),
        ):
            to_slice_source(ctx, 42, 0)

    # noinspection PyMethodMayBeStatic
    def test_raises_if_slice_item_is_a_function(self):
        def hallo():
            pass

        ctx = make_ctx(persist_mem_slices=True)
        with pytest.raises(
            TypeError,
            match=(
                "slice_item must have type str, xarray.Dataset,"
                " zappend.api.FileObj, zappend.api.SliceSource, but was type function"
            ),
        ):
            to_slice_source(ctx, hallo, 0)


@unittest.skip(reason="Rall!")
class X(unittest.TestCase):
    def test_slice_source_is_class(self):
        ctx = make_ctx(
            slice_source="tests.config.test_config.CustomSliceSource",
        )
        slice_cm = open_slice_dataset(ctx, 7)
        self.assertIsInstance(slice_cm, SliceSourceContextManager)
        self.assertIsInstance(slice_cm.slice_source, CustomSliceSource)
        # noinspection PyUnresolvedReferences
        self.assertEqual(7, slice_cm.slice_source.index)

    def test_slice_source_is_func(self):
        ctx = Context(
            dict(
                target_dir="memory://target.zarr",
                slice_source="tests.config.test_config.new_custom_slice_source",
            )
        )
        slice_cm = open_slice_dataset(ctx, 13)
        self.assertIsInstance(slice_cm, SliceSourceContextManager)
        self.assertIsInstance(slice_cm.slice_source, CustomSliceSource)
        # noinspection PyUnresolvedReferences
        self.assertEqual(13, slice_cm.slice_source.index)

    # noinspection PyMethodMayBeStatic
    def test_slice_source_is_bad_func(self):
        ctx = Context(
            dict(
                target_dir="memory://target.zarr",
                slice_source="tests.slice.test_cm.false_slice_source_function",
            )
        )
        with pytest.raises(
            TypeError,
            match=(
                "slice_item must have type str, xarray.Dataset,"
                " zappend.api.FileObj, zappend.api.SliceSource,"
                " but was type int"
            ),
        ):
            open_slice_dataset(ctx, "test.nc")

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

    def test_persistent_wait_success(self):
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
    def test_it_raises_on_persistent_wait_fail(self):
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

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_invalid_type(self):
        ctx = Context(dict(target_dir="memory://target.zarr"))
        with pytest.raises(
            TypeError,
            match=(
                "slice_item must have type str, xarray.Dataset,"
                " zappend.api.FileObj, zappend.api.SliceSource,"
                " but was type int"
            ),
        ):
            # noinspection PyTypeChecker
            open_slice_dataset(ctx, 42)


# noinspection PyUnusedLocal
