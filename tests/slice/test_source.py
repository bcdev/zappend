# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import pytest
import xarray as xr

from tests.helpers import clear_memory_fs
from zappend.context import Context
from zappend.fsutil.fileobj import FileObj
from zappend.slice.source import SliceSource, to_slice_source
from zappend.slice.sources.memory import MemorySliceSource
from zappend.slice.sources.persistent import PersistentSliceSource
from zappend.slice.sources.temporary import TemporarySliceSource


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
        slice_source = to_slice_source(ctx, ([13], {"arg2": True}), 0)
        self.assertIsInstance(slice_source, MemorySliceSource)
        ds = slice_source.get_dataset()
        self.assertEqual(13, ds.attrs.get("arg1"))
        self.assertEqual(True, ds.attrs.get("arg2"))
        self.assertIs(ctx, ds.attrs.get("ctx"))

    def test_slice_item_is_slice_source_context_manager(self):
        import contextlib

        @contextlib.contextmanager
        def my_slice_source(ctx, arg1, arg2=None):
            _ds = xr.Dataset(attrs=dict(arg1=arg1, arg2=arg2, ctx=ctx))
            try:
                yield _ds
            finally:
                _ds.close()

        ctx = make_ctx(slice_source=my_slice_source)
        slice_source = to_slice_source(ctx, ([14], {"arg2": "OK"}), 0)
        self.assertIsInstance(slice_source, contextlib.AbstractContextManager)
        with slice_source as ds:
            self.assertIsInstance(ds, xr.Dataset)
            self.assertEqual(14, ds.attrs.get("arg1"))
            self.assertEqual("OK", ds.attrs.get("arg2"))
            self.assertIs(ctx, ds.attrs.get("ctx"))

    # noinspection PyMethodMayBeStatic
    def test_raises_if_slice_item_is_int(self):
        ctx = make_ctx(persist_mem_slices=True)
        with pytest.raises(
            TypeError,
            match=(
                "slice_item must have type str, xarray.Dataset,"
                " contextlib.AbstractContextManager,"
                " zappend.api.FileObj, zappend.api.SliceSource,"
                " but was type int"
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
                " contextlib.AbstractContextManager,"
                " zappend.api.FileObj, zappend.api.SliceSource,"
                " but was type function"
            ),
        ):
            to_slice_source(ctx, hallo, 0)
