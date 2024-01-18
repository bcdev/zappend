# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import pytest
import xarray as xr

from zappend.context import Context
from zappend.fsutil.fileobj import FileObj
from zappend.slice.abc import SliceSource
from zappend.slice.factory import get_slice_dataset
from zappend.slice.factory import to_slice_factories

# noinspection PyProtectedMember
from zappend.slice.factory import _normalize_arg
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


class YieldSlicesTest(unittest.TestCase):
    ctx = Context({"target_dir": "data/test.zarr"})

    def test_function_without_ctx(self):
        def my_function(path: str):
            return "s3://" + path + ".nc"

        factories = list(
            to_slice_factories(my_function, [(["a"], {}), (["b"], {}), (["c"], {})])
        )
        self.assertEqual(3, len(factories))
        for factory in factories:
            self.assertTrue(callable(factory))
            self.assertTrue(hasattr(factory, "__closure__"))
        slices = [f(self.ctx) for f in factories]
        self.assertEqual("s3://a.nc", slices[0])
        self.assertEqual("s3://b.nc", slices[1])
        self.assertEqual("s3://c.nc", slices[2])

    def test_function_with_ctx_arg(self):
        def my_function(ctx: Context, path: str):
            return ctx.target_dir.parent / (path + ".nc")

        self._test_function_with_ctx(my_function)

    def test_function_with_ctx_kwarg(self):
        def my_function(path: str, ctx: Context = None):
            return ctx.target_dir.parent / (path + ".nc")

        self._test_function_with_ctx(my_function)

    def _test_function_with_ctx(self, my_function):
        factories = list(
            to_slice_factories(my_function, [(["a"], {}), (["b"], {}), (["c"], {})])
        )
        self.assertEqual(3, len(factories))
        for factory in factories:
            self.assertTrue(callable(factory))
            self.assertTrue(hasattr(factory, "__closure__"))
        slices = [f(self.ctx) for f in factories]
        self.assertEqual(FileObj("data/a.nc"), slices[0])
        self.assertEqual(FileObj("data/b.nc"), slices[1])
        self.assertEqual(FileObj("data/c.nc"), slices[2])

    def test_slice_source_class(self):
        class MyClass(SliceSource):
            def __init__(self, ctx: Context, path: str):
                super().__init__(ctx)
                self.path = "s3://" + path

            def get_dataset(self) -> xr.Dataset:
                return xr.open_dataset(self.path)

        factories = list(
            to_slice_factories(MyClass, [(["a"], {}), (["b"], {}), (["c"], {})])
        )
        self.assertEqual(3, len(factories))
        for factory in factories:
            self.assertTrue(callable(factory))
            self.assertTrue(hasattr(factory, "__closure__"))
        slices = [f(self.ctx) for f in factories]
        for s in slices:
            self.assertIsInstance(s, MyClass)
            self.assertIs(self.ctx, slices[0].ctx)
        self.assertEqual("s3://a", slices[0].path)
        self.assertEqual("s3://b", slices[1].path)
        self.assertEqual("s3://c", slices[2].path)

    # noinspection PyMethodMayBeStatic
    def test_not_callable(self):
        with pytest.raises(TypeError, match="13 is not a callable object"):
            # noinspection PyTypeChecker
            next(to_slice_factories(13, [([], {})]))

    def test_normalize_args_ok(self):
        # tuple
        self.assertEqual(((), {}), _normalize_arg(((), {})))
        self.assertEqual(((1, 2), {"c": 3}), _normalize_arg(([1, 2], {"c": 3})))

        # list
        self.assertEqual(((), {}), _normalize_arg([]))
        self.assertEqual(((1, 2, 3), {}), _normalize_arg([1, 2, 3]))

        # dict
        self.assertEqual(((), {}), _normalize_arg({}))
        self.assertEqual(((), {"c": 3}), _normalize_arg({"c": 3}))

        # other
        self.assertEqual(((1,), {}), _normalize_arg(1))
        self.assertEqual((("a",), {}), _normalize_arg("a"))

    # noinspection PyMethodMayBeStatic
    def test_normalize_args_fails(self):
        with pytest.raises(
            TypeError, match="tuple of form \\(args, kwargs\\) expected"
        ):
            _normalize_arg(((), (), ()))
        with pytest.raises(
            TypeError,
            match="args in tuple of form \\(args, kwargs\\) must be a tuple or list",
        ):
            _normalize_arg(({}, {}))
        with pytest.raises(
            TypeError, match="kwargs in tuple of form \\(args, kwargs\\) must be a dict"
        ):
            _normalize_arg(((), ()))
