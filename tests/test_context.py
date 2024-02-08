# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import pytest
import numpy as np
import xarray as xr

from zappend.context import Context
from zappend.fsutil.fileobj import FileObj
from zappend.metadata import DatasetMetadata
from zappend.slice import SliceSource
from .helpers import clear_memory_fs
from .helpers import make_test_dataset


class ContextTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_without_existing_target(self):
        target_dir = "memory://target.zarr"
        ctx = Context({"target_dir": target_dir})
        self.assertIsInstance(ctx.target_dir, FileObj)
        self.assertEqual(target_dir, ctx.target_dir.uri)
        self.assertIsNone(ctx.target_metadata)

    def test_with_existing_target(self):
        target_dir = "memory://target.zarr"
        make_test_dataset(uri=target_dir)
        ctx = Context({"target_dir": target_dir})
        self.assertIsInstance(ctx.target_dir, FileObj)
        self.assertEqual(target_dir, ctx.target_dir.uri)
        self.assertIsInstance(ctx.target_metadata, DatasetMetadata)

    def test_append_dim(self):
        ctx = Context({"target_dir": "memory://target.zarr"})
        self.assertEqual("time", ctx.append_dim_name)
        ctx = Context({"target_dir": "memory://target.zarr", "append_dim": "depth"})
        self.assertEqual("depth", ctx.append_dim_name)

    def test_append_step(self):
        make_test_dataset(uri="memory://target.zarr")
        ctx = Context({"target_dir": "memory://target.zarr"})
        self.assertEqual(None, ctx.append_step_size)
        ctx = Context({"target_dir": "memory://target.zarr", "append_step": "1D"})
        self.assertEqual("1D", ctx.append_step_size)

    def test_last_append_label(self):
        make_test_dataset(uri="memory://target.zarr")
        ctx = Context({"target_dir": "memory://target.zarr"})
        self.assertEqual(None, ctx.last_append_label)
        ctx = Context({"target_dir": "memory://TARGET.zarr", "append_step": "1D"})
        self.assertEqual(None, ctx.last_append_label)
        ctx = Context({"target_dir": "memory://target.zarr", "append_step": "1D"})
        self.assertEqual(np.datetime64("2024-01-03"), ctx.last_append_label)

    def test_slice_polling(self):
        ctx = Context({"target_dir": "memory://target.zarr"})
        self.assertEqual((None, None), ctx.slice_polling)
        ctx = Context({"target_dir": "memory://target.zarr", "slice_polling": False})
        self.assertEqual((None, None), ctx.slice_polling)
        ctx = Context({"target_dir": "memory://target.zarr", "slice_polling": True})
        self.assertEqual((2, 60), ctx.slice_polling)
        ctx = Context(
            {
                "target_dir": "memory://target.zarr",
                "slice_polling": {"interval": 1, "timeout": 10},
            }
        )
        self.assertEqual((1, 10), ctx.slice_polling)

    def test_temp_dir(self):
        ctx = Context({"target_dir": "memory://target.zarr"})
        self.assertIsInstance(ctx.temp_dir, FileObj)
        self.assertTrue(ctx.temp_dir.exists())

    def test_disable_rollback(self):
        ctx = Context({"target_dir": "memory://target.zarr"})
        self.assertFalse(ctx.disable_rollback)
        ctx = Context({"target_dir": "memory://target.zarr", "disable_rollback": True})
        self.assertTrue(ctx.disable_rollback)

    def test_dry_run(self):
        ctx = Context({"target_dir": "memory://target.zarr"})
        self.assertEqual(False, ctx.dry_run)
        ctx = Context({"target_dir": "memory://target.zarr", "dry_run": True})
        self.assertEqual(True, ctx.dry_run)

    def test_slice_source_as_name(self):
        ctx = Context(
            {
                "target_dir": "memory://target.zarr",
                "slice_source": "tests.test_context.new_custom_slice_source",
            }
        )
        self.assertEqual(new_custom_slice_source, ctx.slice_source)

        ctx = Context(
            {
                "target_dir": "memory://target.zarr",
                "slice_source": "tests.test_context.CustomSliceSource",
            }
        )
        self.assertEqual(CustomSliceSource, ctx.slice_source)

        # staticmethod
        ctx = Context(
            {
                "target_dir": "memory://target.zarr",
                "slice_source": "tests.test_context.CustomSliceSource.new1",
            }
        )
        self.assertEqual(CustomSliceSource.new1, ctx.slice_source)

        # classmethod
        ctx = Context(
            {
                "target_dir": "memory://target.zarr",
                "slice_source": "tests.test_context.CustomSliceSource.new2",
            }
        )
        self.assertEqual(CustomSliceSource.new2, ctx.slice_source)

    def test_slice_source_as_type(self):
        ctx = Context(
            {
                "target_dir": "memory://target.zarr",
                "slice_source": new_custom_slice_source,
            }
        )
        self.assertIs(new_custom_slice_source, ctx.slice_source)

        ctx = Context(
            {
                "target_dir": "memory://target.zarr",
                "slice_source": CustomSliceSource,
            }
        )
        self.assertIs(CustomSliceSource, ctx.slice_source)

        with pytest.raises(
            TypeError,
            match=(
                "slice_source must a callable"
                " or the fully qualified name of a callable"
            ),
        ):
            Context(
                {
                    "target_dir": "memory://target.zarr",
                    "slice_source": 11,
                }
            )


def new_custom_slice_source(ctx: Context, index: int):
    return CustomSliceSource(ctx, index)


class CustomSliceSource(SliceSource):
    def __init__(self, ctx: Context, index: int):
        super().__init__(ctx)
        self.index = index

    def get_dataset(self) -> xr.Dataset:
        return make_test_dataset(index=self.index)

    @staticmethod
    def new1(ctx: Context, index: int):
        return CustomSliceSource(ctx, index)

    @classmethod
    def new2(cls, ctx: Context, index: int):
        return cls(ctx, index)
