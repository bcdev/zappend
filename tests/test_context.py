# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import xarray as xr
from zappend.api import zappend
from zappend.context import Context
from zappend.fsutil.fileobj import FileObj
from zappend.metadata import DatasetMetadata
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
