# Copyright © 2023 Norman Fomferra
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
        target_uri = "memory://target.zarr"
        ctx = Context({"target_uri": target_uri})
        self.assertIsInstance(ctx.target_dir, FileObj)
        self.assertEqual(target_uri, ctx.target_dir.uri)
        self.assertIsNone(ctx.target_metadata)

    def test_with_existing_target(self):
        target_uri = "memory://target.zarr"
        make_test_dataset(uri=target_uri)
        ctx = Context({"target_uri": target_uri})
        self.assertIsInstance(ctx.target_dir, FileObj)
        self.assertEqual(target_uri, ctx.target_dir.uri)
        self.assertIsInstance(ctx.target_metadata, DatasetMetadata)

    def test_append_dim(self):
        ctx = Context({"target_uri": "memory://target.zarr"})
        self.assertEqual("time", ctx.append_dim_name)

        ctx = Context({"target_uri": "memory://target.zarr",
                       "append_dim": "depth"})
        self.assertEqual("depth", ctx.append_dim_name)

    def test_slice_polling(self):
        ctx = Context({"target_uri": "memory://target.zarr"})
        self.assertEqual((None, None), ctx.slice_polling)

        ctx = Context({"target_uri": "memory://target.zarr",
                       "slice_polling": False})
        self.assertEqual((None, None), ctx.slice_polling)

        ctx = Context({"target_uri": "memory://target.zarr",
                       "slice_polling": True})
        self.assertEqual((2, 60), ctx.slice_polling)

        ctx = Context({"target_uri": "memory://target.zarr",
                       "slice_polling": {"interval": 1, "timeout": 10}})
        self.assertEqual((1, 10), ctx.slice_polling)

    def test_temp_dir(self):
        ctx = Context({"target_uri": "memory://target.zarr"})
        self.assertIsInstance(ctx.temp_dir, FileObj)
        self.assertTrue(ctx.temp_dir.exists())