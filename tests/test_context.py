# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import numpy as np

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
        self.assertIsInstance(ctx.config.target_dir, FileObj)
        self.assertEqual(target_dir, ctx.config.target_dir.uri)
        self.assertIsNone(ctx.target_metadata)

    def test_with_existing_target(self):
        target_dir = "memory://target.zarr"
        make_test_dataset(uri=target_dir)
        ctx = Context({"target_dir": target_dir})
        self.assertIsInstance(ctx.config.target_dir, FileObj)
        self.assertEqual(target_dir, ctx.config.target_dir.uri)
        self.assertIsInstance(ctx.target_metadata, DatasetMetadata)

    def test_last_append_label(self):
        make_test_dataset(uri="memory://target.zarr")
        ctx = Context({"target_dir": "memory://target.zarr"})
        self.assertEqual(None, ctx.last_append_label)
        ctx = Context({"target_dir": "memory://TARGET.zarr", "append_step": "1D"})
        self.assertEqual(None, ctx.last_append_label)
        ctx = Context({"target_dir": "memory://target.zarr", "append_step": "1D"})
        self.assertEqual(np.datetime64("2024-01-03"), ctx.last_append_label)
