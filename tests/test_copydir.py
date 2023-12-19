# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import xarray as xr

from zappend.copydir import copy_dir
from zappend.copydir import split_path
from zappend.copydir import make_dirs
from .helpers import get_memory_fs
from .helpers import make_test_dataset


class SplitPathTest(unittest.TestCase):
    def test_split_path_0c(self):
        self.assertEqual([""], split_path(""))

    def test_split_path_1c(self):
        self.assertEqual(["/"], split_path("/"))

        self.assertEqual(["a"], split_path("a"))
        self.assertEqual(["/a"], split_path("/a"))
        self.assertEqual(["a/"], split_path("a/"))
        self.assertEqual(["/a/"], split_path("/a/"))

    def test_split_path_2c(self):
        self.assertEqual(["a", "b"], split_path("a/b"))
        self.assertEqual(["/a", "b"], split_path("/a/b"))
        self.assertEqual(["a", "b/"], split_path("a/b/"))
        self.assertEqual(["/a", "b/"], split_path("/a/b/"))

    def test_split_path_3c(self):
        self.assertEqual(["a", "b", "c"], split_path("a/b/c"))
        self.assertEqual(["/a", "b", "c"], split_path("/a/b/c"))
        self.assertEqual(["a", "b", "c/"], split_path("a/b/c/"))
        self.assertEqual(["/a", "b", "c/"], split_path("/a/b/c/"))


class MakeDirsTest(unittest.TestCase):

    def test_make_dirs(self):
        fs = get_memory_fs()

        self.assertEquals(1, make_dirs(fs, "a"))
        self.assertTrue(1, fs.isdir("a"))

        self.assertEquals(1, make_dirs(fs, "a/b"))
        self.assertTrue(1, fs.isdir("a/b"))

        self.assertEquals(1, make_dirs(fs, "a/b/c"))
        self.assertTrue(1, fs.isdir("a/b/c"))

        self.assertEquals(0, make_dirs(fs, "a/b/c"))
        self.assertTrue(1, fs.isdir("a/b/c"))

        self.assertEquals(3, make_dirs(fs, "c/a/b"))
        self.assertTrue(1, fs.isdir("c/a/b"))

        fs.rm("/a", recursive=True)
        fs.rm("/c", recursive=True)

    def test_make_dirs_with_cb(self):
        ops = []

        def cb(op: str, path: str):
            ops.append((op, path))

        fs = get_memory_fs()

        make_dirs(fs, "a", file_op_cb=cb)
        self.assertEquals([('create_dir', 'a')], ops)

        ops = []
        make_dirs(fs, "a/b", file_op_cb=cb)
        self.assertEquals([('create_dir', 'a/b')], ops)

        ops = []
        make_dirs(fs, "a/b/c", file_op_cb=cb)
        self.assertEquals([('create_dir', 'a/b/c')], ops)

        ops = []
        make_dirs(fs, "a/b/c", file_op_cb=cb)
        self.assertEquals([], ops)

        ops = []
        make_dirs(fs, "c/a/b", file_op_cb=cb)
        self.assertEquals([('create_dir', 'c')], ops)

        fs.rm("/a", recursive=True)
        fs.rm("/c", recursive=True)


class CopyDirTest(unittest.TestCase):
    def test_copy_dir(self):
        ops = []

        def callback(op: str, path: str):
            ops.append((op, path))

        source_dataset = make_test_dataset(uri="memory://source.zarr")
        self.assertIsInstance(source_dataset, xr.Dataset)

        fs = get_memory_fs()
        copy_dir(fs, "source.zarr",
                 fs, "target.zarr",
                 file_op_cb=callback)

        self.assertEqual([], ops)

        target_dataset = xr.open_zarr("memory://target.zarr",
                                      decode_cf=False)
        self.assertIsInstance(target_dataset, xr.Dataset)
