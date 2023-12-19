# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import fsspec
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

        fs = get_memory_fs()

        undo_manager = UndoManager()
        make_dirs(fs, "a", undo_op_cb=undo_manager.add_undo_op)
        self.assertEquals([('delete_dir', 'a')], undo_manager.ops)

        undo_manager.reset()
        make_dirs(fs, "a/b", undo_op_cb=undo_manager.add_undo_op)
        self.assertEquals([('delete_dir', 'a/b')], undo_manager.ops)

        undo_manager.reset()
        make_dirs(fs, "a/b/c", undo_op_cb=undo_manager.add_undo_op)
        self.assertEquals([('delete_dir', 'a/b/c')], undo_manager.ops)

        undo_manager.reset()
        make_dirs(fs, "a/b/c", undo_op_cb=undo_manager.add_undo_op)
        self.assertEquals([], undo_manager.ops)

        undo_manager.reset()
        make_dirs(fs, "c/a/b", undo_op_cb=undo_manager.add_undo_op)
        self.assertEquals([('delete_dir', 'c')], undo_manager.ops)

        fs.rm("/a", recursive=True)
        fs.rm("/c", recursive=True)


class CopyDirTest(unittest.TestCase):

    def test_copy_dir(self):
        undo_manager = UndoManager()

        protocol = "memory"
        fs: fsspec.AbstractFileSystem = fsspec.filesystem(protocol)

        source_path = "source.zarr"
        target_path = "target.zarr"
        if fs.exists(source_path):
            fs.rm(source_path, recursive=True)
        if fs.exists(target_path):
            fs.rm(target_path, recursive=True)

        source_ds = make_test_dataset(uri=f"{protocol}://{source_path}")

        copy_dir(fs, source_path,
                 fs, target_path,
                 undo_op_cb=undo_manager.add_undo_op)

        self.assertEqual([('delete_dir', 'target.zarr')],
                         undo_manager.ops)

        fs.rm(target_path, recursive=True)
        fs.mkdir(target_path)
        undo_manager.reset()
        copy_dir(fs, source_path,
                 fs, target_path,
                 undo_op_cb=undo_manager.add_undo_op)

        self.assertEqual(
            {
                ('delete_file', 'target.zarr/.zmetadata'),
                ('delete_file', 'target.zarr/.zgroup'),
                ('delete_file', 'target.zarr/.zattrs'),
                ('delete_dir', 'target.zarr/chl'),
                ('delete_dir', 'target.zarr/tsm'),
                ('delete_dir', 'target.zarr/x'),
                ('delete_dir', 'target.zarr/y'),
                ('delete_dir', 'target.zarr/time'),
            },
            set(undo_manager.ops)
        )

        undo_manager.reset()
        copy_dir(fs, source_path,
                 fs, target_path,
                 undo_op_cb=undo_manager.add_undo_op)

        self.assertNotIn(('delete_dir', 'target.zarr'), undo_manager.ops)
        self.assertIn(('replace_file', 'target.zarr/.zmetadata'),
                      undo_manager.ops)
        self.assertIn(('replace_file', 'target.zarr/.zgroup'),
                      undo_manager.ops)
        self.assertIn(('replace_file', 'target.zarr/.zattrs'),
                      undo_manager.ops)
        self.assertIn(('replace_file', 'target.zarr/y/.zarray'),
                      undo_manager.ops)
        self.assertIn(('replace_file', 'target.zarr/y/.zattrs'),
                      undo_manager.ops)
        self.assertIn(('replace_file', 'target.zarr/y/0'),
                      undo_manager.ops)

        target_ds = xr.open_zarr(f"{protocol}://{target_path}",
                                 decode_cf=False)

        self.assertEqual(source_ds.dims, target_ds.dims)
        self.assertEqual(set(source_ds.variables.keys()),
                         set(target_ds.variables.keys()))

        fs.rm(source_path, recursive=True)
        fs.rm(target_path, recursive=True)


class UndoManager:
    def __init__(self):
        self.ops = []

    # noinspection PyUnusedLocal
    def add_undo_op(self, op: str, path: str, data: bytes | None):
        self.ops.append((op, path))

    def reset(self):
        self.ops = []
