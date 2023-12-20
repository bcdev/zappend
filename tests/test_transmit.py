# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import xarray as xr

from zappend.transmit import transmit
from zappend.transmit import split_path
from zappend.transmit import make_dirs
from zappend.fileobj import FileObj
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
        root = FileObj("memory:///")
        fs = root.fs

        self.assertEqual(1, make_dirs(root / "a"))
        self.assertTrue(1, fs.isdir("a"))

        self.assertEqual(1, make_dirs(root / "a" / "b"))
        self.assertTrue(1, fs.isdir("a/b"))

        self.assertEqual(1, make_dirs(root / "a" / "b" / "c"))
        self.assertTrue(1, fs.isdir("a/b/c"))

        self.assertEqual(0, make_dirs(root / "a" / "b" / "c"))
        self.assertTrue(1, fs.isdir("a/b/c"))

        self.assertEqual(3, make_dirs(root / "c" / "a" / "b"))
        self.assertTrue(1, fs.isdir("c/a/b"))

        root.delete(recursive=True)

    def test_make_dirs_with_rollback_cb(self):
        root = FileObj("memory:///")

        undo_manager = UndoManager()
        make_dirs(root / "a",
                  rollback_cb=undo_manager.add_undo_op)
        self.assertEqual([('delete_dir', '/a')], undo_manager.ops)

        undo_manager.reset()
        make_dirs(root / "a" / "b",
                  rollback_cb=undo_manager.add_undo_op)
        self.assertEqual([('delete_dir', '/a/b')], undo_manager.ops)

        undo_manager.reset()
        make_dirs(root / "a" / "b" / "c",
                  rollback_cb=undo_manager.add_undo_op)
        self.assertEqual([('delete_dir', '/a/b/c')], undo_manager.ops)

        undo_manager.reset()
        make_dirs(root / "a" / "b" / "c",
                  rollback_cb=undo_manager.add_undo_op)
        self.assertEqual([], undo_manager.ops)

        undo_manager.reset()
        make_dirs(root / "c" / "a" / "b",
                  rollback_cb=undo_manager.add_undo_op)
        self.assertEqual([('delete_dir', '/c')], undo_manager.ops)

        root.delete(recursive=True)


class TransmitTest(unittest.TestCase):

    def test_transmit_with_rollback_cb(self):
        undo_manager = UndoManager()

        protocol = "memory"
        source_dir = FileObj(f"{protocol}://source.zarr")
        target_dir = FileObj(f"{protocol}://target.zarr")
        if source_dir.exists():
            source_dir.delete(recursive=True)
        if target_dir.exists():
            target_dir.delete(recursive=True)

        source_ds = make_test_dataset(uri=source_dir.uri)

        transmit(source_dir, target_dir,
                 rollback_cb=undo_manager.add_undo_op)

        self.assertEqual([('delete_dir', '/target.zarr')],
                         undo_manager.ops)

        target_dir.delete(recursive=True)
        target_dir.mkdir()

        undo_manager.reset()
        transmit(source_dir, target_dir,
                 rollback_cb=undo_manager.add_undo_op)

        self.assertEqual(
            {
                ('delete_file', '/target.zarr/.zmetadata'),
                ('delete_file', '/target.zarr/.zgroup'),
                ('delete_file', '/target.zarr/.zattrs'),
                ('delete_dir', '/target.zarr/chl'),
                ('delete_dir', '/target.zarr/tsm'),
                ('delete_dir', '/target.zarr/x'),
                ('delete_dir', '/target.zarr/y'),
                ('delete_dir', '/target.zarr/time'),
            },
            set(undo_manager.ops)
        )

        undo_manager.reset()
        transmit(source_dir, target_dir,
                 rollback_cb=undo_manager.add_undo_op)

        self.assertNotIn(('delete_dir', '/target.zarr'), undo_manager.ops)
        self.assertIn(('replace_file', '/target.zarr/.zmetadata'),
                      undo_manager.ops)
        self.assertIn(('replace_file', '/target.zarr/.zgroup'),
                      undo_manager.ops)
        self.assertIn(('replace_file', '/target.zarr/.zattrs'),
                      undo_manager.ops)
        self.assertIn(('replace_file', '/target.zarr/y/.zarray'),
                      undo_manager.ops)
        self.assertIn(('replace_file', '/target.zarr/y/.zattrs'),
                      undo_manager.ops)
        self.assertIn(('replace_file', '/target.zarr/y/0'),
                      undo_manager.ops)

        target_ds = xr.open_zarr(target_dir.uri, decode_cf=False)

        self.assertEqual(source_ds.dims, target_ds.dims)
        self.assertEqual(set(source_ds.variables.keys()),
                         set(target_ds.variables.keys()))

        source_dir.delete(recursive=True)
        target_dir.delete(recursive=True)

    def test_transmit_with_file_filter(self):
        records = set()

        def my_file_filter(path, filename, data):
            self.assertIsInstance(path, str)
            self.assertIsInstance(filename, str)
            self.assertIsInstance(data, (bytes, type(None)))
            records.add((path, filename, None if data is None else len(data)))
            return filename, data

        protocol = "memory"
        source_dir = FileObj(f"{protocol}://source.zarr")
        target_dir = FileObj(f"{protocol}://target.zarr")
        if source_dir.exists():
            source_dir.delete(recursive=True)
        if target_dir.exists():
            target_dir.delete(recursive=True)

        make_test_dataset(uri=source_dir.uri)

        transmit(source_dir, target_dir,
                 file_filter=my_file_filter)

        self.assertEqual(
            {
                ('/source.zarr', '.zattrs', 2),
                ('/source.zarr', '.zgroup', 24),
                ('/source.zarr', '.zmetadata', 3501),
                ('/source.zarr/chl', '.zarray', 361),
                ('/source.zarr/chl', '.zattrs', 123),
                ('/source.zarr/chl', '0.0.0', 60),
                ('/source.zarr/chl', '0.0.1', 60),
                ('/source.zarr/chl', '0.1.0', 68),
                ('/source.zarr/chl', '0.1.1', 68),
                ('/source.zarr/chl', '1.0.0', 60),
                ('/source.zarr/chl', '1.0.1', 60),
                ('/source.zarr/chl', '1.1.0', 68),
                ('/source.zarr/chl', '1.1.1', 68),
                ('/source.zarr/chl', '2.0.0', 60),
                ('/source.zarr/chl', '2.0.1', 60),
                ('/source.zarr/chl', '2.1.0', 68),
                ('/source.zarr/chl', '2.1.1', 68),
                ('/source.zarr/time', '.zarray', 312),
                ('/source.zarr/time', '.zattrs', 51),
                ('/source.zarr/time', '0', 40),
                ('/source.zarr/tsm', '.zarray', 362),
                ('/source.zarr/tsm', '.zattrs', 127),
                ('/source.zarr/tsm', '0.0.0', 60),
                ('/source.zarr/tsm', '0.0.1', 60),
                ('/source.zarr/tsm', '0.1.0', 68),
                ('/source.zarr/tsm', '0.1.1', 68),
                ('/source.zarr/tsm', '1.0.0', 60),
                ('/source.zarr/tsm', '1.0.1', 60),
                ('/source.zarr/tsm', '1.1.0', 68),
                ('/source.zarr/tsm', '1.1.1', 68),
                ('/source.zarr/tsm', '2.0.0', 60),
                ('/source.zarr/tsm', '2.0.1', 60),
                ('/source.zarr/tsm', '2.1.0', 68),
                ('/source.zarr/tsm', '2.1.1', 68),
                ('/source.zarr/x', '.zarray', 317),
                ('/source.zarr/x', '.zattrs', 48),
                ('/source.zarr/x', '0', 745),
                ('/source.zarr/y', '.zarray', 315),
                ('/source.zarr/y', '.zattrs', 48),
                ('/source.zarr/y', '0', 393),
            },
            records
        )


class UndoManager:
    def __init__(self):
        self.ops = []

    # noinspection PyUnusedLocal
    def add_undo_op(self, op: str, path: str, data: bytes | None):
        self.ops.append((op, path))

    def reset(self):
        self.ops = []
