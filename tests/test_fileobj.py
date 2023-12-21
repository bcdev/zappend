# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
import os.path
import uuid

import fsspec
import pytest

from zappend.fileobj import FileObj


class FileObjTest(unittest.TestCase):

    def test_str(self):
        self.assertEqual(
            "memory://test.zarr",
            str(FileObj("memory://test.zarr"))
        )
        self.assertEqual(
            "memory://test.zarr",
            str(FileObj("memory://test.zarr",
                        storage_options=dict(asynchronous=False)))
        )

    def test_repr(self):
        self.assertEqual(
            "FileObj('memory://test.zarr')",
            repr(FileObj("memory://test.zarr"))
        )
        self.assertEqual(
            "FileObj('memory://test.zarr',"
            " storage_options={'asynchronous': False})",
            repr(FileObj("memory://test.zarr",
                         storage_options=dict(asynchronous=False)))
        )

    def test_memory_protocol(self):
        zarr_dir = FileObj("memory://test.zarr")
        self.assertEqual("memory://test.zarr", zarr_dir.uri)
        self.assertEqual(None, zarr_dir.storage_options)
        self.assertIsInstance(zarr_dir.fs, fsspec.AbstractFileSystem)
        self.assertEqual("memory", to_protocol(zarr_dir.fs))
        self.assertEqual("/test.zarr", zarr_dir.path)

    def test_file_protocol(self):
        zarr_dir = FileObj("file://test.zarr")
        self.assertEqual("file://test.zarr", zarr_dir.uri)
        self.assertEqual(None, zarr_dir.storage_options)
        self.assertIsInstance(zarr_dir.fs, fsspec.AbstractFileSystem)
        self.assertEqual("file", to_protocol(zarr_dir.fs))
        self.assertEqual(os.path.abspath("test.zarr").replace("\\", "/"),
                         zarr_dir.path)

    def test_local_protocol(self):
        zarr_dir = FileObj("test.zarr")
        self.assertEqual("test.zarr", zarr_dir.uri)
        self.assertEqual(None, zarr_dir.storage_options)
        self.assertIsInstance(zarr_dir.fs, fsspec.AbstractFileSystem)
        self.assertEqual("file", to_protocol(zarr_dir.fs))
        self.assertEqual(os.path.abspath("test.zarr").replace("\\", "/"),
                         zarr_dir.path)

    def test_s3_protocol(self):
        zarr_dir = FileObj("s3://eo-data/test.zarr")
        self.assertEqual("s3://eo-data/test.zarr", zarr_dir.uri)
        self.assertEqual(None, zarr_dir.storage_options)
        self.assertIsInstance(zarr_dir.fs, fsspec.AbstractFileSystem)
        self.assertEqual("s3", to_protocol(zarr_dir.fs))
        self.assertEqual("eo-data/test.zarr", zarr_dir.path)

    def test_close(self):
        zarr_dir = FileObj("s3://eo-data/test.zarr")
        self.assertIsNone(zarr_dir._fs)
        self.assertIsNone(zarr_dir._path)
        fs = zarr_dir.fs
        self.assertIsInstance(fs, fsspec.AbstractFileSystem)
        self.assertIsNotNone(zarr_dir._fs)
        zarr_dir.close()
        self.assertIsNone(zarr_dir._fs)
        # See if we can close once more w.o. error
        zarr_dir.close()

    def test_truediv_override(self):
        root = FileObj("s3://eo-data/test.zarr")
        derived = root / ".zgroup"
        self.assert_derived_ok(root, derived,
                               "s3://eo-data/test.zarr/.zgroup",
                               "eo-data/test.zarr/.zgroup")

        derived = root / "chl" / ".zarray"
        self.assert_derived_ok(root, derived,
                               "s3://eo-data/test.zarr/chl/.zarray",
                               "eo-data/test.zarr/chl/.zarray")

    def test_parent(self):
        file = FileObj("s3://eo-data/test.zarr/.zmetadata")
        fs = file.fs

        parent = file.parent
        self.assertIsInstance(parent, FileObj)
        self.assertEqual("s3://eo-data/test.zarr", parent.uri)
        self.assertEqual("eo-data/test.zarr", parent.path)
        self.assertIs(fs, parent.fs)

        parent = parent.parent
        self.assertIsInstance(parent, FileObj)
        self.assertEqual("s3://eo-data", parent.uri)
        self.assertEqual("eo-data", parent.path)
        self.assertIs(fs, parent.fs)

        parent = parent.parent
        self.assertIsInstance(parent, FileObj)
        self.assertEqual("s3://", parent.uri)
        self.assertEqual("", parent.path)
        self.assertIs(fs, parent.fs)

        with pytest.raises(ValueError,
                           match="cannot get parent of empty path"):
            # noinspection PyUnusedLocal
            parent = parent.parent

    def test_for_path(self):
        root = FileObj("s3://eo-data/test.zarr")
        derived = root.for_path(".zgroup")
        self.assert_derived_ok(root, derived,
                               "s3://eo-data/test.zarr/.zgroup",
                               "eo-data/test.zarr/.zgroup")

    def test_for_path_with_chained_uri(self):
        root = FileObj("dir://chl::file:/eo-data/test.zarr")
        derived = root.for_path(".zarray")
        self.assert_derived_ok(root, derived,
                               "dir://chl/.zarray::file:/eo-data/test.zarr",
                               "chl/.zarray")

    def assert_derived_ok(self,
                          root: FileObj,
                          derived: FileObj,
                          expected_uri: str,
                          expected_path: str):
        self.assertEqual(expected_uri, derived.uri)
        self.assertEqual(expected_path, derived.path)
        self.assertIs(root.fs, derived.fs)
        self.assertIs(root.storage_options, derived.storage_options)

    # noinspection PyMethodMayBeStatic
    def test_for_path_with_abs_path(self):
        fo = FileObj("file:/eo-data/test.zarr")
        with pytest.raises(ValueError, match="rel_path must be relative"):
            fo.for_path("/test-2.zarr")

    # noinspection PyMethodMayBeStatic
    def test_for_path_with_wrong_type(self):
        fo = FileObj("file:/eo-data/test.zarr")
        with pytest.raises(TypeError, match="rel_path must have type str"):
            # noinspection PyTypeChecker
            fo.for_path(42)

    def test_basic_filesystem_ops(self):
        test_dir = FileObj(f"memory://{uuid.uuid4()}.zarr")
        self.assertFalse(test_dir.exists())

        test_dir.mkdir()
        self.assertTrue(test_dir.exists())

        txt_file = test_dir / "test.txt"
        self.assertFalse(txt_file.exists())

        txt_file.write("abc")
        self.assertTrue(txt_file.exists())

        txt_file.write("def", mode="a")
        self.assertTrue(txt_file.exists())

        bin_data = txt_file.read()
        self.assertIsInstance(bin_data, bytes)
        self.assertEqual(b"abcdef", bin_data)

        txt_data = txt_file.read(mode="r")
        self.assertIsInstance(txt_data, str)
        self.assertEqual("abcdef", txt_data)

        bin_file = test_dir / "test.bin"
        self.assertFalse(bin_file.exists())

        bin_file.write(b"123")
        self.assertTrue(bin_file.exists())

        bin_file.write(b"456", mode="ab")
        self.assertTrue(bin_file.exists())

        bin_data = bin_file.read()
        self.assertIsInstance(bin_data, bytes)
        self.assertEqual(b"123456", bin_data)

        bin_file.delete()
        self.assertFalse(bin_file.exists())

        with pytest.raises(OSError, match="Directory not empty"):
            # should fail because "<test_dir>/test.txt" still exist
            test_dir.delete()
        self.assertTrue(test_dir.exists())

        test_dir.delete(recursive=True)
        self.assertFalse(test_dir.exists())


def to_protocol(fs: fsspec.AbstractFileSystem):
    if isinstance(fs.protocol, tuple):
        return fs.protocol[0]
    return fs.protocol
