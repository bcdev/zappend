# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
import os.path
import fsspec

from zappend.fileobj import FileObj


class FileObjTest(unittest.TestCase):
    def test_memory_protocol(self):
        fo = FileObj("memory://test.zarr")
        self.assertEqual("memory://test.zarr", fo.uri)
        self.assertEqual({}, fo.storage_options)
        self.assertIsInstance(fo.filesystem, fsspec.AbstractFileSystem)
        self.assertEqual("memory", to_protocol(fo.filesystem))
        self.assertEqual("/test.zarr", fo.path)

    def test_file_protocol(self):
        fo = FileObj("file://test.zarr")
        self.assertEqual("file://test.zarr", fo.uri)
        self.assertEqual({}, fo.storage_options)
        self.assertIsInstance(fo.filesystem, fsspec.AbstractFileSystem)
        self.assertEqual("file", to_protocol(fo.filesystem))
        self.assertEqual(os.path.abspath("test.zarr").replace("\\", "/"),
                         fo.path)

    def test_local_protocol(self):
        fo = FileObj("test.zarr")
        self.assertEqual("test.zarr", fo.uri)
        self.assertEqual({}, fo.storage_options)
        self.assertIsInstance(fo.filesystem, fsspec.AbstractFileSystem)
        self.assertEqual("file", to_protocol(fo.filesystem))
        self.assertEqual(os.path.abspath("test.zarr").replace("\\", "/"),
                         fo.path)

    def test_s3_protocol(self):
        fo = FileObj("s3://eo-data/test.zarr")
        self.assertEqual("s3://eo-data/test.zarr", fo.uri)
        self.assertEqual({}, fo.storage_options)
        self.assertIsInstance(fo.filesystem, fsspec.AbstractFileSystem)
        self.assertEqual("s3", to_protocol(fo.filesystem))
        self.assertEqual("eo-data/test.zarr", fo.path)

    def test_close(self):
        fo = FileObj("s3://eo-data/test.zarr")
        self.assertIsNone(fo._filesystem)
        self.assertIsNone(fo._path)
        fs = fo.filesystem
        self.assertIsInstance(fs, fsspec.AbstractFileSystem)
        self.assertIsNotNone(fo._filesystem)
        fo.close()
        self.assertIsNone(fo._filesystem)
        # See if we can close once more w.o. error
        fo.close()

    def test_for_suffix(self):
        fo = FileObj("s3://eo-data/test.zarr")
        fo_suffixed = fo.for_suffix(".zgroup")
        fs = fo.filesystem
        fs_options = fo.storage_options
        self.assertEqual("s3://eo-data/test.zarr/.zgroup",
                         fo_suffixed.uri)
        self.assertEqual("eo-data/test.zarr/.zgroup",
                         fo_suffixed.path)
        self.assertIs(fs, fo_suffixed.filesystem)
        self.assertIs(fs_options, fo_suffixed.storage_options)


def to_protocol(fs: fsspec.AbstractFileSystem):
    if isinstance(fs.protocol, tuple):
        return fs.protocol[0]
    return fs.protocol
