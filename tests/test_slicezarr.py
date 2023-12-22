# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

from zappend.context import Context
from zappend.fsutil.fileobj import FileObj
from zappend.slicezarr.common import open_slice_zarr
from zappend.slicezarr.inmemory import InMemorySliceZarr
from zappend.slicezarr.persistent import PersistentSliceZarr
from .helpers import make_test_dataset


class ZarrSliceTest(unittest.TestCase):
    def setUp(self) -> None:
        FileObj("memory:///").delete(recursive=True)

    def test_in_memory(self):
        dataset_dir = FileObj("memory://slice.zarr")
        dataset = make_test_dataset(uri=dataset_dir.uri)
        ctx = Context(dict(target_uri="memory://target.zarr",
                           temp_dir="memory://temp"))
        slice_zarr = open_slice_zarr(ctx, dataset)
        self.assertIsInstance(slice_zarr, InMemorySliceZarr)
        with slice_zarr as slice_fo:
            self.assertIsInstance(slice_fo, FileObj)
            self.assertTrue(slice_fo.uri.startswith("memory://temp/"))
            self.assertTrue(slice_fo.uri.endswith(".zarr"))

    def test_persistent_temp(self):
        dataset_dir = FileObj("memory://slice.zarr")
        make_test_dataset(uri=dataset_dir.uri)
        ctx = Context(dict(target_uri="memory://target.zarr",
                           temp_dir="memory://temp"))
        slice_zarr = open_slice_zarr(ctx, dataset_dir.uri)
        self.assertIsInstance(slice_zarr, PersistentSliceZarr)
        with slice_zarr as slice_fo:
            self.assertIsInstance(slice_fo, FileObj)
            self.assertTrue(slice_fo.uri.startswith("memory://temp/"))
            self.assertTrue(slice_fo.uri.endswith(".zarr"))

    def test_persistent_source(self):
        dataset_dir = FileObj("memory://slice.zarr")
        make_test_dataset(uri=dataset_dir.uri)
        ctx = Context(dict(target_uri="memory://target.zarr",
                           slice_access_mode="source",
                           temp_dir="memory://temp"))
        slice_zarr = open_slice_zarr(ctx, dataset_dir.uri)
        self.assertIsInstance(slice_zarr, PersistentSliceZarr)
        with slice_zarr as slice_fo:
            self.assertIsInstance(slice_fo, FileObj)
            self.assertEqual(dataset_dir.uri, slice_fo.uri)

