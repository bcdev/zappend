# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import zarr

from zappend.fileobj import FileObj
from zappend.zgroup import get_zarr_updates
from zappend.zgroup import open_zarr_group
from .helpers import make_test_dataset


class OpenZarrGroupTest(unittest.TestCase):

    def test_open_zarr_group(self):
        make_test_dataset(uri="memory://test.zarr")
        group = open_zarr_group(FileObj("memory://test.zarr"))
        self.assertIsInstance(group, zarr.Group)
        self.assertEqual({"chl", "tsm", "time", "x", "y"},
                         set(k for k, v in group.arrays()))
        self.assertEqual({}, group.attrs)


class GenerateUpdateRecordsTest(unittest.TestCase):
    def test_generate_update_records(self):
        make_test_dataset(uri="memory://target.zarr",
                          shape=(3, 10, 20),
                          chunks=(1, 5, 10))
        make_test_dataset(uri="memory://slice.zarr",
                          shape=(1, 10, 20),
                          chunks=(1, 5, 10))
        target_group = open_zarr_group(FileObj("memory://target.zarr"))
        slice_group = open_zarr_group(FileObj("memory://slice.zarr"))
        updates = get_zarr_updates(target_group, slice_group, "time")
        self.assertIsInstance(updates, dict)
        self.assertEqual(
            {
                "chl": (0, []),
                "tsm": (0, []),
                "time": (0, []),
            },
            updates
        )
