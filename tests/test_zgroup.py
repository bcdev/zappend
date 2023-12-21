# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
import math
import unittest

import zarr

from zappend.fileobj import FileObj
from zappend.zgroup import get_zarr_updates
from zappend.zgroup import open_zarr_group
from zappend.zgroup import get_chunks_update_range
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


class GetChunkActionsTest(unittest.TestCase):

    def test_chunk_size_1(self):
        def range_with_chunk_size_1(size, append_size):
            return get_chunks_update_range(size, chunk_size=1,
                                           append_size=append_size)

        self.assertEqual(
            (False, (0, 1)),
            range_with_chunk_size_1(size=0, append_size=1)
        )
        self.assertEqual(
            (False, (1, 2)),
            range_with_chunk_size_1(size=1, append_size=1)
        )
        self.assertEqual(
            (False, (2, 3)),
            range_with_chunk_size_1(size=2, append_size=1)
        )
        self.assertEqual(
            (False, (3, 4)),
            range_with_chunk_size_1(size=3, append_size=1)
        )
        self.assertEqual(
            (False, (1, 3)),
            range_with_chunk_size_1(size=1, append_size=2)
        )
        self.assertEqual(
            (False, (1, 4)),
            range_with_chunk_size_1(size=1, append_size=3)
        )

    def test_chunk_size_3(self):
        def range_with_chunk_size_3(size, append_size):
            return get_chunks_update_range(size, chunk_size=3,
                                           append_size=append_size)

        self.assertEqual(
            (True, (0, 1)),
            range_with_chunk_size_3(size=1, append_size=1)
        )
        self.assertEqual(
            (True, (0, 1)),
            range_with_chunk_size_3(size=1, append_size=1)
        )
        self.assertEqual(
            (True, (0, 1)),
            range_with_chunk_size_3(size=2, append_size=1)
        )
        self.assertEqual(
            (False, (1, 2)),
            range_with_chunk_size_3(size=3, append_size=1)
        )
        self.assertEqual(
            (True, (1, 2)),
            range_with_chunk_size_3(size=4, append_size=1)
        )
        self.assertEqual(
            (True, (1, 2)),
            range_with_chunk_size_3(size=4, append_size=2)
        )
        self.assertEqual(
            (True, (1, 3)),
            range_with_chunk_size_3(size=4, append_size=3)
        )
        self.assertEqual(
            (False, (4, 5)),
            range_with_chunk_size_3(size=12, append_size=3)
        )
        self.assertEqual(
            (False, (4, 6)),
            range_with_chunk_size_3(size=12, append_size=4)
        )
        self.assertEqual(
            (True, (4, 6)),
            range_with_chunk_size_3(size=13, append_size=4)
        )
        self.assertEqual(
            (False, (4, 8)),
            range_with_chunk_size_3(size=12, append_size=12)
        )

