# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
import math
import unittest

import zarr

from zappend.fileobj import FileObj
from zappend.zgroup import get_zarr_updates
from zappend.zgroup import open_zarr_group
from zappend.zgroup import get_chunk_actions
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
        def get_chunk_actions_cs1(size, append_size):
            return get_chunk_actions(size, append_size, chunk_size=1)

        self.assertEqual(
            [('create', 0)],
            get_chunk_actions_cs1(size=0, append_size=1)
        )
        self.assertEqual(
            [('create', 1)],
            get_chunk_actions_cs1(size=1, append_size=1)
        )
        self.assertEqual(
            [('create', 2)],
            get_chunk_actions_cs1(size=2, append_size=1)
        )
        self.assertEqual(
            [('create', 3)],
            get_chunk_actions_cs1(size=3, append_size=1)
        )
        self.assertEqual(
            [('create', 1), ('create', 2)],
            get_chunk_actions_cs1(size=1, append_size=2)
        )
        self.assertEqual(
            [('create', 1), ('create', 2), ('create', 3)],
            get_chunk_actions_cs1(size=1, append_size=3)
        )

    def test_chunk_size_3(self):
        def get_chunk_actions_cs3(size, append_size):
            return get_chunk_actions(size, append_size, chunk_size=3)

        self.assertEqual(
            [('update', 0)],
            get_chunk_actions_cs3(size=1, append_size=1)
        )
        self.assertEqual(
            [('update', 0)],
            get_chunk_actions_cs3(size=1, append_size=1)
        )
        self.assertEqual(
            [('update', 0)],
            get_chunk_actions_cs3(size=2, append_size=1)
        )
        self.assertEqual(
            [('create', 1)],
            get_chunk_actions_cs3(size=3, append_size=1)
        )
        self.assertEqual(
            [('update', 1)],
            get_chunk_actions_cs3(size=4, append_size=1)
        )
        self.assertEqual(
            [('update', 1)],
            get_chunk_actions_cs3(size=4, append_size=2)
        )
        self.assertEqual(
            [('update', 1), ('create', 2)],
            get_chunk_actions_cs3(size=4, append_size=3)
        )
        self.assertEqual(
            [('create', 4)],
            get_chunk_actions_cs3(size=12, append_size=3)
        )
        self.assertEqual(
            [('create', 4), ('create', 5)],
            get_chunk_actions_cs3(size=12, append_size=4)
        )
        self.assertEqual(
            [('update', 4), ('create', 5)],
            get_chunk_actions_cs3(size=13, append_size=4)
        )
        self.assertEqual(
            [('create', 4), ('create', 5), ('create', 6), ('create', 7)],
            get_chunk_actions_cs3(size=12, append_size=12)
        )

