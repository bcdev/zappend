# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

from zappend.chunkutil import get_chunk_update_range
from zappend.chunkutil import get_chunk_indices


class GetChunkIndicesTest(unittest.TestCase):

    def assert_ok(self,
                  expected,
                  shape: tuple[int, ...],
                  chunks: tuple[int, ...],
                  append_dim_axis: int,
                  append_dim_range: tuple[int, int]):
        self.assertEqual(
            expected,
            list(get_chunk_indices(shape,
                                   chunks,
                                   append_dim_axis,
                                   append_dim_range))
        )

    def test_get_chunk_indices_no_range(self):
        self.assert_ok(
            [],
            (1,), (1,), 0, (0, 0)
        )
        self.assert_ok(
            [],
            (10, 20, 30), (10, 20, 30), 1, (100, 100)
        )

    def test_get_chunk_indices_1d(self):
        self.assert_ok(
            [(0,)],
            (1,), (1,), 0, (0, 1)
        )
        self.assert_ok(
            [(0,), (1,), (2,), (3,)],
            (1,), (1,), 0, (0, 4)
        )
        self.assert_ok(
            [(6,), (7,)],
            (200,), (50,), 0, (6, 8)
        )
        self.assert_ok(
            [(6,), (7,)],
            (8923495,), (134563,), 0, (6, 8)
        )

    def test_get_chunk_indices_3d(self):
        self.assert_ok(
            [
                (0, 0, 0), (0, 0, 1), (0, 0, 2),
                (0, 1, 0), (0, 1, 1), (0, 1, 2),
            ],
            (50, 100, 300), (1, 50, 100), 0, (0, 1)
        )
        self.assert_ok(
            [
                (3, 0, 0), (3, 0, 1), (3, 0, 2),
                (3, 1, 0), (3, 1, 1), (3, 1, 2),
                (4, 0, 0), (4, 0, 1), (4, 0, 2),
                (4, 1, 0), (4, 1, 1), (4, 1, 2),
            ],
            (50, 100, 300), (10, 50, 100), 0, (3, 5)
        )
        self.assert_ok(
            [
                (0, 0, 3), (0, 0, 4), (0, 1, 3), (0, 1, 4),
                (1, 0, 3), (1, 0, 4), (1, 1, 3), (1, 1, 4),
                (2, 0, 3), (2, 0, 4), (2, 1, 3), (2, 1, 4),
                (3, 0, 3), (3, 0, 4), (3, 1, 3), (3, 1, 4),
                (4, 0, 3), (4, 0, 4), (4, 1, 3), (4, 1, 4),
            ],
            (50, 100, 300), (10, 50, 100), 2, (3, 5)
        )


class GetChunkUpdateRangeTest(unittest.TestCase):

    def test_chunk_size_1(self):
        def range_with_chunk_size_1(size, append_size):
            return get_chunk_update_range(size, chunk_size=1,
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
            return get_chunk_update_range(size, chunk_size=3,
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
