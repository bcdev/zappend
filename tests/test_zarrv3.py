#  Copyright © 2024-2026 Brockmann Consult and contributors
#  Permissions are hereby granted under the terms of the MIT License:
#  https://opensource.org/licenses/MIT.

import unittest

import xarray as xr
import zarr

from zappend.api import zappend

from .helpers import clear_memory_fs, make_test_dataset


class ZarrV3Test(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_append_sharded_v3_target(self):
        target_dir = "memory://target.zarr"
        slices = [
            make_test_dataset(shape=(1, 10, 20), chunks=(1, 5, 10)),
            make_test_dataset(shape=(1, 10, 20), chunks=(1, 5, 10)),
        ]

        zappend(
            slices,
            target_dir=target_dir,
            zarr_version=3,
            variables={
                "chl": {"encoding": {"chunks": [1, 5, 10], "shards": [2, 10, 20]}},
                "tsm": {"encoding": {"chunks": [1, 5, 10], "shards": [2, 10, 20]}},
            },
        )

        with xr.open_zarr(target_dir, zarr_format=3) as dataset:
            self.assertEqual({"time": 2, "y": 10, "x": 20}, dataset.sizes)

        group = zarr.open_group(target_dir, zarr_format=3)
        self.assertTrue(
            any(
                type(codec).__name__ == "ShardingCodec"
                for codec in group["chl"].metadata.codecs
            )
        )
