# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
import unittest

import xarray as xr

from zappend.fsutil import FileObj
from zappend.levels import write_levels
from zappend.levels import get_variables_config
from .helpers import clear_memory_fs
from .helpers import make_test_dataset

try:
    import xcube
except:
    xcube = None


class GetVariablesConfigTest(unittest.TestCase):
    def test_it(self):
        dataset = make_test_dataset()
        variables = get_variables_config(dataset, dict(x=512, y=256, time=1))
        self.assertEqual(
            {
                "x": {"encoding": {"chunks": None}},
                "y": {"encoding": {"chunks": None}},
                "time": {"encoding": {"chunks": None}},
                "chl": {"encoding": {"chunks": [1, 256, 512]}},
                "tsm": {"encoding": {"chunks": [1, 256, 512]}},
            },
            variables,
        )


@unittest.skipIf(xcube is None, reason="xcube is not installed")
class WriteLevelsTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_it(self):
        source_path = "memory://source.zarr"
        make_test_dataset(
            uri=source_path,
            dims=("time", "y", "x"),
            shape=(3, 1024, 2048),
            chunks=(1, 128, 256),
            crs="EPSG:4326",
        )

        target_dir = FileObj("memory://target.levels")
        self.assertFalse(target_dir.exists())

        write_levels(source_path=source_path, target_path=target_dir.uri)

        self.assertTrue(target_dir.exists())

        levels_file = target_dir.for_path(".zlevels")
        self.assertTrue(levels_file.exists())
        levels_info = json.loads(levels_file.read())
        self.assertEqual(
            {
                "version": "1.0",
                "num_levels": 4,
                "agg_methods": {"chl": "mean", "tsm": "mean"},
                "use_saved_levels": False,
            },
            levels_info,
        )

        ds0 = xr.open_zarr(target_dir.uri + f"/0.zarr")
        self.assertEqual({"time": 3, "y": 1024, "x": 2048}, ds0.sizes)

        ds1 = xr.open_zarr(target_dir.uri + f"/1.zarr")
        self.assertEqual({"time": 3, "y": 512, "x": 1024}, ds1.sizes)

        ds2 = xr.open_zarr(target_dir.uri + f"/2.zarr")
        self.assertEqual({"time": 3, "y": 256, "x": 512}, ds2.sizes)

        ds3 = xr.open_zarr(target_dir.uri + f"/3.zarr")
        self.assertEqual({"time": 3, "y": 128, "x": 256}, ds3.sizes)
