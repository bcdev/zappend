# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
import os
import shutil
import unittest

import xarray as xr
from zappend.api import zappend
from zappend.fsutil.fileobj import FileObj
from .helpers import clear_memory_fs
from .helpers import make_test_dataset


class ApiTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_no_slices(self):
        target_dir = "memory://target.zarr"
        zappend([], target_dir=target_dir)
        self.assertFalse(FileObj(target_dir).exists())

    def test_some_slices_memory(self):
        target_dir = "memory://target.zarr"
        slices = [make_test_dataset(), make_test_dataset(), make_test_dataset()]
        zappend(slices, target_dir=target_dir)
        ds = xr.open_zarr(target_dir)
        self.assertEqual({"time": 9, "y": 50, "x": 100}, ds.sizes)
        self.assertEqual({"chl", "tsm"}, set(ds.data_vars))
        self.assertEqual({"time", "y", "x"}, set(ds.coords))

    def test_some_slices_local(self):
        target_dir = "target.zarr"
        slices = [
            "slice-1.zarr",
            "slice-2.zarr",
            "slice-3.zarr",
        ]
        for uri in slices:
            make_test_dataset(uri=uri)
        try:
            zappend(slices, target_dir=target_dir)
            ds = xr.open_zarr(target_dir)
            self.assertEqual({"time": 9, "y": 50, "x": 100}, ds.sizes)
            self.assertEqual({"chl", "tsm"}, set(ds.data_vars))
            self.assertEqual({"time", "y", "x"}, set(ds.coords))
        finally:
            shutil.rmtree(target_dir, ignore_errors=True)
            for slice_dir in slices:
                shutil.rmtree(slice_dir, ignore_errors=True)

    def test_some_slices_with_profiling(self):
        target_dir = "memory://target.zarr"
        slices = [
            "memory://slice-1.zarr",
            "memory://slice-2.zarr",
            "memory://slice-3.zarr",
        ]
        for uri in slices:
            make_test_dataset(uri=uri)
        try:
            zappend(
                slices,
                config={
                    "target_dir": target_dir,
                    "profiling": {
                        "path": "prof.out",
                        "keys": ["tottime", "time", "ncalls"],
                    },
                },
            )
            self.assertTrue(os.path.exists("prof.out"))
        finally:
            if os.path.exists("prof.out"):
                os.remove("prof.out")
