# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

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
        target_uri = "memory://target.zarr"
        zappend([],
                target_uri=target_uri)
        self.assertFalse(FileObj(target_uri).exists())

    def test_3_slices(self):
        target_uri = "memory://target.zarr"
        slices = [make_test_dataset(),
                  make_test_dataset(),
                  make_test_dataset()]
        zappend(slices,
                target_uri=target_uri)
        ds = xr.open_zarr(target_uri)
        self.assertEqual({'time': 9, 'y': 50, 'x': 100}, ds.dims)
        self.assertEqual({'chl', 'tsm'}, set(ds.data_vars))
        self.assertEqual({'time', 'y', 'x'}, set(ds.coords))
