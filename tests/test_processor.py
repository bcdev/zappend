# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
import xarray as xr

from zappend.fsutil.fileobj import FileObj
from zappend.processor import Processor
from .helpers import clear_memory_fs
from .helpers import make_test_dataset


class TestProcessor(unittest.TestCase):

    def setUp(self):
        clear_memory_fs()

    def test_process_one_slice(self):
        target_dir = FileObj("memory://target.zarr")
        self.assertFalse(target_dir.exists())

        processor = Processor(dict(target_uri=target_dir.uri))
        test_ds_kwargs = dict(shape=(1, 10, 20), chunks=(1, 5, 10))
        ds1 = make_test_dataset(uri="memory://slice-1.zarr", **test_ds_kwargs)
        processor.process_slices([ds1])

        self.assertTrue(target_dir.exists())
        ds = xr.open_zarr(target_dir.uri,
                          storage_options=target_dir.storage_options,
                          decode_cf=True)
        self.assertEqual({'time': 1, 'y': 10, 'x': 20}, ds.dims)
        self.assertEqual({'x', 'y', 'time', 'chl', 'tsm'}, set(ds.variables))

    def test_process_two_slices(self):
        target_dir = FileObj("memory://target.zarr")
        self.assertFalse(target_dir.exists())

        processor = Processor(dict(target_uri=target_dir.uri))
        test_ds_kwargs = dict(shape=(1, 10, 20), chunks=(1, 5, 10))
        ds1 = make_test_dataset(uri="memory://slice-1.zarr", **test_ds_kwargs)
        ds2 = make_test_dataset(uri="memory://slice-2.zarr", **test_ds_kwargs)
        processor.process_slices([ds1, ds2])

        self.assertTrue(target_dir.exists())
        ds = xr.open_zarr(target_dir.uri,
                          storage_options=target_dir.storage_options,
                          decode_cf=True)
        self.assertEqual({'x', 'y', 'time', 'chl', 'tsm'}, set(ds.variables))
        self.assertEqual({'time': 2, 'y': 10, 'x': 20}, ds.dims)
