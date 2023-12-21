# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
import xarray as xr

from zappend.context import Context
from zappend.processor import Processor
from .helpers import make_test_dataset


class TestProcessor(unittest.TestCase):
    def test_process_one_slice(self):
        test_ds_kwargs = dict(shape=(1, 10, 20), chunks=(1, 5, 10))
        config = {}

        ctx = Context("memory://target.zarr", config=config)
        if ctx.target_dir.exists():
            ctx.target_dir.delete(recursive=True)
        self.assertFalse(ctx.target_dir.exists())

        processor = Processor(ctx)
        ds1 = make_test_dataset(uri="memory://slice-1.zarr", **test_ds_kwargs)
        processor.process_slices([ds1])

        self.assertTrue(ctx.target_dir.exists())

        ds = xr.open_zarr(ctx.target_dir.uri,
                          storage_options=ctx.target_dir.storage_options,
                          decode_cf=True)
        self.assertEqual({'time': 1, 'y': 10, 'x': 20}, ds.dims)
        self.assertEqual({'x', 'y', 'time', 'chl', 'tsm'}, set(ds.variables))

    def test_process_two_slices(self):
        test_ds_kwargs = dict(shape=(1, 10, 20), chunks=(1, 5, 10))

        config = {}

        ctx = Context("memory://target.zarr", config=config)
        if ctx.target_dir.exists():
            ctx.target_dir.delete(recursive=True)
        self.assertFalse(ctx.target_dir.exists())

        processor = Processor(ctx)
        ds1 = make_test_dataset(uri="memory://slice-1.zarr", **test_ds_kwargs)
        ds2 = make_test_dataset(uri="memory://slice-2.zarr", **test_ds_kwargs)
        processor.process_slices([ds1, ds2])

        self.assertTrue(ctx.target_dir.exists())

        ds = xr.open_zarr(ctx.target_dir.uri,
                          storage_options=ctx.target_dir.storage_options,
                          decode_cf=True)
        self.assertEqual({'x', 'y', 'time', 'chl', 'tsm'}, set(ds.variables))
        self.assertEqual({'time': 2, 'y': 10, 'x': 20}, ds.dims)
