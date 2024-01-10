# Copyright © 2024 Norman Fomferra
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

        processor = Processor(dict(target_dir=target_dir.uri))
        test_ds_kwargs = dict(shape=(1, 10, 20), chunks=(1, 5, 10))
        make_test_dataset(uri="memory://slice-1.zarr", **test_ds_kwargs)
        processor.process_slices(["memory://slice-1.zarr"])

        self.assertTrue(target_dir.exists())
        ds = xr.open_zarr(
            target_dir.uri, storage_options=target_dir.storage_options, decode_cf=True
        )
        self.assertEqual({"time": 1, "y": 10, "x": 20}, ds.dims)
        self.assertEqual({"x", "y", "time", "chl", "tsm"}, set(ds.variables))

        self.assertEqual((20,), ds.x.encoding.get("chunks"))
        self.assertEqual((10,), ds.y.encoding.get("chunks"))
        self.assertEqual((1,), ds.time.encoding.get("chunks"))
        self.assertEqual((1, 5, 10), ds.chl.encoding.get("chunks"))
        self.assertEqual((1, 5, 10), ds.tsm.encoding.get("chunks"))

    def test_process_two_slices(self):
        target_dir = FileObj("memory://target.zarr")
        self.assertFalse(target_dir.exists())

        processor = Processor(dict(target_dir=target_dir.uri))
        test_ds_kwargs = dict(shape=(1, 10, 20), chunks=(1, 5, 10))
        make_test_dataset(uri="memory://slice-1.zarr", **test_ds_kwargs)
        make_test_dataset(uri="memory://slice-2.zarr", **test_ds_kwargs)
        processor.process_slices(["memory://slice-1.zarr", "memory://slice-2.zarr"])

        self.assertTrue(target_dir.exists())
        ds = xr.open_zarr(
            target_dir.uri, storage_options=target_dir.storage_options, decode_cf=True
        )

        self.assertEqual({"x", "y", "time", "chl", "tsm"}, set(ds.variables))
        self.assertEqual({"time": 2, "y": 10, "x": 20}, ds.dims)

        self.assertEqual((20,), ds.x.encoding.get("chunks"))
        self.assertEqual((10,), ds.y.encoding.get("chunks"))
        self.assertEqual((1,), ds.time.encoding.get("chunks"))
        self.assertEqual((1, 5, 10), ds.chl.encoding.get("chunks"))
        self.assertEqual((1, 5, 10), ds.tsm.encoding.get("chunks"))

        self.assertEqual(None, ds.x.chunks)
        self.assertEqual(None, ds.y.chunks)
        self.assertEqual(None, ds.time.chunks)
        self.assertEqual(((1, 1), (5, 5), (10, 10)), ds.chl.chunks)
        self.assertEqual(((1, 1), (5, 5), (10, 10)), ds.tsm.chunks)

    def test_process_many_slices_with_single_append_dim_chunk(self):
        many = 10

        target_dir = FileObj("memory://target.zarr")
        self.assertFalse(target_dir.exists())

        processor = Processor(
            {
                "target_dir": target_dir.uri,
                "variables": {"time": {"encoding": {"chunks": [many]}}},
            }
        )

        slices = [f"memory://slice-{i}.zarr" for i in range(many)]
        for uri in slices:
            make_test_dataset(uri=uri, shape=(1, 10, 20), chunks=(1, 5, 10))
        processor.process_slices(slices)

        self.assertTrue(target_dir.exists())
        ds = xr.open_zarr(
            target_dir.uri, storage_options=target_dir.storage_options, decode_cf=True
        )

        self.assertEqual({"x", "y", "time", "chl", "tsm"}, set(ds.variables))
        self.assertEqual({"time": many, "y": 10, "x": 20}, ds.dims)

        self.assertEqual((20,), ds.x.encoding.get("chunks"))
        self.assertEqual((10,), ds.y.encoding.get("chunks"))
        self.assertEqual((many,), ds.time.encoding.get("chunks"))
        self.assertEqual((1, 5, 10), ds.chl.encoding.get("chunks"))
        self.assertEqual((1, 5, 10), ds.tsm.encoding.get("chunks"))

        self.assertEqual(None, ds.x.chunks)
        self.assertEqual(None, ds.y.chunks)
        self.assertEqual(None, ds.time.chunks)
        self.assertEqual((many * (1,), (5, 5), (10, 10)), ds.chl.chunks)
        self.assertEqual((many * (1,), (5, 5), (10, 10)), ds.tsm.chunks)

    def test_process_two_slices_with_chunk_overlap(self):
        target_dir = FileObj("memory://target.zarr")
        self.assertFalse(target_dir.exists())

        processor = Processor(
            dict(
                target_dir=target_dir.uri,
                variables=dict(
                    chl=dict(encoding=dict(chunks=[3, 5, 10])),
                    tsm=dict(encoding=dict(chunks=[3, 5, 10])),
                ),
            )
        )
        test_ds_kwargs = dict(shape=(2, 10, 20), chunks=(2, 5, 10))
        make_test_dataset(uri="memory://slice-1.zarr", **test_ds_kwargs)
        make_test_dataset(uri="memory://slice-2.zarr", **test_ds_kwargs)
        processor.process_slices(["memory://slice-1.zarr", "memory://slice-2.zarr"])

        self.assertTrue(target_dir.exists())
        ds = xr.open_zarr(
            target_dir.uri, storage_options=target_dir.storage_options, decode_cf=True
        )

        self.assertEqual({"x", "y", "time", "chl", "tsm"}, set(ds.variables))
        self.assertEqual({"time": 4, "y": 10, "x": 20}, ds.dims)

        self.assertEqual((20,), ds.x.encoding.get("chunks"))
        self.assertEqual((10,), ds.y.encoding.get("chunks"))
        self.assertEqual((2,), ds.time.encoding.get("chunks"))
        self.assertEqual((3, 5, 10), ds.chl.encoding.get("chunks"))
        self.assertEqual((3, 5, 10), ds.tsm.encoding.get("chunks"))

        self.assertEqual(None, ds.x.chunks)
        self.assertEqual(None, ds.y.chunks)
        self.assertEqual(None, ds.time.chunks)
        self.assertEqual(((3, 1), (5, 5), (10, 10)), ds.chl.chunks)
        self.assertEqual(((3, 1), (5, 5), (10, 10)), ds.tsm.chunks)
