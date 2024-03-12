# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import numpy as np
import pytest
import xarray as xr

from zappend.fsutil.fileobj import FileObj
from zappend.context import Context
from zappend.processor import Processor
from zappend.processor import to_timedelta
from zappend.processor import verify_append_labels
from .helpers import clear_memory_fs
from .helpers import make_test_dataset


class ProcessorTest(unittest.TestCase):
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
        self.assertEqual({"time": 1, "y": 10, "x": 20}, ds.sizes)
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
        self.assertEqual({"time": 2, "y": 10, "x": 20}, ds.sizes)

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
        self.assertEqual({"time": many, "y": 10, "x": 20}, ds.sizes)

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
        self.assertEqual({"time": 4, "y": 10, "x": 20}, ds.sizes)

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


# noinspection PyMethodMayBeStatic
class AppendLabelValidationTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_verify_append_labels_succeeds(self):
        ctx = Context({"target_dir": "memory://target.zarr", "append_step": "1D"})

        # Ok, because we have no delta
        slice_ds = make_test_dataset(shape=(1, 50, 100))
        verify_append_labels(ctx, slice_ds)

        # Ok, because we have 4 deltas that are 1D
        slice_ds = make_test_dataset(shape=(5, 50, 100))
        verify_append_labels(ctx, slice_ds)

        # Ok, because after removing "time" coordinate variable,
        # xarray will use numerical labels
        ctx = Context({"target_dir": "memory://target.zarr", "append_step": 1})
        slice_ds = make_test_dataset(shape=(3, 50, 100)).drop_vars(["time"])
        verify_append_labels(ctx, slice_ds)

        # Ok, because "foo" has no labels
        ctx = Context(
            {
                "target_dir": "memory://target.zarr",
                "append_dim": "foo",
                "append_step": "1D",
            }
        )
        slice_ds = make_test_dataset(shape=(3, 50, 100))
        verify_append_labels(ctx, slice_ds)

    def test_verify_append_labels_fails(self):
        ctx = Context({"target_dir": "memory://target.zarr", "append_step": "2D"})
        slice_ds = make_test_dataset(shape=(3, 50, 100))
        with pytest.raises(
            ValueError,
            match="Cannot append slice because this would result in an invalid step size.",
        ):
            verify_append_labels(ctx, slice_ds)

        ctx = Context({"target_dir": "memory://target.zarr", "append_step": "-"})
        slice_ds = make_test_dataset(shape=(3, 50, 100))
        with pytest.raises(
            ValueError,
            match="Cannot append slice because labels must be monotonically decreasing.",
        ):
            verify_append_labels(ctx, slice_ds)

        ctx = Context({"target_dir": "memory://target.zarr", "append_step": "+"})
        slice_ds = make_test_dataset(shape=(3, 50, 100))
        time = slice_ds["time"]
        slice_ds["time"] = xr.DataArray(
            list(reversed(time.values)), dims=time.dims, attrs=time.attrs
        )
        with pytest.raises(
            ValueError,
            match="Cannot append slice because labels must be monotonically increasing.",
        ):
            verify_append_labels(ctx, slice_ds)


class ToTimedeltaTest(unittest.TestCase):
    def test_it(self):
        self.assertEqual(np.timedelta64(1, "s"), to_timedelta("s"))
        self.assertEqual(np.timedelta64(1, "m"), to_timedelta("m"))
        self.assertEqual(np.timedelta64(1, "h"), to_timedelta("h"))
        self.assertEqual(np.timedelta64(1, "h"), to_timedelta("1h"))
        self.assertEqual(np.timedelta64(24, "h"), to_timedelta("24h"))
        self.assertEqual(np.timedelta64(1, "D"), to_timedelta("24h"))
        self.assertEqual(np.timedelta64(1, "D"), to_timedelta("D"))
        self.assertEqual(np.timedelta64(1, "D"), to_timedelta("1D"))
        self.assertEqual(np.timedelta64(7, "D"), to_timedelta("7D"))
        self.assertEqual(np.timedelta64(1, "W"), to_timedelta("7D"))
        self.assertEqual(np.timedelta64(12, "D"), to_timedelta("12D"))
        self.assertEqual(np.timedelta64(60 * 60 * 24, "s"), to_timedelta(60 * 60 * 24))
        self.assertEqual(np.timedelta64(1, "D"), to_timedelta(60 * 60 * 24))
