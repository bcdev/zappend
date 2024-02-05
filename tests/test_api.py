# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
import os
import shutil
import unittest

import numpy as np
import pytest
import xarray as xr

from zappend.api import FileObj
from zappend.api import SliceSource
from zappend.api import zappend
from .helpers import clear_memory_fs
from .helpers import make_test_dataset


# noinspection PyMethodMayBeStatic
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

    def test_some_slices_with_class_slice_source(self):
        target_dir = "memory://target.zarr"
        slices = [make_test_dataset(), make_test_dataset(), make_test_dataset()]
        zappend(slices, target_dir=target_dir, slice_source=MySliceSource)
        ds = xr.open_zarr(target_dir)
        self.assertEqual({"time": 9, "y": 50, "x": 100}, ds.sizes)
        self.assertEqual({"chl"}, set(ds.data_vars))
        self.assertEqual({"time", "y", "x"}, set(ds.coords))

    def test_some_slices_with_func_slice_source(self):
        def process_slice(ctx, slice_ds: xr.Dataset) -> SliceSource:
            return MySliceSource(ctx, slice_ds)

        target_dir = "memory://target.zarr"
        slices = [make_test_dataset(), make_test_dataset(), make_test_dataset()]
        zappend(slices, target_dir=target_dir, slice_source=process_slice)
        ds = xr.open_zarr(target_dir)
        self.assertEqual({"time": 9, "y": 50, "x": 100}, ds.sizes)
        self.assertEqual({"chl"}, set(ds.data_vars))
        self.assertEqual({"time", "y", "x"}, set(ds.coords))

    def test_some_slices_with_inc_append_step(self):
        target_dir = "memory://target.zarr"
        slices = [
            make_test_dataset(index=0, shape=(1, 50, 100)),
            make_test_dataset(index=1, shape=(1, 50, 100)),
            make_test_dataset(index=2, shape=(1, 50, 100)),
        ]
        zappend(slices, target_dir=target_dir, append_step="1D")
        ds = xr.open_zarr(target_dir)
        np.testing.assert_array_equal(
            ds.time.values,
            np.array(["2024-01-01", "2024-01-02", "2024-01-03"], dtype=np.datetime64),
        )

    def test_some_slices_with_dec_append_step(self):
        target_dir = "memory://target.zarr"
        slices = [
            make_test_dataset(index=2, shape=(1, 50, 100)),
            make_test_dataset(index=1, shape=(1, 50, 100)),
            make_test_dataset(index=0, shape=(1, 50, 100)),
        ]
        zappend(slices, target_dir=target_dir, append_step="-1D")
        ds = xr.open_zarr(target_dir)
        np.testing.assert_array_equal(
            ds.time.values,
            np.array(["2024-01-03", "2024-01-02", "2024-01-01"], dtype=np.datetime64),
        )

    # # See https://github.com/bcdev/zappend/issues/21
    #
    # def test_some_slices_with_one_missing_append_step(self):
    #     target_dir = "memory://target.zarr"
    #     slices = [
    #         make_test_dataset(index=0, shape=(1, 50, 100)),
    #         make_test_dataset(index=2, shape=(1, 50, 100)),
    #     ]
    #     zappend(slices, target_dir=target_dir, append_step="1D")
    #     ds = xr.open_zarr(target_dir)
    #     np.testing.assert_array_equal(
    #         ds.time.values,
    #         np.array(
    #             ["2024-01-01", "2024-01-02", "2024-01-03"], dtype="datetime64[ns]"
    #         ),
    #     )

    # # See https://github.com/bcdev/zappend/issues/21
    #
    # def test_some_slices_with_three_missing_append_steps(self):
    #     target_dir = "memory://target.zarr"
    #     slices = [
    #         make_test_dataset(index=0, shape=(1, 50, 100)),
    #         make_test_dataset(index=4, shape=(1, 50, 100)),
    #     ]
    #     zappend(slices, target_dir=target_dir, append_step="1D")
    #     ds = xr.open_zarr(target_dir)
    #     np.testing.assert_array_equal(
    #         ds.time.values,
    #         np.array(
    #             [
    #                 "2024-01-01",
    #                 "2024-01-02",
    #                 "2024-01-03",
    #                 "2024-01-04",
    #                 "2024-01-05",
    #             ],
    #             dtype="datetime64[ns]",
    #         ),
    #     )

    def test_it_raises_for_wrong_append_step(self):
        target_dir = "memory://target.zarr"
        slices = [
            make_test_dataset(index=0, shape=(1, 50, 100)),
            make_test_dataset(index=1, shape=(1, 50, 100)),
        ]
        with pytest.raises(
            ValueError,
            match=(
                "Cannot append slice because this would result in"
                " an invalid step size."
            ),
        ):
            zappend(slices, target_dir=target_dir, append_step="2D")

    def test_some_slices_with_inc_append_labels(self):
        append_step = "+"

        target_dir = "memory://target.zarr"
        slices = [
            make_test_dataset(index=0, shape=(1, 50, 100)),
            make_test_dataset(index=1, shape=(1, 50, 100)),
            make_test_dataset(index=2, shape=(1, 50, 100)),
        ]
        # OK!
        zappend(slices, target_dir=target_dir, append_step=append_step)

        target_dir = "memory://target.zarr"
        slices = [
            make_test_dataset(index=1, shape=(1, 50, 100)),
            make_test_dataset(index=0, shape=(1, 50, 100)),
        ]
        with pytest.raises(
            ValueError,
            match=(
                "Cannot append slice because labels must be monotonically increasing"
            ),
        ):
            zappend(slices, target_dir=target_dir, append_step=append_step)

    def test_some_slices_with_dec_append_labels(self):
        append_step = "-"

        target_dir = "memory://target.zarr"
        slices = [
            make_test_dataset(index=2, shape=(1, 50, 100)),
            make_test_dataset(index=1, shape=(1, 50, 100)),
            make_test_dataset(index=0, shape=(1, 50, 100)),
        ]
        # OK!
        zappend(slices, target_dir=target_dir, append_step=append_step)

        target_dir = "memory://target.zarr"
        slices = [
            make_test_dataset(index=0, shape=(1, 50, 100)),
            make_test_dataset(index=1, shape=(1, 50, 100)),
        ]
        with pytest.raises(
            ValueError,
            match=(
                "Cannot append slice because labels must be monotonically decreasing"
            ),
        ):
            zappend(slices, target_dir=target_dir, append_step=append_step)

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


class MySliceSource(SliceSource):
    def __init__(self, ctx, slice_ds):
        super().__init__(ctx)
        self.slice_ds = slice_ds

    def get_dataset(self) -> xr.Dataset:
        return self.slice_ds.drop_vars(["tsm"])
