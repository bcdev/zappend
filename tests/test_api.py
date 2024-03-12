# Copyright © 2024 Norman Fomferra and contributors
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
from zappend.fsutil.transaction import Transaction
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

    def test_one_slices_memory(self):
        target_dir = "memory://target.zarr"
        slices = [make_test_dataset()]
        zappend(slices, target_dir=target_dir)
        ds = xr.open_zarr(target_dir)
        self.assertEqual({"time": 3, "y": 50, "x": 100}, ds.sizes)
        self.assertEqual({"chl", "tsm"}, set(ds.data_vars))
        self.assertEqual({"time", "y", "x"}, set(ds.coords))
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "title": "Test 1-3",
            },
            ds.attrs,
        )

    def test_some_slices_memory(self):
        target_dir = "memory://target.zarr"
        slices = [make_test_dataset(index=3 * i) for i in range(3)]
        zappend(slices, target_dir=target_dir)
        ds = xr.open_zarr(target_dir)
        self.assertEqual({"time": 9, "y": 50, "x": 100}, ds.sizes)
        self.assertEqual({"chl", "tsm"}, set(ds.data_vars))
        self.assertEqual({"time", "y", "x"}, set(ds.coords))
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "title": "Test 1-3",
            },
            ds.attrs,
        )

    def test_some_slices_local(self):
        target_dir = "target.zarr"
        slices = [
            "slice-1.zarr",
            "slice-2.zarr",
            "slice-3.zarr",
        ]
        for index, uri in enumerate(slices):
            make_test_dataset(uri=uri, index=3 * index)
        try:
            zappend(slices, target_dir=target_dir)
            ds = xr.open_zarr(target_dir)
            self.assertEqual({"time": 9, "y": 50, "x": 100}, ds.sizes)
            self.assertEqual({"chl", "tsm"}, set(ds.data_vars))
            self.assertEqual({"time", "y", "x"}, set(ds.coords))
            self.assertEqual(
                {
                    "Conventions": "CF-1.8",
                    "title": "Test 1-3",
                },
                ds.attrs,
            )
        finally:
            shutil.rmtree(target_dir, ignore_errors=True)
            for slice_dir in slices:
                shutil.rmtree(slice_dir, ignore_errors=True)

    def test_some_slices_local_output_to_non_existing_dir(self):
        target_dir = "non_existent_dir/target.zarr"
        slices = [
            "slice-1.zarr",
            "slice-2.zarr",
            "slice-3.zarr",
        ]
        for uri in slices:
            make_test_dataset(uri=uri)
        try:
            with pytest.raises(
                FileNotFoundError,
                match=(
                    "\\ATarget parent directory does not exist: .*/non_existent_dir\\Z"
                ),
            ):
                zappend(slices, target_dir=target_dir)
        finally:
            shutil.rmtree(target_dir, ignore_errors=True)
            for slice_dir in slices:
                shutil.rmtree(slice_dir, ignore_errors=True)

    def test_some_slices_local_output_to_existing_dir_force_new(self):
        target_dir = "memory://target.zarr"
        slices = [
            "memory://slice-0.zarr",
            "memory://slice-1.zarr",
            "memory://slice-2.zarr",
            "memory://slice-3.zarr",
        ]
        for uri in slices:
            make_test_dataset(uri=uri)

        # Expect nothing else to happen, even though force_new=True.
        zappend(slices[:1], target_dir=target_dir, force_new=True)
        target_ds = xr.open_zarr(target_dir)
        self.assertEqual({"time": 3, "y": 50, "x": 100}, target_ds.sizes)

        # Expect deletion of existing target_dir
        zappend(slices[1:], target_dir=target_dir, force_new=True)
        target_ds = xr.open_zarr(target_dir)
        self.assertEqual({"time": 9, "y": 50, "x": 100}, target_ds.sizes)

        # Expect no changes, even if force_new=True, because dry_run=True
        zappend(slices, target_dir=target_dir, force_new=True, dry_run=True)
        target_ds = xr.open_zarr(target_dir)
        self.assertEqual({"time": 9, "y": 50, "x": 100}, target_ds.sizes)

        # Expect the lock file to be deleted too
        lock_file = Transaction.get_lock_file(FileObj(target_dir))
        lock_file.write("")
        self.assertEqual(True, lock_file.exists())
        zappend(slices, target_dir=target_dir, force_new=True)
        self.assertEqual(False, lock_file.exists())

    def test_some_slices_with_slice_source_class(self):
        class DropTsm(SliceSource):
            def __init__(self, slice_ds):
                self.slice_ds = slice_ds

            def get_dataset(self) -> xr.Dataset:
                return self.slice_ds.drop_vars(["tsm"])

            def dispose(self):
                pass

        target_dir = "memory://target.zarr"
        slices = [make_test_dataset(index=3 * i) for i in range(3)]
        zappend(slices, target_dir=target_dir, slice_source=DropTsm)
        ds = xr.open_zarr(target_dir)
        self.assertEqual({"time": 9, "y": 50, "x": 100}, ds.sizes)
        self.assertEqual({"chl"}, set(ds.data_vars))
        self.assertEqual({"time", "y", "x"}, set(ds.coords))
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "title": "Test 1-3",
            },
            ds.attrs,
        )

    def test_some_slices_with_slice_source_func(self):
        def drop_tsm(slice_ds: xr.Dataset) -> xr.Dataset:
            return slice_ds.drop_vars(["tsm"])

        target_dir = "memory://target.zarr"
        slices = [make_test_dataset(index=3 * i) for i in range(3)]
        zappend(slices, target_dir=target_dir, slice_source=drop_tsm)
        ds = xr.open_zarr(target_dir)
        self.assertEqual({"time": 9, "y": 50, "x": 100}, ds.sizes)
        self.assertEqual({"chl"}, set(ds.data_vars))
        self.assertEqual({"time", "y", "x"}, set(ds.coords))
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "title": "Test 1-3",
            },
            ds.attrs,
        )

    # See https://github.com/bcdev/zappend/issues/77
    def test_some_slices_with_cropping_slice_source_no_chunks_spec(self):
        def crop_ds(slice_ds: xr.Dataset) -> xr.Dataset:
            w = slice_ds.x.size
            h = slice_ds.y.size
            return slice_ds.isel(x=slice(5, w - 5), y=slice(5, h - 5))

        target_dir = "memory://target.zarr"
        slices = [make_test_dataset(index=3 * i) for i in range(3)]
        zappend(slices, target_dir=target_dir, slice_source=crop_ds)
        ds = xr.open_zarr(target_dir)
        self.assertEqual({"time": 9, "y": 40, "x": 90}, ds.sizes)
        self.assertEqual({"chl", "tsm"}, set(ds.data_vars))
        self.assertEqual({"time", "y", "x"}, set(ds.coords))
        self.assertEqual((90,), ds.x.encoding.get("chunks"))
        self.assertEqual((40,), ds.y.encoding.get("chunks"))
        self.assertEqual((3,), ds.time.encoding.get("chunks"))
        # Chunk sizes are the ones of the original array, because we have not
        # specified chunks in encoding.
        self.assertEqual((1, 25, 45), ds.chl.encoding.get("chunks"))
        self.assertEqual((1, 25, 45), ds.tsm.encoding.get("chunks"))

    # See https://github.com/bcdev/zappend/issues/77
    def test_some_slices_with_cropping_slice_source_with_chunks_spec(self):
        def crop_ds(slice_ds: xr.Dataset) -> xr.Dataset:
            w = slice_ds.x.size
            h = slice_ds.y.size
            return slice_ds.isel(x=slice(5, w - 5), y=slice(5, h - 5))

        variables = {
            "*": {
                "encoding": {
                    "chunks": None,
                }
            },
            "chl": {
                "encoding": {
                    "chunks": [1, None, None],
                }
            },
            "tsm": {
                "encoding": {
                    "chunks": [None, 25, 50],
                }
            },
        }

        target_dir = "memory://target.zarr"
        slices = [make_test_dataset(index=3 * i) for i in range(3)]
        zappend(
            slices, target_dir=target_dir, slice_source=crop_ds, variables=variables
        )
        ds = xr.open_zarr(target_dir)
        self.assertEqual({"time": 9, "y": 40, "x": 90}, ds.sizes)
        self.assertEqual({"chl", "tsm"}, set(ds.data_vars))
        self.assertEqual({"time", "y", "x"}, set(ds.coords))
        self.assertEqual((90,), ds.x.encoding.get("chunks"))
        self.assertEqual((40,), ds.y.encoding.get("chunks"))
        self.assertEqual((3,), ds.time.encoding.get("chunks"))
        self.assertEqual((1, 40, 90), ds.chl.encoding.get("chunks"))
        self.assertEqual((3, 25, 50), ds.tsm.encoding.get("chunks"))

    def test_some_slices_with_inc_append_step(self):
        target_dir = "memory://target.zarr"
        slices = [make_test_dataset(index=i, shape=(1, 50, 100)) for i in range(3)]
        zappend(slices, target_dir=target_dir, append_step="1D")
        ds = xr.open_zarr(target_dir)
        np.testing.assert_array_equal(
            ds.time.values,
            np.array(["2024-01-01", "2024-01-02", "2024-01-03"], dtype=np.datetime64),
        )
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "title": "Test 1-1",
            },
            ds.attrs,
        )

    def test_some_slices_with_dec_append_step(self):
        target_dir = "memory://target.zarr"
        slices = [
            make_test_dataset(index=i, shape=(1, 50, 100)) for i in reversed(range(3))
        ]
        zappend(slices, target_dir=target_dir, append_step="-1D")
        ds = xr.open_zarr(target_dir)
        np.testing.assert_array_equal(
            ds.time.values,
            np.array(["2024-01-03", "2024-01-02", "2024-01-01"], dtype=np.datetime64),
        )
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "title": "Test 3-3",
            },
            ds.attrs,
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

    def test_dynamic_attrs_with_one_slice(self):
        target_dir = "memory://target.zarr"
        slices = [make_test_dataset()]
        zappend(
            slices,
            target_dir=target_dir,
            permit_eval=True,
            attrs={
                "title": "HROC Ocean Colour Monthly Composite",
                "time_coverage_start": "{{ ds.time[0] }}",
                "time_coverage_end": "{{ ds.time[-1] }}",
            },
        )
        ds = xr.open_zarr(target_dir)
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "title": "HROC Ocean Colour Monthly Composite",
                "time_coverage_start": np.datetime_as_string(ds.time[0], unit="s"),
                "time_coverage_end": np.datetime_as_string(ds.time[-1], unit="s"),
            },
            ds.attrs,
        )

    def test_dynamic_attrs_with_some_slices(self):
        target_dir = "memory://target.zarr"
        slices = [make_test_dataset(index=i) for i in range(3)]
        zappend(
            slices,
            target_dir=target_dir,
            permit_eval=True,
            attrs={
                "title": "HROC Ocean Colour Monthly Composite",
                "time_coverage_start": "{{ ds.time[0] }}",
                "time_coverage_end": "{{ ds.time[-1] }}",
            },
        )
        ds = xr.open_zarr(target_dir)
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "title": "HROC Ocean Colour Monthly Composite",
                "time_coverage_start": np.datetime_as_string(ds.time[0], unit="s"),
                "time_coverage_end": np.datetime_as_string(ds.time[-1], unit="s"),
            },
            ds.attrs,
        )

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
