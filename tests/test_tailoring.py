# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import numpy as np
import pyproj
import xarray as xr

from zappend.context import Context
from zappend.tailoring import tailor_target_dataset
from zappend.tailoring import tailor_slice_dataset
from .helpers import clear_memory_fs


def make_context(config: dict, target_ds: xr.Dataset, write: bool = False) -> Context:
    target_dir = "memory://target.zarr"
    if write:
        target_ds.to_zarr(target_dir, mode="w")
    ctx = Context({"target_dir": target_dir, **config})
    ctx.target_metadata = ctx.get_dataset_metadata(target_ds)
    return ctx


class TailorTargetDatasetTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_it_sets_vars_metadata(self):
        slice_ds = xr.Dataset(
            {
                "a": xr.DataArray(
                    np.zeros((2, 3, 4)),
                    dims=("time", "y", "x"),
                    attrs={"units": "mg/m^3", "scale_factor": 0.025},
                ),
                "b": xr.DataArray(
                    np.zeros((2, 3, 4)),
                    dims=("time", "y", "x"),
                    attrs={"units": "g/m^3", "scale_factor": 0.03},
                ),
            }
        )
        ctx = make_context(
            {
                "variables": {
                    "a": {"encoding": {"dtype": "uint8", "fill_value": 0}},
                    "b": {"encoding": {"dtype": "int8", "fill_value": -1}},
                },
            },
            slice_ds,
        )

        tailored_ds = tailor_target_dataset(ctx, slice_ds)
        self.assertIsInstance(tailored_ds, xr.Dataset)
        self.assertEqual({"a", "b"}, set(tailored_ds.variables.keys()))

        a = tailored_ds.a
        self.assertEqual(np.dtype("uint8"), a.encoding.get("dtype"))
        self.assertEqual(0, a.encoding.get("_FillValue"))
        self.assertEqual(0.025, a.encoding.get("scale_factor"))
        self.assertEqual({"units": "mg/m^3"}, a.attrs)

        b = tailored_ds.b
        self.assertEqual(np.dtype("int8"), b.encoding.get("dtype"))
        self.assertEqual(-1, b.encoding.get("_FillValue"))
        self.assertEqual(0.03, b.encoding.get("scale_factor"))
        self.assertEqual({"units": "g/m^3"}, b.attrs)

    def test_it_strips_vars(self):
        slice_ds = xr.Dataset(
            {
                "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                "b": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
            }
        )

        ctx = make_context({"included_variables": ["b"]}, slice_ds)
        tailored_ds = tailor_target_dataset(ctx, slice_ds)
        self.assertEqual({"b"}, set(tailored_ds.variables.keys()))

        ctx = make_context({"excluded_variables": ["b"]}, slice_ds)
        tailored_ds = tailor_target_dataset(ctx, slice_ds)
        self.assertEqual({"a"}, set(tailored_ds.variables.keys()))

    def test_it_completes_vars(self):
        slice_ds = xr.Dataset(
            {
                "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
            }
        )
        ctx = make_context(
            {
                "variables": {
                    "a": {"dims": ["time", "y", "x"]},
                    "b": {
                        "dims": ["time", "y", "x"],
                        "encoding": {"dtype": "int16", "fill_value": 0},
                    },
                    "c": {
                        "dims": ["time", "y", "x"],
                        "encoding": {"dtype": "uint32"},
                    },
                },
            },
            slice_ds,
        )

        tailored_ds = tailor_target_dataset(ctx, slice_ds)
        self.assertEqual({"a", "b", "c"}, set(tailored_ds.variables.keys()))

        b = tailored_ds.b
        self.assertEqual(np.dtype("float64"), b.dtype)
        self.assertEqual(np.dtype("int16"), b.encoding.get("dtype"))

        c = tailored_ds.c
        self.assertEqual(np.dtype("uint32"), c.dtype)
        self.assertEqual(np.dtype("uint32"), c.encoding.get("dtype"))

    def test_it_updates_attrs_according_to_update_mode(self):
        target_ds = xr.Dataset(
            {
                "a": xr.DataArray(
                    np.zeros((2, 3, 4)),
                    dims=("time", "y", "x"),
                ),
            },
            attrs={"Conventions": "CF-1.8"},
        )

        ctx = make_context(
            {"attrs_update_mode": "keep", "attrs": {"a": 12, "b": True}}, target_ds
        )
        tailored_ds = tailor_target_dataset(ctx, target_ds)
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "a": 12,
                "b": True,
            },
            tailored_ds.attrs,
        )

        ctx = make_context(
            {"attrs_update_mode": "replace", "attrs": {"a": 12, "b": True}}, target_ds
        )
        tailored_ds = tailor_target_dataset(ctx, target_ds)
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "a": 12,
                "b": True,
            },
            tailored_ds.attrs,
        )

        ctx = make_context(
            {"attrs_update_mode": "update", "attrs": {"a": 12, "b": True}}, target_ds
        )
        tailored_ds = tailor_target_dataset(ctx, target_ds)
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "a": 12,
                "b": True,
            },
            tailored_ds.attrs,
        )

        ctx = make_context(
            {"attrs_update_mode": "ignore", "attrs": {"a": 12, "b": True}}, target_ds
        )
        tailored_ds = tailor_target_dataset(ctx, target_ds)
        self.assertEqual(
            {
                "a": 12,
                "b": True,
            },
            tailored_ds.attrs,
        )


class TailorSliceDatasetTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_it_drops_constant_variables(self):
        slice_ds = xr.Dataset(
            {
                "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                "b": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                "spatial_ref": xr.DataArray(
                    np.array(0), attrs=pyproj.CRS("EPSG:4326").to_cf()
                ),
            },
            coords={
                "x": xr.DataArray(np.linspace(0.0, 1.0, 4), dims="x"),
                "y": xr.DataArray(np.linspace(0.0, 1.0, 3), dims="y"),
            },
        )
        ctx = make_context({}, slice_ds)
        tailored_ds = tailor_slice_dataset(ctx, slice_ds)
        self.assertIsInstance(tailored_ds, xr.Dataset)
        self.assertEqual({"a", "b"}, set(tailored_ds.variables.keys()))

    def test_it_clears_var_encoding_and_attrs(self):
        slice_ds = xr.Dataset(
            {
                "a": xr.DataArray(
                    np.zeros((2, 3, 4)),
                    dims=("time", "y", "x"),
                    attrs={"units": "mg/m^3", "scale_factor": 0.025},
                ),
                "b": xr.DataArray(
                    np.zeros((2, 3, 4)),
                    dims=("time", "y", "x"),
                    attrs={"units": "g/m^3", "scale_factor": 0.03},
                ),
            }
        )
        ctx = make_context(
            {
                "variables": {
                    "a": {"encoding": {"dtype": "uint8", "fill_value": 0}},
                    "b": {"encoding": {"dtype": "int8", "fill_value": -1}},
                }
            },
            slice_ds,
        )
        tailored_ds = tailor_slice_dataset(ctx, slice_ds)
        self.assertIsInstance(tailored_ds, xr.Dataset)

        self.assertIn("a", tailored_ds.variables)
        a = tailored_ds.a
        self.assertEqual({}, a.encoding)
        self.assertEqual({}, a.attrs)

        self.assertIn("b", tailored_ds.variables)
        b = tailored_ds.b
        self.assertEqual({}, b.encoding)
        self.assertEqual({}, b.attrs)

    def test_it_updates_attrs_according_to_update_mode(self):
        target_ds = xr.Dataset(
            {
                "a": xr.DataArray(
                    np.zeros((2, 3, 4)),
                    dims=("time", "y", "x"),
                ),
            },
            attrs={"Conventions": "CF-1.8"},
        )
        slice_ds = xr.Dataset(
            {
                "a": xr.DataArray(
                    np.zeros((2, 3, 4)),
                    dims=("time", "y", "x"),
                ),
            },
            attrs={"title": "OCC 2024"},
        )

        ctx = make_context(
            {"attrs_update_mode": "keep", "attrs": {"a": 12, "b": True}},
            target_ds,
            True,
        )
        tailored_ds = tailor_slice_dataset(ctx, slice_ds)
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "a": 12,
                "b": True,
            },
            tailored_ds.attrs,
        )

        ctx = make_context(
            {"attrs_update_mode": "replace", "attrs": {"a": 12, "b": True}},
            target_ds,
            True,
        )
        tailored_ds = tailor_slice_dataset(ctx, slice_ds)
        self.assertEqual(
            {
                "title": "OCC 2024",
                "a": 12,
                "b": True,
            },
            tailored_ds.attrs,
        )

        ctx = make_context(
            {"attrs_update_mode": "update", "attrs": {"a": 12, "b": True}},
            target_ds,
            True,
        )
        tailored_ds = tailor_slice_dataset(ctx, slice_ds)
        self.assertEqual(
            {
                "Conventions": "CF-1.8",
                "title": "OCC 2024",
                "a": 12,
                "b": True,
            },
            tailored_ds.attrs,
        )

        ctx = make_context(
            {"attrs_update_mode": "ignore", "attrs": {"a": 12, "b": True}},
            target_ds,
            True,
        )
        tailored_ds = tailor_slice_dataset(ctx, slice_ds)
        self.assertEqual(
            {
                "a": 12,
                "b": True,
            },
            tailored_ds.attrs,
        )
