# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import numpy as np
import pyproj
import xarray as xr

from zappend.metadata import DatasetMetadata
from zappend.tailoring import tailor_target_dataset
from zappend.tailoring import tailor_slice_dataset


class TailorTargetDatasetTest(unittest.TestCase):
    def test_it_sets_metadata(self):
        ds = xr.Dataset(
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
        tailored_ds = tailor_target_dataset(
            ds,
            DatasetMetadata.from_dataset(
                ds,
                {
                    "variables": {
                        "a": {"encoding": {"dtype": "uint8", "fill_value": 0}},
                        "b": {"encoding": {"dtype": "int8", "fill_value": -1}},
                    }
                },
            ),
        )
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
        ds = xr.Dataset(
            {
                "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                "b": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
            }
        )

        tailored_ds = tailor_target_dataset(
            ds, DatasetMetadata.from_dataset(ds, {"included_variables": ["b"]})
        )
        self.assertEqual({"b"}, set(tailored_ds.variables.keys()))

        tailored_ds = tailor_target_dataset(
            ds, DatasetMetadata.from_dataset(ds, {"excluded_variables": ["b"]})
        )
        self.assertEqual({"a"}, set(tailored_ds.variables.keys()))

    def test_it_completes_vars(self):
        ds = xr.Dataset(
            {
                "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
            }
        )

        tailored_ds = tailor_target_dataset(
            ds,
            DatasetMetadata.from_dataset(
                ds,
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
            ),
        )
        self.assertEqual({"a", "b", "c"}, set(tailored_ds.variables.keys()))

        b = tailored_ds.b
        self.assertEqual(np.dtype("float64"), b.dtype)
        self.assertEqual(np.dtype("int16"), b.encoding.get("dtype"))

        c = tailored_ds.c
        self.assertEqual(np.dtype("uint32"), c.dtype)
        self.assertEqual(np.dtype("uint32"), c.encoding.get("dtype"))

    # noinspection PyMethodMayBeStatic


class TailorSliceDatasetTest(unittest.TestCase):
    def test_it_drops_constant_variables(self):
        ds = xr.Dataset(
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
        tailored_ds = tailor_slice_dataset(
            ds, DatasetMetadata.from_dataset(ds, {}), "time"
        )
        self.assertIsInstance(tailored_ds, xr.Dataset)
        self.assertEqual({"a", "b"}, set(tailored_ds.variables.keys()))

    def test_it_clears_encoding_and_attrs(self):
        ds = xr.Dataset(
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
        tailored_ds = tailor_slice_dataset(
            ds,
            DatasetMetadata.from_dataset(
                ds,
                {
                    "variables": {
                        "a": {"encoding": {"dtype": "uint8", "fill_value": 0}},
                        "b": {"encoding": {"dtype": "int8", "fill_value": -1}},
                    }
                },
            ),
            "time",
        )
        self.assertIsInstance(tailored_ds, xr.Dataset)

        self.assertIn("a", tailored_ds.variables)
        a = tailored_ds.a
        self.assertEqual({}, a.encoding)
        self.assertEqual({}, a.attrs)

        self.assertIn("b", tailored_ds.variables)
        b = tailored_ds.b
        self.assertEqual({}, b.encoding)
        self.assertEqual({}, b.attrs)
