# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
import math
import unittest

import numcodecs
import numpy as np
import pytest
import xarray as xr

from zappend.metadata import get_effective_target_dims
from zappend.metadata import get_effective_variables


class GetEffectiveTargetDimsTest(unittest.TestCase):

    def test_without_fixed_sizes(self):
        self.assertEqual(
            {"z": 2, "y": 3, "x": 4},
            get_effective_target_dims(
                None,
                "z",
                xr.Dataset({
                    "a": xr.DataArray(np.zeros((2, 3, 4)),
                                      dims=("z", "y", "x"))
                })
            )
        )

    def test_with_fixed_sizes(self):
        self.assertEqual(
            {"z": 2, "y": 3, "x": 4},
            get_effective_target_dims(
                {"y": 3, "x": 4},
                "z",
                xr.Dataset({
                    "a": xr.DataArray(np.zeros((2, 3, 4)),
                                      dims=("z", "y", "x"))
                })
            )
        )

    # noinspection PyMethodMayBeStatic
    def test_raise_append_dim_not_found(self):
        with pytest.raises(ValueError,
                           match="Append dimension 'z' not found in dataset"):
            get_effective_target_dims(
                None,
                "z",
                xr.Dataset({
                    "a": xr.DataArray(np.zeros((2, 3, 4)),
                                      dims=("time", "y", "x"))
                })
            )

    # noinspection PyMethodMayBeStatic
    def test_raise_append_dim_must_not_be_fixed(self):
        with pytest.raises(ValueError,
                           match="Size of append dimension 'time'"
                                 " must not be fixed"):
            get_effective_target_dims(
                {"time": 2, "y": 3, "x": 4},
                "time",
                xr.Dataset({
                    "a": xr.DataArray(np.zeros((2, 3, 4)),
                                      dims=("time", "y", "x"))
                })
            )

    # noinspection PyMethodMayBeStatic
    def test_raise_fixed_dim_not_found_in_ds(self):
        with pytest.raises(ValueError,
                           match="Fixed dimension 'z' not found in dataset"):
            get_effective_target_dims(
                {"y": 3, "z": 4},
                "time",
                xr.Dataset({
                    "a": xr.DataArray(np.zeros((2, 3, 4)),
                                      dims=("time", "y", "x"))
                })
            )

    # noinspection PyMethodMayBeStatic
    def test_raise_wrong_size_found_in_ds(self):
        with pytest.raises(ValueError,
                           match="Wrong size for fixed dimension 'x'"
                                 " in dataset: expected 5, found 4"):
            get_effective_target_dims(
                {"y": 3, "x": 5},
                "time",
                xr.Dataset({
                    "a": xr.DataArray(np.zeros((2, 3, 4)),
                                      dims=("time", "y", "x"))
                })
            )


class GetEffectiveVariablesTest(unittest.TestCase):

    def test_add_missing(self):
        self.assertEqual(
            {
                "a": {"dims": ["time", "y", "x"], "encoding": {}, "attrs": {}},
                "b": {"dims": ["time", "y", "x"], "encoding": {}, "attrs": {}},
            },
            get_effective_variables(
                {
                    "a": {"dims": ["time", "y", "x"]}
                },
                xr.Dataset({
                    "a": xr.DataArray(np.zeros((2, 3, 4)),
                                      dims=("time", "y", "x")),
                    "b": xr.DataArray(np.zeros((2, 3, 4)),
                                      dims=("time", "y", "x")),
                })
            )
        )

    def test_merge_metadata(self):
        config_vars = {
            "a": {"encoding": {"dtype": "uint16"},
                  "attrs": {"title": "A"}},
            "b": {"encoding": {"dtype": "int32"},
                  "attrs": {"title": "B"}},
        }
        ds = xr.Dataset({
            "a": xr.DataArray(np.zeros((2, 3, 4)),
                              dims=("time", "y", "x"),
                              attrs={"units": "m/s"}),
            "b": xr.DataArray(np.zeros((2, 3, 4)),
                              dims=("time", "y", "x"),
                              attrs={"units": "m/s^2"}),
        })
        ds.a.encoding.update(scale_factor=0.001)
        ds.b.encoding.update(add_offset=1.0)
        self.assertEqual(
            {
                "a": {"dims": ["time", "y", "x"],
                      "encoding": {"dtype": np.dtype("uint16"),
                                   "scale_factor": 0.001},
                      "attrs": {"title": "A", "units": "m/s"}},
                "b": {"dims": ["time", "y", "x"],
                      "encoding": {"dtype": np.dtype("int32"),
                                   "add_offset": 1.0},
                      "attrs": {"title": "B", "units": "m/s^2"}},
            },
            get_effective_variables(config_vars, ds)
        )

    def test_move_encoding_from_attrs(self):
        self.assertEqual(
            {
                "a": {"dims": ["time", "y", "x"],
                      "encoding": {"_FillValue": 999},
                      "attrs": {}},
                "b": {"dims": ["time", "y", "x"],
                      "encoding": {"_FillValue": -1},
                      "attrs": {}},
            },
            get_effective_variables(
                {
                    "a": {"dims": ["time", "y", "x"]},
                    "b": {"dims": ["time", "y", "x"],
                          "attrs": {"_FillValue": -1}},
                },
                xr.Dataset({
                    "a": xr.DataArray(np.zeros((2, 3, 4)),
                                      dims=("time", "y", "x"),
                                      attrs={"_FillValue": 999}),
                    "b": xr.DataArray(np.zeros((2, 3, 4)),
                                      dims=("time", "y", "x")),
                })
            )
        )

    def test_encoding_normalisation(self):
        def normalize(k, v):
            variables = get_effective_variables(
                {
                    "a": {"encoding": {k: v}}
                },
                xr.Dataset(
                    {"a": xr.DataArray(np.zeros((2, 3, 4)),
                                       dims=("time", "y", "x")), }
                )
            )
            return variables["a"]["encoding"][k]

        self.assertEqual(np.dtype("int32"), normalize("dtype", "int32"))
        dtype = np.dtype("int32")
        self.assertIs(dtype, normalize("dtype", dtype))
        self.assertIs(None, normalize("chunks", None))
        self.assertIs(None, normalize("chunks", ()))
        self.assertEqual((1, 2, 3), normalize("chunks", [1, 2, 3]))
        self.assertEqual((1, 3), normalize("chunks", ((1, 1, 1), (3, 3, 2))))
        self.assertTrue(math.isnan(normalize("fill_value", "NaN")))
        self.assertEqual(3.0, normalize("add_offset", "3.0"))
        self.assertEqual(0.01, normalize("scale_factor", 0.01))
        self.assertIs(None, normalize("compressor", None))
        self.assertIs(None, normalize("compressor", {}))
        self.assertIsInstance(normalize("compressor",
                                        {
                                            'id': 'blosc',
                                            'cname': 'lz4',
                                            'clevel': 5,
                                            'blocksize': 0,
                                            'shuffle': 1,
                                        }),
                              numcodecs.Blosc)
        compressor = numcodecs.Blosc()
        self.assertIs(compressor, normalize("compressor", compressor))
        self.assertIs(None, normalize("filters", None))
        self.assertIs(None, normalize("filters", []))
        filters = normalize("filters",
                            [{"id": "delta", "dtype": "int8"},
                             {"id": "delta", "dtype": "int16"}])
        self.assertIsInstance(filters, list)
        self.assertEqual(2, len(filters))
        self.assertIsInstance(filters[0], numcodecs.Delta)
        self.assertIsInstance(filters[1], numcodecs.Delta)

    # noinspection PyMethodMayBeStatic
    def test_raise_wrong_size_found_in_ds(self):
        with pytest.raises(ValueError,
                           match="Dimension mismatch for variable 'a':"
                                 " expected \\['z', 'y', 'x'\\],"
                                 " got \\['time', 'y', 'x'\\]"):
            get_effective_variables(
                {
                    "a": {"dims": ["z", "y", "x"]}
                },
                xr.Dataset({
                    "a": xr.DataArray(np.zeros((2, 3, 4)),
                                      dims=("time", "y", "x")),
                })
            )
