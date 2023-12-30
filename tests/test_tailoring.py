# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import numpy as np
import pytest
import xarray as xr

from zappend.tailoring import tailor_target_dataset
from zappend.tailoring import tailor_slice_dataset


class TailorTargetDatasetTest(unittest.TestCase):

    def test_it_sets_metadata(self):
        # TODO: implement
        tailored_ds = tailor_target_dataset(xr.Dataset(), set(), set(), {}, {})
        self.assertIsInstance(tailored_ds, xr.Dataset)

    def test_it_strips_vars(self):
        ds = xr.Dataset({
            "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
            "b": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
        })

        tailored_ds = tailor_target_dataset(ds, {"b"}, set(), {}, {})
        self.assertEqual(
            {"b"},
            set(tailored_ds.variables.keys())
        )

        tailored_ds = tailor_target_dataset(ds, set(), {"b"}, {}, {})
        self.assertEqual(
            {"a"},
            set(tailored_ds.variables.keys())
        )

    def test_it_completes_vars(self):
        ds = xr.Dataset({
            "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
        })

        tailored_ds = tailor_target_dataset(
            ds,
            set(), set(),
            {
                "a": {"dims": ["time", "y", "x"]},
                "b": {"dims": ["time", "y", "x"]},
            },
            {})
        self.assertEqual(
            {"a", "b"},
            set(tailored_ds.variables.keys())
        )

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_missing_dims(self):
        ds = xr.Dataset({
            "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
        })

        with pytest.raises(ValueError,
                           match="Cannot create variable 'b' because its dimensions are not specified"):
            tailor_target_dataset(
                ds,
                set(), set(),
                {
                    "a": {"dims": ["time", "y", "x"]},
                    "b": {},
                },
                {}
            )

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_wrong_dims(self):
        ds = xr.Dataset({
            "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
        })

        with pytest.raises(ValueError,
                           match="Cannot create variable 'b' because at least"
                                 " one of its dimensions \\['time', 'Y', 'x'\\]"
                                 " does not exist in the dataset"):
            tailor_target_dataset(
                ds,
                set(), set(),
                {
                    "a": {"dims": ["time", "y", "x"]},
                    "b": {"dims": ["time", "Y", "x"]},
                },
                {}
            )


class TailorSliceDatasetTest(unittest.TestCase):

    def test_it_sets_metadata(self):
        # TODO: implement
        tailored_ds = tailor_slice_dataset(xr.Dataset(), set(), set(), {})
        self.assertIsInstance(tailored_ds, xr.Dataset)
