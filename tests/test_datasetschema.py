# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
import numpy as np
import xarray as xr
from zappend.datasetschema import DatasetSchema


class DatasetSchemaTest(unittest.TestCase):
    def test_from_dataset(self):
        shape = 50, 100, 200
        ds = xr.Dataset(
            data_vars=dict(
                chl=xr.DataArray(np.zeros(shape, dtype="uint16"),
                                 dims=("time", "y", "x")),
                tsm=xr.DataArray(np.zeros(shape, dtype="uint16"),
                                 dims=("time", "y", "x")),
            ),
            coords=dict(
                time=xr.DataArray(np.arange(shape[0], dtype="uint64"),
                                  dims="time"),
                y=xr.DataArray(np.linspace(0, 1, shape[1], dtype="float64"),
                               dims="y"),
                x=xr.DataArray(np.linspace(0, 1, shape[2], dtype="float64"),
                               dims="x"),
            )
        )

        schema = DatasetSchema.from_dataset(ds)
        self.assertIsInstance(schema, DatasetSchema)
        self.assertEqual({'time': 50, 'y': 100, 'x': 200}, schema.dims)
        self.assertEqual({'chl', 'tsm', 'time', 'y', 'x'},
                         set(schema.variables.keys()))
