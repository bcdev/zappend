# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
from typing import Any

import fsspec
import numpy as np
import xarray as xr

from zappend.config import ZARR_V2_DEFAULT_COMPRESSOR
from zappend.outline import DatasetOutline
from zappend.outline import VariableOutline
# noinspection PyProtectedMember
from zappend.outline._helpers import to_comparable_value


class DatasetSchemaTest(unittest.TestCase):
    def test_from_config(self):
        config = make_test_config()
        schema = DatasetOutline.from_config(config)
        self.assertDatasetSchemaOk(schema)

    def test_from_dataset(self):
        ds = make_test_dataset()
        schema = DatasetOutline.from_dataset(ds)
        self.assertDatasetSchemaOk(schema)

    def assertDatasetSchemaOk(self, schema: DatasetOutline):
        self.assertIsInstance(schema, DatasetOutline)
        dims = dict(schema.dims)
        self.assertIn("time", dims)
        del dims["time"]
        self.assertEqual({'y': 100, 'x': 200}, dims)
        self.assertEqual({'chl', 'tsm', 'time', 'y', 'x'},
                         set(schema.variables.keys()))
        self.assertEqualVariableSchema(
            VariableOutline(dtype="uint16",
                            dims=("time", "y", "x"),
                            shape=(50, 100, 200),
                            chunks=(1, 30, 40),
                            fill_value=9999,
                            scale_factor=0.2,
                            add_offset=0,
                            compressor=ZARR_V2_DEFAULT_COMPRESSOR,
                            filters=None),
            schema.variables['chl']
        )
        self.assertEqualVariableSchema(
            VariableOutline(dtype="int16",
                            dims=("time", "y", "x"),
                            shape=(50, 100, 200),
                            chunks=(1, 30, 40),
                            fill_value=-9999,
                            scale_factor=0.01,
                            add_offset=-200,
                            compressor=ZARR_V2_DEFAULT_COMPRESSOR,
                            filters=None),
            schema.variables['tsm']
        )
        self.assertEqualVariableSchema(
            VariableOutline(dtype="float64",
                            dims=("x",),
                            shape=(200,),
                            chunks=(200,),
                            fill_value=float("NaN"),
                            scale_factor=None,
                            add_offset=None,
                            compressor=ZARR_V2_DEFAULT_COMPRESSOR,
                            filters=None),
            schema.variables['x']
        )

    def assertEqualVariableSchema(self,
                                  expected_schema: VariableOutline,
                                  actual_schema: VariableOutline):
        for attr_name, expected_value in expected_schema.__dict__.items():
            expected_value = to_comparable_value(expected_value)
            actual_value = to_comparable_value(
                getattr(actual_schema, attr_name)
            )
            self.assertEqual(expected_value, actual_value, msg=attr_name)

    def test_get_noncompliance(self):
        schema_1 = DatasetOutline.from_dataset(make_test_dataset())
        self.assertEqual([], schema_1.get_noncompliance(schema_1))
        schema_2 = DatasetOutline.from_config(make_test_config())
        self.assertEqual([], schema_2.get_noncompliance(schema_1))


def make_test_dataset(shape=(50, 100, 200), raw=False):
    ds = xr.Dataset(
        data_vars=dict(
            chl=xr.DataArray(np.zeros(shape, dtype="uint16"),
                             dims=("time", "y", "x"),
                             attrs=dict(scale_factor=0.2,
                                        add_offset=0,
                                        _FillValue=9999)),
            tsm=xr.DataArray(np.zeros(shape, dtype="int16"),
                             dims=("time", "y", "x"),
                             attrs=dict(scale_factor=0.01,
                                        add_offset=-200,
                                        _FillValue=-9999)),
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

    ds = ds.chunk(dict(time=1, y=30, x=40))
    # ds.chl.encoding.update(fill_value=9999)
    # ds.tsm.encoding.update(fill_value=-9999)

    if raw:
        return ds

    mem_fs: fsspec.AbstractFileSystem = fsspec.filesystem("memory")
    if mem_fs.exists("/test.zarr"):
        mem_fs.rm("/test.zarr", recursive=True)
    ds.to_zarr("memory://test.zarr",
               zarr_version=2,
               # encoding=dict(chl=dict(fill_value=9999),
               #               tsm=dict(fill_value=-9999)),
               write_empty_chunks=False)
    return xr.open_zarr("memory://test.zarr", decode_cf=False)


def make_test_config(shape=(50, 100, 200)) -> dict[str, Any]:
    return dict(
        fixed_dims=dict(x=200, y=100),
        append_dim="time",
        variables=dict(
            chl=dict(
                dtype="uint16",
                shape=shape,
                chunks=(1, 30, 40),
                dims=("time", "y", "x"),
                scale_factor=0.2,
                add_offset=0,
                fill_value=9999
            ),
            tsm=dict(
                dtype="int16",
                shape=shape,
                chunks=(1, 30, 40),
                dims=("time", "y", "x"),
                scale_factor=0.01,
                add_offset=-200,
                fill_value=-9999
            ),
            time=dict(
                dtype="uint64",
                shape=shape[0],
                dims="time"
            ),
            y=dict(
                dtype="float64",
                shape=shape[1],
                dims="y"
            ),
            x=dict(
                dtype="float64",
                shape=shape[2],
                dims="x"
            ),
        )
    )
