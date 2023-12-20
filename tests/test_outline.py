# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

from zappend.config import ZARR_V2_DEFAULT_COMPRESSOR
from zappend.outline import DatasetOutline
from zappend.outline import VariableOutline
# noinspection PyProtectedMember
from zappend.outline._helpers import to_comparable_value
from .helpers import make_test_config
from .helpers import make_test_dataset


class DatasetOutlineTest(unittest.TestCase):
    def test_from_config(self):
        config = make_test_config()
        schema = DatasetOutline.from_config(config)
        self.assertDatasetSchemaOk(schema)

    def test_from_dataset(self):
        ds = make_test_dataset(uri="memory://test.zarr")
        schema = DatasetOutline.from_dataset(ds)
        self.assertDatasetSchemaOk(schema)

    def assertDatasetSchemaOk(self, schema: DatasetOutline):
        self.assertIsInstance(schema, DatasetOutline)
        dims = dict(schema.dims)
        self.assertIn("time", dims)
        del dims["time"]
        self.assertEqual({'y': 50, 'x': 100}, dims)
        self.assertEqual({'chl', 'tsm', 'time', 'y', 'x'},
                         set(schema.variables.keys()))
        self.assertEqualVariableSchema(
            VariableOutline(dtype="uint16",
                            dims=("time", "y", "x"),
                            shape=(3, 50, 100),
                            chunks=(1, 30, 50),
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
                            shape=(3, 50, 100),
                            chunks=(1, 30, 50),
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
                            shape=(100,),
                            chunks=(100,),
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
        schema_1 = DatasetOutline.from_dataset(
            make_test_dataset(uri="memory://test.zarr")
        )
        self.assertEqual([], schema_1.get_noncompliance(schema_1))

        schema_2 = DatasetOutline.from_config(
            make_test_config()
        )
        self.assertEqual([], schema_2.get_noncompliance(schema_1))
