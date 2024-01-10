# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
import unittest

import fsspec
import pytest
import yaml

from zappend.config import CONFIG_V1_SCHEMA
from zappend.config import schema_to_json
from zappend.config import schema_to_md
from zappend.config import merge_configs
from zappend.config import normalize_config
from zappend.config import validate_config
from zappend.fsutil.fileobj import FileObj
from .helpers import clear_memory_fs


class ConfigValidateTest(unittest.TestCase):
    def test_validate_empty_ok(self):
        config = {}
        self.assertIs(config, validate_config(config))

    def test_validate_versions_ok(self):
        config = {"version": 1, "zarr_version": 2}
        self.assertIs(config, validate_config(config))

    # noinspection PyMethodMayBeStatic
    def test_validate_versions_fail(self):
        config = {"zarr_version": 1}
        with pytest.raises(
            ValueError,
            match="Invalid configuration:" " 2 was expected for zarr_version",
        ):
            validate_config(config)

    # noinspection PyMethodMayBeStatic
    def test_validate_variable_fail(self):
        config = {
            "zarr_version": 2,
            "variables": {
                "chl": {
                    "dims": [10, 20, 30],
                    "encoding": {
                        "dtype": "int32",
                    },
                }
            },
        }
        with pytest.raises(
            ValueError,
            match="Invalid configuration:"
            " 10 is not of type 'string'"
            " for variables.chl.dims.0",
        ):
            validate_config(config)


class ConfigNormalizeTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_normalize_dict(self):
        config = {"version": 1, "zarr_version": 2}
        self.assertIs(config, normalize_config(config))

    def test_normalize_none(self):
        self.assertEqual({}, normalize_config(None))

    def test_normalize_json_uri(self):
        uri = "memory://config.json"
        config = {"version": 1, "zarr_version": 2}
        with fsspec.open(uri, "w") as f:
            f.write(json.dumps(config))
        self.assertEqual(config, normalize_config(uri))

    def test_normalize_yaml_uri(self):
        uri = "memory://config.yaml"
        config = {"version": 1, "zarr_version": 2}
        with fsspec.open(uri, "w") as f:
            f.write(yaml.dump(config))
        self.assertEqual(config, normalize_config(uri))

    def test_normalize_file_obj(self):
        file_obj = FileObj("memory://config.yaml")
        config = {"version": 1, "zarr_version": 2}
        file_obj.write(yaml.dump(config))
        self.assertEqual(config, normalize_config(file_obj))

    # noinspection PyMethodMayBeStatic
    def test_it_raises_if_config_is_not_object(self):
        file_obj = FileObj("memory://config.yaml")
        file_obj.write("what?")
        with pytest.raises(
            TypeError,
            match="Invalid configuration:" " memory://config.yaml: object expected",
        ):
            normalize_config(file_obj)

    def test_normalize_sequence(self):
        configs = (
            {
                "version": 1,
                "zarr_version": 2,
                "fixed_dims": {
                    "x": 200,
                },
                "append_dim": "time",
            },
            {
                "fixed_dims": {
                    "y": 100,
                },
                "variables": {
                    "time": {
                        "dims": "time",
                        "encoding": {
                            "dtype": "uint64",
                        },
                    },
                    "y": {
                        "dims": "y",
                        "encoding": {
                            "dtype": "float64",
                        },
                    },
                    "x": {
                        "dims": "x",
                        "encoding": {
                            "dtype": "float64",
                        },
                    },
                },
            },
            {
                "variables": {
                    "chl": {
                        "dims": ("time", "y", "x"),
                        "encoding": {
                            "dtype": "float32",
                            "chunks": (1, 20, 30),
                            "fill_value": None,
                        },
                    },
                    "tsm": {
                        "dims": ("time", "y", "x"),
                        "encoding": {
                            "dtype": "float32",
                            "chunks": (1, 20, 30),
                            "fill_value": None,
                        },
                    },
                }
            },
        )
        self.assertEqual(
            {
                "version": 1,
                "zarr_version": 2,
                "fixed_dims": {
                    "x": 200,
                    "y": 100,
                },
                "append_dim": "time",
                "variables": {
                    "x": {
                        "dims": "x",
                        "encoding": {"dtype": "float64"},
                    },
                    "y": {
                        "dims": "y",
                        "encoding": {"dtype": "float64"},
                    },
                    "time": {
                        "dims": "time",
                        "encoding": {"dtype": "uint64"},
                    },
                    "chl": {
                        "dims": ("time", "y", "x"),
                        "encoding": {
                            "dtype": "float32",
                            "chunks": (1, 20, 30),
                            "fill_value": None,
                        },
                    },
                    "tsm": {
                        "dims": ("time", "y", "x"),
                        "encoding": {
                            "dtype": "float32",
                            "chunks": (1, 20, 30),
                            "fill_value": None,
                        },
                    },
                },
            },
            normalize_config(configs),
        )

    # noinspection PyMethodMayBeStatic
    def test_normalize_invalid(self):
        with pytest.raises(TypeError):
            normalize_config(42)
        with pytest.raises(TypeError):
            normalize_config(True)
        with pytest.raises(TypeError):
            normalize_config(bytes())

    def test_schema(self):
        schema = CONFIG_V1_SCHEMA
        self.assertIn("properties", schema)
        self.assertIsInstance(schema["properties"], dict)
        self.assertEqual(
            {
                "append_dim",
                "disable_rollback",
                "dry_run",
                "excluded_variables",
                "fixed_dims",
                "included_variables",
                "logging",
                "slice_engine",
                "slice_polling",
                "slice_storage_options",
                "target_storage_options",
                "target_dir",
                "temp_dir",
                "temp_storage_options",
                "variables",
                "version",
                "zarr_version",
            },
            set(schema["properties"].keys()),
        )

    def test_merge_config(self):
        self.assertEqual({}, merge_configs())
        self.assertEqual({}, merge_configs({}))
        self.assertEqual({}, merge_configs({}, {}))
        self.assertEqual({"a": 1}, merge_configs({"a": 1}))
        self.assertEqual({"a": 2}, merge_configs({"a": 1}, {"a": 2}))
        self.assertEqual({"a": None}, merge_configs({"a": 1}, {"a": None}))
        self.assertEqual({"a": 2}, merge_configs({"a": None}, {"a": 2}))
        self.assertEqual({"a": [3, 4]}, merge_configs({"a": [1, 2]}, {"a": [3, 4]}))
        self.assertEqual(
            {"a": {"b": 3, "c": 4}},
            merge_configs({"a": {"b": 2, "c": 4}}, {"a": {"b": 3}}),
        )

    def test_schema_to_json(self):
        # Smoke test is sufficient here
        text = schema_to_json()
        self.assertIsInstance(text, str)
        self.assertTrue(len(text) > 0)

    def test_schema_to_md(self):
        # Smoke test is sufficient here
        text = schema_to_md()
        self.assertIsInstance(text, str)
        self.assertTrue(len(text) > 0)
