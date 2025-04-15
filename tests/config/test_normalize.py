# Copyright © 2024, 2025 Brockmann Consult and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
import os
import unittest

import fsspec
import pytest
import yaml

from zappend.config import exclude_from_config, merge_configs, normalize_config
from zappend.fsutil import FileObj

from ..helpers import clear_memory_fs


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

    def test_interpolate_env_vars_json(self):
        uri = "memory://config.json"
        config = {
            "slice_storage_options": {
                "key": "${_TEST_S3_KEY}",
                "secret": "$_TEST_S3_SECRET",
            }
        }
        with fsspec.open(uri, "wt") as f:
            f.write(json.dumps(config))
        os.environ["_TEST_S3_KEY"] = "abc"
        os.environ["_TEST_S3_SECRET"] = "123"
        self.assertEqual(
            {"slice_storage_options": {"key": "abc", "secret": "123"}},
            normalize_config(uri),
        )

    def test_interpolate_env_vars_yaml(self):
        uri = "memory://config.yaml"
        config = {
            "slice_storage_options": {
                "key": "${_TEST_S3_KEY}",
                "secret": "$_TEST_S3_SECRET",
            }
        }
        with fsspec.open(uri, "w") as f:
            f.write(yaml.dump(config))
        os.environ["_TEST_S3_KEY"] = "abc"
        os.environ["_TEST_S3_SECRET"] = "123"
        self.assertEqual(
            {"slice_storage_options": {"key": "abc", "secret": 123}},
            normalize_config(uri),
        )

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
            match="Invalid configuration: memory://config.yaml: object expected",
        ):
            normalize_config(file_obj)

    def test_normalize_sequence(self):
        data_var_spec = {
            "dims": ("time", "y", "x"),
            "encoding": {
                "dtype": "float32",
                "chunks": (1, 20, 30),
                "fill_value": None,
            },
        }
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
                    "chl": data_var_spec,
                    "tsm": data_var_spec,
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
                    "chl": data_var_spec,
                    "tsm": data_var_spec,
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

    def test_exclude_from_config(self):
        with exclude_from_config({}) as config:
            self.assertEqual({}, config)
        with exclude_from_config({"a": 1, "b": 2}) as config:
            self.assertEqual({"a": 1, "b": 2}, config)
        with exclude_from_config({}, "a") as config:
            self.assertEqual({}, config)
        with exclude_from_config({"a": 1, "b": 2}, "a") as config:
            self.assertEqual({"b": 2}, config)
        with exclude_from_config({"a": 1, "b": 2}, "b", "a") as config:
            self.assertEqual({}, config)
