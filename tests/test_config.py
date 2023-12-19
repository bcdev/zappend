# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
import json
import unittest

import fsspec
import pytest
import yaml


from zappend.config import normalize_config
from zappend.config import validate_config
from zappend.fileobj import FileObj


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
        with pytest.raises(ValueError,
                           match="Invalid configuration:"
                                 " 2 was expected for zarr_version"):
            validate_config(config)

    # noinspection PyMethodMayBeStatic
    def test_validate_variable_fail(self):
        config = {"zarr_version": 2,
                  "variables": {
                      "chl": {
                        "dtype": "int32",
                        "dims": [10, 20, 30],
                      }
                  }}
        with pytest.raises(ValueError,
                           match="Invalid configuration:"
                                 " 10 is not of type 'string'"
                                 " for variables.chl.dims.0"):
            validate_config(config)


class ConfigNormalizeTest(unittest.TestCase):
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
        with file_obj.filesystem.open(file_obj.path, "w") as f:
            f.write(yaml.dump(config))
        self.assertEqual(config, normalize_config(file_obj))

    def test_normalize_sequence(self):
        configs = (
            {
                "version": 1,
                "zarr_version": 2,
                "dims": {
                    "time": None
                },
            },
            {
                "dims": {
                    "x": 200,
                    "y": 100,
                },
                "variables": {
                    "time": {
                        "dtype": "uint64",
                        "dims": "time"
                    },
                    "y": {
                        "dtype": "float64",
                        "dims": "y"
                    },
                    "x": {
                        "dtype": "float64",
                        "dims": "x"
                    },
                }
            },
            {
                "variables": {
                    "chl": {
                        "dtype": "float32",
                        "dims": ("time", "y", "x"),
                        "chunks": (1, 20, 30),
                        "fill_value": None
                    },
                    "tsm": {
                        "dtype": "float32",
                        "dims": ("time", "y", "x"),
                        "chunks": (1, 20, 30),
                        "fill_value": None
                    },
                }
            }
        )
        self.assertEqual(
            {
                "version": 1,
                "zarr_version": 2,
                "dims": {
                    "x": 200,
                    "y": 100,
                    "time": None
                },
                "variables": {
                    "time": {
                        "dtype": "uint64",
                        "dims": "time"
                    },
                    "y": {
                        "dtype": "float64",
                        "dims": "y"
                    },
                    "x": {
                        "dtype": "float64",
                        "dims": "x"
                    },
                    "chl": {
                        "dtype": "float32",
                        "dims": ("time", "y", "x"),
                        "chunks": (1, 20, 30),
                        "fill_value": None
                    },
                    "tsm": {
                        "dtype": "float32",
                        "dims": ("time", "y", "x"),
                        "chunks": (1, 20, 30),
                        "fill_value": None
                    },
                }
            },
            normalize_config(configs)
        )

    # noinspection PyMethodMayBeStatic
    def test_normalize_invalid(self):
        with pytest.raises(TypeError):
            normalize_config(42)
        with pytest.raises(TypeError):
            normalize_config(True)
        with pytest.raises(TypeError):
            normalize_config(bytes())
