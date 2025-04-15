# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import pytest

from zappend.config import validate_config


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
            match="Invalid configuration: 2 was expected for zarr_version",
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
            " [1-3]0 is not of type 'string'"
            " for variables.chl.dims.[0-2]",
        ):
            validate_config(config)
