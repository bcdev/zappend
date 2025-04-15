# Copyright © 2024, 2025 Brockmann Consult and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import datetime
import json
import unittest

import numpy as np
import pytest
import xarray as xr

from zappend.config.attrs import (
    ConfigAttrsUserFunctions,
    eval_dyn_config_attrs,
    eval_expr,
    get_dyn_config_attrs_env,
    has_dyn_config_attrs,
)

from ..helpers import make_test_dataset


class EvalDynConfigAttrsTest(unittest.TestCase):
    def setUp(self):
        ds = make_test_dataset()
        ds.attrs["title"] = "Ocean Colour"
        self.env = get_dyn_config_attrs_env(ds, N=10)

    def test_zero(self):
        self.assertEqual(
            {"value": "Chlorophyll A"},
            eval_dyn_config_attrs({"value": "Chlorophyll A"}, self.env),
        )
        self.assertEqual(
            {"value": 11},
            eval_dyn_config_attrs({"value": 11}, self.env),
        )

    def test_one(self):
        self.assertEqual(
            {"value": "Ocean Colour"},
            eval_dyn_config_attrs({"value": "{{ ds.attrs['title'] }}"}, self.env),
        )
        self.assertEqual(
            {"value": 10},
            eval_dyn_config_attrs({"value": "{{ N }}"}, self.env),
        )
        self.assertEqual(
            {"value": " 10 "},
            eval_dyn_config_attrs({"value": " {{ N }} "}, self.env),
        )

    def test_two(self):
        self.assertEqual(
            {"value": "Ocean Colour10"},
            eval_dyn_config_attrs({"value": "{{ds.attrs['title']}}{{N}}"}, self.env),
        )
        self.assertEqual(
            {"value": "Ocean Colour / 10"},
            eval_dyn_config_attrs({"value": "{{ds.attrs['title']}} / {{N}}"}, self.env),
        )
        self.assertEqual(
            {"value": "1010"},
            eval_dyn_config_attrs({"value": "{{ N }}{{ N }}"}, self.env),
        )

    def test_x_min_max(self):
        attrs = eval_dyn_config_attrs(
            {"x_min": "{{ ds.x[0] }}", "x_max": "{{ ds.x[-1] }}"}, self.env
        )
        self.assertEqual(
            {"x_min": 0.005, "x_max": 0.995},
            attrs,
        )
        self.assertEqual(attrs, json.loads(json.dumps(attrs)))

    def test_x_min_max_center(self):
        attrs = eval_dyn_config_attrs(
            {
                "x_min": "{{ ds.x[0] - (ds.x[1]-ds.x[0])/2 }}",
                "x_max": "{{ ds.x[-1] + (ds.x[1]-ds.x[0])/2 }}",
            },
            self.env,
        )
        self.assertAlmostEqual(0.0, attrs.get("x_min"))
        self.assertAlmostEqual(1.0, attrs.get("x_max"))
        self.assertEqual(attrs, json.loads(json.dumps(attrs)))

    def test_x_bounds_center(self):
        attrs = eval_dyn_config_attrs(
            {
                "x_min": "{{ lower_bound(ds.x, ref='center') }}",
                "x_max": "{{ upper_bound(ds.x, ref='center') }}",
            },
            self.env,
        )
        self.assertAlmostEqual(0.0, attrs.get("x_min"))
        self.assertAlmostEqual(1.0, attrs.get("x_max"))
        self.assertEqual(attrs, json.loads(json.dumps(attrs)))

    def test_time_min_max(self):
        attrs = eval_dyn_config_attrs(
            {"time_min": "{{ ds.time[0] }}", "time_max": "{{ ds.time[-1] }}"}, self.env
        )
        self.assertEqual(
            {"time_min": "2024-01-01T00:00:00", "time_max": "2024-01-03T00:00:00"},
            attrs,
        )
        self.assertEqual(attrs, json.loads(json.dumps(attrs)))

    def test_time_bounds(self):
        attrs = eval_dyn_config_attrs(
            {
                "time_min": "{{ lower_bound(ds.time) }}",
                "time_max": "{{ upper_bound(ds.time) }}",
            },
            self.env,
        )
        self.assertEqual(
            {"time_min": "2024-01-01T00:00:00", "time_max": "2024-01-04T00:00:00"},
            attrs,
        )
        self.assertEqual(attrs, json.loads(json.dumps(attrs)))


class HasDynConfigAttrsTest(unittest.TestCase):
    def test_no(self):
        self.assertFalse(
            has_dyn_config_attrs({"value": 3}),
        )
        self.assertFalse(
            has_dyn_config_attrs({"value": "Chlorophyll A"}),
        )
        self.assertFalse(
            has_dyn_config_attrs({"value": "Chlorophyll {{A"}),
        )
        self.assertFalse(
            has_dyn_config_attrs({"value": "Chlorophyll A}}"}),
        )

    def test_yes(self):
        self.assertTrue(
            has_dyn_config_attrs({"value": "{{}}"}),
        )
        self.assertTrue(
            has_dyn_config_attrs({"value": "{{A}}"}),
        )
        self.assertTrue(
            has_dyn_config_attrs({"value": "{{'Number'}} {{2}}"}),
        )


class EvalExprTest(unittest.TestCase):
    def test_scalar_result(self):
        # scalars
        self.assertEqual(None, eval_expr("None", {}))
        self.assertEqual(True, eval_expr("True", {}))
        self.assertEqual(13, eval_expr("13", {}))
        self.assertEqual("nan", eval_expr("float('NaN')", {}))
        self.assertEqual(0.5, eval_expr("0.5", {}))
        self.assertEqual("ABC", eval_expr("'ABC'", {}))
        time = datetime.date.fromisoformat("2024-01-02")
        self.assertEqual(
            "2024-01-02",
            eval_expr("time", dict(time=time)),
        )
        time = datetime.datetime.fromisoformat("2024-01-02T10:20:30")
        self.assertEqual(
            "2024-01-02T10:20:30",
            eval_expr("time", dict(time=time)),
        )
        with pytest.raises(
            ValueError, match="cannot serialize value of type <class 'object'>"
        ):
            eval_expr("obj", dict(obj=object()))

    def test_dict_result(self):
        self.assertEqual({}, eval_expr("d", dict(d={})))
        self.assertEqual(
            {
                "b": True,
                "i": 13,
                "t": [1, "B", {}],
                "f": 13.2,
                "d": "2020-05-04",
                "np_a": [0.1, 0.2],
                "xr_a": [0.3, 0.4],
            },
            eval_expr(
                "d",
                dict(
                    d={
                        "b": True,
                        "i": 13,
                        "t": (1, "B", {}),
                        "f": 13.2,
                        "d": datetime.date.fromisoformat("2020-05-04"),
                        "np_a": np.array([0.1, 0.2]),
                        "xr_a": xr.DataArray(np.array([0.3, 0.4])),
                    }
                ),
            ),
        )

    def test_array_1d_result(self):
        # arrays
        self.assert_array_ok([True, False])
        self.assert_array_ok([3, 4, 5])
        self.assert_array_ok([11.05, 11.15, 11.25])
        self.assert_array_ok([11.05, "nan", 11.25], dtype="float64")
        self.assert_array_ok(["A", "B"])
        self.assert_array_ok(["2024-01-02T10:20:30"], dtype="datetime64[s]")
        with pytest.raises(
            ValueError,
            match=(
                "cannot serialize 0-d array"
                " of type <class 'numpy.ndarray'>, dtype=dtype\\('O'\\)"
            ),
        ):
            eval_expr("a", dict(a=xr.DataArray([object(), object()])))

    def test_array_2d_result(self):
        self.assert_array_ok([[3, 4], [5, 6]])

    def assert_array_ok(self, a: list, dtype=None):
        # Test list
        self.assertEqual(
            a,
            eval_expr("a", dict(a=a)),
        )
        self.assertEqual(
            a[0],
            eval_expr("a[0]", dict(a=a)),
        )

        # Test numpy.ndarray
        np_a = np.array(a, dtype=dtype) if dtype is not None else np.array(a)
        self.assertEqual(
            a,
            eval_expr("a", dict(a=np_a)),
        )
        self.assertEqual(
            a[0],
            eval_expr("a[0]", dict(a=np_a)),
        )

        # Test xarray-DataArray
        xr_a = xr.DataArray(np_a)
        self.assertEqual(
            a,
            eval_expr("a", dict(a=xr_a)),
        )
        self.assertEqual(
            a[0],
            eval_expr("a[0]", dict(a=xr_a)),
        )


class GetDynConfigAttrsEnvTest(unittest.TestCase):
    def test_env(self):
        ds = make_test_dataset()
        env = get_dyn_config_attrs_env(ds)
        self.assertIsInstance(env, dict)
        self.assertEqual({"ds", "lower_bound", "upper_bound"}, set(env.keys()))
        self.assertIs(ds, env["ds"])
        self.assertTrue(callable(env["lower_bound"]))
        self.assertTrue(callable(env["upper_bound"]))


class ConfigAttrsUserFunctionsTest(unittest.TestCase):
    def test_lower_bounds_numerical_ok(self):
        self._assert_lower_bound_ok(np.array([0, 1, 2]), 0, -0.5, -1)
        self._assert_lower_bound_ok(np.array([2, 1, 0]), 0, -0.5, -1)
        self._assert_lower_bound_ok(np.array([0.5, 1.5, 2.5]), 0.5, 0.0, -0.5)

    def test_lower_bounds_datetime_ok(self):
        self._assert_lower_bound_ok(
            np.array(["2024-01-01", "2024-01-02", "2024-01-03"], dtype="datetime64[h]"),
            np.datetime64("2024-01-01T00", "h"),
            np.datetime64("2023-12-31T12", "h"),
            np.datetime64("2023-12-31T00", "h"),
        )
        self._assert_lower_bound_ok(
            np.array(
                ["2024-01-01 12:00:00", "2024-01-02 12:00:00", "2024-01-03 12:00:00"],
                dtype="datetime64[h]",
            ),
            np.datetime64("2024-01-01T12", "h"),
            np.datetime64("2024-01-01T00", "h"),
            np.datetime64("2023-12-31T12", "h"),
        )

    def test_upper_bounds_numerical_ok(self):
        self._assert_upper_bound_ok(np.array([0, 1, 2]), 3, 2.5, 2)
        self._assert_upper_bound_ok(np.array([2, 1, 0]), 3, 2.5, 2)
        self._assert_upper_bound_ok(np.array([2.5, 1.5, 0.5]), 3.5, 3.0, 2.5)

    def test_upper_bounds_datetime_ok(self):
        self._assert_upper_bound_ok(
            np.array(["2024-01-01", "2024-01-02", "2024-01-03"], dtype="datetime64[h]"),
            np.datetime64("2024-01-04T00", "h"),
            np.datetime64("2024-01-03T12", "h"),
            np.datetime64("2024-01-03T00", "h"),
        )
        self._assert_upper_bound_ok(
            np.array(
                ["2024-01-01 12:00:00", "2024-01-02 12:00:00", "2024-01-03 12:00:00"],
                dtype="datetime64[h]",
            ),
            np.datetime64("2024-01-04T12", "h"),
            np.datetime64("2024-01-04T00", "h"),
            np.datetime64("2024-01-03T12", "h"),
        )

    def _assert_lower_bound_ok(
        self, array, expected_lower, expected_center, expected_upper
    ):
        self._assert_bound_func_ok(
            ConfigAttrsUserFunctions.lower_bound,
            array,
            expected_lower,
            expected_center,
            expected_upper,
        )

    def _assert_upper_bound_ok(
        self, array, expected_lower, expected_center, expected_upper
    ):
        self._assert_bound_func_ok(
            ConfigAttrsUserFunctions.upper_bound,
            array,
            expected_lower,
            expected_center,
            expected_upper,
        )

    def _assert_bound_func_ok(
        self, f, array, expected_lower, expected_center, expected_upper
    ):
        self.assertEqual(expected_lower, f(array), "default")
        self.assertEqual(expected_lower, f(array, ref="lower"), "lower")
        self.assertEqual(expected_center, f(array, ref="center"), "center")
        self.assertEqual(expected_upper, f(array, ref="upper"), "upper")

    def test_upper_lower_bounds_fail_for_wrong_shape(self):
        self._assert_upper_lower_bounds_fail(np.array(3))
        self._assert_upper_lower_bounds_fail(np.array([]))
        self._assert_upper_lower_bounds_fail(np.array([[1, 2], [3, 4]]))

    # noinspection PyMethodMayBeStatic
    def _assert_upper_lower_bounds_fail(self, array):
        with pytest.raises(ValueError):
            ConfigAttrsUserFunctions.lower_bound(array)
        with pytest.raises(ValueError):
            ConfigAttrsUserFunctions.upper_bound(array)
