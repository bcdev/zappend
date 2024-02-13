# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
import unittest

from zappend.config.attrs import eval_dyn_config_attrs
from ..helpers import make_test_dataset


class ConfigEvalAttrsTest(unittest.TestCase):
    def setUp(self):
        ds = make_test_dataset()
        ds.attrs["title"] = "Ocean Colour"
        self.env = dict(ds=ds, N=10)

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

    def test_x_min_max_corr(self):
        attrs = eval_dyn_config_attrs(
            {
                "x_min": "{{ ds.x[0] - (ds.x[1]-ds.x[0])/2 }}",
                "x_max": "{{ ds.x[-1] + (ds.x[1]-ds.x[0])/2 }}",
            },
            self.env,
        )
        self.assertAlmostEquals(0.0, attrs.get("x_min"))
        self.assertAlmostEquals(1.0, attrs.get("x_max"))
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
