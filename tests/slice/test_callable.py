# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import pytest

from zappend.context import Context
from zappend.slice.callable import import_attribute
from zappend.slice.callable import to_slice_args


class ToSliceArgsTest(unittest.TestCase):
    def test_to_slice_args_ok(self):
        # tuple
        self.assertEqual(((), {}), to_slice_args(((), {})))
        self.assertEqual(((1, 2), {"c": 3}), to_slice_args(([1, 2], {"c": 3})))

        # list
        self.assertEqual(((), {}), to_slice_args([]))
        self.assertEqual(((1, 2, 3), {}), to_slice_args([1, 2, 3]))

        # dict
        self.assertEqual(((), {}), to_slice_args({}))
        self.assertEqual(((), {"c": 3}), to_slice_args({"c": 3}))

        # other
        self.assertEqual(((1,), {}), to_slice_args(1))
        self.assertEqual((("a",), {}), to_slice_args("a"))

    # noinspection PyMethodMayBeStatic
    def test_normalize_args_fails(self):
        with pytest.raises(
            TypeError, match="tuple of form \\(args, kwargs\\) expected"
        ):
            to_slice_args(((), (), ()))
        with pytest.raises(
            TypeError,
            match="args in tuple of form \\(args, kwargs\\) must be a tuple or list",
        ):
            to_slice_args(({}, {}))
        with pytest.raises(
            TypeError, match="kwargs in tuple of form \\(args, kwargs\\) must be a dict"
        ):
            to_slice_args(((), ()))


class ImportAttributeTest(unittest.TestCase):
    def test_import_attribute_ok(self):
        self.assertIs(
            ImportAttributeTest,
            import_attribute("tests.slice.test_callable.ImportAttributeTest"),
        )

        self.assertIs(
            ImportAttributeTest.test_import_attribute_ok,
            import_attribute(
                "tests.slice.test_callable.ImportAttributeTest.test_import_attribute_ok"
            ),
        )

    # noinspection PyMethodMayBeStatic
    def test_import_attribute_fails(self):
        with pytest.raises(
            ImportError,
            match="attribute 'Pippo' not found in module 'tests.slice.test_callable'",
        ):
            import_attribute("tests.slice.test_callable.Pippo")

        with pytest.raises(
            ImportError,
            match="no attribute found named 'tests.slice.test_callable.'",
        ):
            import_attribute("tests.slice.test_callable.")

        with pytest.raises(
            ImportError,
            match="no attribute found named 'pippo.test_slice.ImportObjectTest'",
        ):
            import_attribute("pippo.test_slice.ImportObjectTest")


def false_slice_source_function(ctx: Context, path: str):
    return 17
