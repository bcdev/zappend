# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import pytest

from zappend.fsutil.path import split_components, split_parent


class SplitFilenameTest(unittest.TestCase):
    # noinspection PyMethodMayBeStatic
    def test_empty_path(self):
        with pytest.raises(ValueError):
            split_parent("")

    def test_split_parent_1c(self):
        self.assertEqual(("/", ""), split_parent("/"))
        self.assertEqual(("", "a"), split_parent("a"))
        self.assertEqual(("/", "a"), split_parent("/a"))
        self.assertEqual(("a", ""), split_parent("a/"))
        self.assertEqual(("/a", ""), split_parent("/a/"))

    def test_split_parent_2c(self):
        self.assertEqual(("a", "b"), split_parent("a/b"))
        self.assertEqual(("/a", "b"), split_parent("/a/b"))
        self.assertEqual(("a/b", ""), split_parent("a/b/"))
        self.assertEqual(("/a/b", ""), split_parent("/a/b/"))

    def test_split_parent_3c(self):
        self.assertEqual(("a/b", "c"), split_parent("a/b/c"))
        self.assertEqual(("/a/b", "c"), split_parent("/a/b/c"))
        self.assertEqual(("a/b/c", ""), split_parent("a/b/c/"))
        self.assertEqual(("/a/b/c", ""), split_parent("/a/b/c/"))


class SplitComponentsTest(unittest.TestCase):
    def test_split_components_0c(self):
        self.assertEqual([""], split_components(""))

    def test_split_components_1c(self):
        self.assertEqual(["/"], split_components("/"))

        self.assertEqual(["a"], split_components("a"))
        self.assertEqual(["/a"], split_components("/a"))
        self.assertEqual(["a/"], split_components("a/"))
        self.assertEqual(["/a/"], split_components("/a/"))

    def test_split_components_2c(self):
        self.assertEqual(["a", "b"], split_components("a/b"))
        self.assertEqual(["/a", "b"], split_components("/a/b"))
        self.assertEqual(["a", "b/"], split_components("a/b/"))
        self.assertEqual(["/a", "b/"], split_components("/a/b/"))

    def test_split_components_3c(self):
        self.assertEqual(["a", "b", "c"], split_components("a/b/c"))
        self.assertEqual(["/a", "b", "c"], split_components("/a/b/c"))
        self.assertEqual(["a", "b", "c/"], split_components("a/b/c/"))
        self.assertEqual(["/a", "b", "c/"], split_components("/a/b/c/"))
