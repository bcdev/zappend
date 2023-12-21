# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import pytest

from zappend.path import split_components
from zappend.path import split_filename


class SplitFilenameTest(unittest.TestCase):
    # noinspection PyMethodMayBeStatic
    def test_empty_path(self):
        with pytest.raises(ValueError):
            split_filename("")

    def test_split_filename_1c(self):
        self.assertEqual(("", ""), split_filename("/"))
        self.assertEqual(('', 'a'), split_filename("a"))
        self.assertEqual(('', 'a'), split_filename("/a"))
        self.assertEqual(('a', ''), split_filename("a/"))
        self.assertEqual(('/a', ''), split_filename("/a/"))

    def test_split_filename_2c(self):
        self.assertEqual(('a', 'b'), split_filename("a/b"))
        self.assertEqual(('/a', 'b'), split_filename("/a/b"))
        self.assertEqual(('a/b', ''), split_filename("a/b/"))
        self.assertEqual(('/a/b', ''), split_filename("/a/b/"))

    def test_split_filename_3c(self):
        self.assertEqual(('a/b', 'c'), split_filename("a/b/c"))
        self.assertEqual(('/a/b', 'c'), split_filename("/a/b/c"))
        self.assertEqual(('a/b/c', ''), split_filename("a/b/c/"))
        self.assertEqual(('/a/b/c', ''), split_filename("/a/b/c/"))


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
