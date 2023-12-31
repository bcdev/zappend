# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
from click.testing import CliRunner

from zappend.cli import zappend
from .helpers import clear_memory_fs
from .helpers import make_test_dataset

expected_help_text = \
    """
    Usage: zappend [OPTIONS] [SLICES]...
    
      Create or update a Zarr dataset TARGET from slice datasets SLICES.
    
    Options:
      -c, --config CONFIG  Configuration JSON or YAML file. If multiple are passed,
                           they will be deeply merged into one.
      -t, --target TARGET  Target Zarr dataset path or URI. Overrides the
                           'target_uri' configuration field.
      --dry-run            Run the tool without creating, changing, or deleting any
                           files.
      --help-config        Show configuration help and exit.
      --help               Show this message and exit.
    """

# remove indent
expected_help_text = expected_help_text.replace("\n    ", "\n").lstrip("\n")


class CliTest(unittest.TestCase):

    def setUp(self):
        clear_memory_fs()

    def test_help(self):
        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(zappend, ['--help'])
        self.assertEqual(0, result.exit_code)
        self.assertEqual(expected_help_text, result.output)

    def test_help_config(self):
        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(zappend, ['--help-config'])
        self.assertEqual(0, result.exit_code)
        self.assertIn("Configuration JSON schema:", result.output)

    def test_no_slices(self):
        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(zappend, [])
        self.assertEqual(0, result.exit_code)
        self.assertEqual("No slice datasets given.\n", result.output)

    def test_some_slices_and_target(self):
        make_test_dataset(uri="memory://slice-1.zarr")
        make_test_dataset(uri="memory://slice-2.zarr")
        make_test_dataset(uri="memory://slice-3.zarr")

        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(zappend,
                               [
                                   "--target",
                                   "memory://target.zarr",
                                   "memory://slice-1.zarr",
                                   "memory://slice-2.zarr",
                                   "memory://slice-3.zarr",
                               ])
        self.assertEqual("", result.output)
        self.assertEqual(0, result.exit_code)

    def test_some_slices_and_no_target(self):
        make_test_dataset(uri="memory://slice-1.zarr")
        make_test_dataset(uri="memory://slice-2.zarr")
        make_test_dataset(uri="memory://slice-3.zarr")

        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(zappend,
                               [
                                   "memory://slice-1.zarr",
                                   "memory://slice-2.zarr",
                                   "memory://slice-3.zarr",
                               ])
        self.assertEqual(1, result.exit_code)
        self.assertEqual("Error: Missing 'target_uri' in configuration\n",
                         result.output)
