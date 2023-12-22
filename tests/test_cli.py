# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
from click.testing import CliRunner
from zappend.cli import zappend

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
