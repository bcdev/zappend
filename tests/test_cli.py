# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
from click.testing import CliRunner
from zappend.cli import zappend


class CliTest(unittest.TestCase):
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(zappend, ['--help'])
        self.assertEqual(0, result.exit_code)
        self.assertEqual(
            (
                'Usage: zappend <options> <target-path> <slice-paths>\n'
                '\n'
                '  Tool to create or update a Zarr dataset from slices.\n'
                '\n'
                'Options:\n'
                '  -c, --config <config-path>  Configuration file.\n'
                '  --help                      Show this message and exit.\n'
            ),
            result.output)
