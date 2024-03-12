# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
from click.testing import CliRunner

from zappend import __version__
from zappend.cli import zappend
from zappend.fsutil.fileobj import FileObj
from .helpers import clear_memory_fs
from .helpers import make_test_dataset


class CliTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_version(self):
        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(zappend, ["--version"])
        self.assertEqual(0, result.exit_code)
        self.assertEqual(f"{__version__}\n", result.output)

    def test_help(self):
        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(zappend, ["--help"])
        self.assertEqual(0, result.exit_code)
        self.assertIn("subsequent configurations are incremental", result.output)

    def test_help_config(self):
        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(zappend, ["--help-config", "json"])
        self.assertEqual(0, result.exit_code)
        self.assertIn('"target_dir": {', result.output)
        # noinspection PyTypeChecker
        result = runner.invoke(zappend, ["--help-config", "md"])
        self.assertEqual(0, result.exit_code)
        self.assertIn("## `target_dir`", result.output)

    def test_no_slices(self):
        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(zappend, [])
        self.assertEqual(0, result.exit_code)
        self.assertEqual("No slice datasets given.\n", result.output)
        self.assertFalse(FileObj("memory://target.zarr").exists())

    def test_some_slices_and_target(self):
        make_test_dataset(uri="memory://slice-1.zarr")
        make_test_dataset(uri="memory://slice-2.zarr")
        make_test_dataset(uri="memory://slice-3.zarr")

        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(
            zappend,
            [
                "--target",
                "memory://target.zarr",
                "memory://slice-1.zarr",
                "memory://slice-2.zarr",
                "memory://slice-3.zarr",
            ],
        )
        self.assertEqual("", result.output)
        self.assertEqual(0, result.exit_code)
        self.assertTrue(FileObj("memory://target.zarr").exists())

    def test_some_slices_and_target_dry_run(self):
        make_test_dataset(uri="memory://slice-1.zarr")
        make_test_dataset(uri="memory://slice-2.zarr")
        make_test_dataset(uri="memory://slice-3.zarr")

        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(
            zappend,
            [
                "--target",
                "memory://target.zarr",
                "--dry-run",
                "memory://slice-1.zarr",
                "memory://slice-2.zarr",
                "memory://slice-3.zarr",
            ],
        )
        self.assertEqual("", result.output)
        self.assertEqual(0, result.exit_code)
        self.assertFalse(FileObj("memory://target.zarr").exists())

    def test_some_slices_and_no_target(self):
        make_test_dataset(uri="memory://slice-1.zarr")
        make_test_dataset(uri="memory://slice-2.zarr")
        make_test_dataset(uri="memory://slice-3.zarr")

        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(
            zappend,
            [
                "memory://slice-1.zarr",
                "memory://slice-2.zarr",
                "memory://slice-3.zarr",
            ],
        )
        self.assertEqual(1, result.exit_code)
        self.assertEqual(
            "Error: Missing 'target_dir' in configuration\n", result.output
        )
        self.assertFalse(FileObj("memory://target.zarr").exists())

    def test_some_slices_and_no_target_with_traceback(self):
        make_test_dataset(uri="memory://slice-1.zarr")
        make_test_dataset(uri="memory://slice-2.zarr")
        make_test_dataset(uri="memory://slice-3.zarr")

        runner = CliRunner()
        # noinspection PyTypeChecker
        result = runner.invoke(
            zappend,
            [
                "--traceback",
                "memory://slice-1.zarr",
                "memory://slice-2.zarr",
                "memory://slice-3.zarr",
            ],
        )
        print(result.output)
        self.assertEqual(1, result.exit_code)
        self.assertIn("Traceback (most recent call last):\n", result.output)
        self.assertIn("Error: Missing 'target_dir' in configuration\n", result.output)
        self.assertFalse(FileObj("memory://target.zarr").exists())
