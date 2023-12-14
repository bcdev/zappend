import unittest
from click.testing import CliRunner
from zappend.cli import zappend


class CliTest(unittest.TestCase):
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(zappend, ['--help'])
        self.assertEqual(0, result.exit_code)
        self.assertEqual((
            "Usage: zappend [OPTIONS] TARGET [SUBSETS]...\n"
            "\n"
            "  Tool to create or update a Zarr dataset from subsets.\n"
            "\n"
            "Options:\n"
            "  -c, --config TEXT  Configuration file.\n"
            "  --help             Show this message and exit.\n"
        ), result.output)

    def test_zappend(self):
        runner = CliRunner()
        result = runner.invoke(zappend, ['target.zarr',
                                         'source-1.zarr',
                                         'source-2.zarr',
                                         '--config',
                                         'zappend-config.yaml'])
        self.assertEqual(0, result.exit_code)
        self.assertEqual((
            "Reading zappend-config.yaml\n"
            "Creating target.zarr\n"
            "Appending source-1.zarr to target.zarr\n"
            "Appending source-2.zarr to target.zarr\n"
            "Done.\n"
        ), result.output)
