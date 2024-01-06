# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import xarray as xr

from zappend.fsutil.fileobj import FileObj
from zappend.rollbackstore import RollbackStore
from .helpers import clear_memory_fs
from .helpers import make_test_dataset


class RollbackStoreOverridesTest(unittest.TestCase):
    def test_overrides(self):
        # TODO: implement tests for specific store overrides
        pass


class RollbackStoreZarrTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()
        self.records = []

    def handle_rollback_action(self, *args):
        self.records.append(args)

    def test_create_zarr(self):
        target_dir = FileObj("memory://target.zarr")
        ds = make_test_dataset()
        ds.to_zarr(RollbackStore(target_dir.fs.get_mapper(
            root=target_dir.path.strip("/"),
            create=True
        ), self.handle_rollback_action))
        self.assertTrue(target_dir.exists())
        ds2 = xr.open_zarr(target_dir.fs.get_mapper(
            root=target_dir.path.strip("/"),
            create=False
        ))
        self.assertEqual(ds.dims, ds2.dims)
        self.assertEqual(
            {
                ('delete_file', '.zmetadata'),
                ('delete_file', '.zgroup'),
                ('delete_file', '.zattrs'),
                ('delete_file', 'x/.zarray'),
                ('delete_file', 'x/.zattrs'),
                ('delete_file', 'x/0'),
                ('delete_file', 'y/.zarray'),
                ('delete_file', 'y/.zattrs'),
                ('delete_file', 'y/0'),
                ('delete_file', 'time/.zarray'),
                ('delete_file', 'time/.zattrs'),
                ('delete_file', 'time/0'),
                ('delete_file', 'chl/.zarray'),
                ('delete_file', 'chl/.zattrs'),
                ('delete_file', 'chl/0.0.0'),
                ('delete_file', 'chl/0.0.1'),
                ('delete_file', 'chl/0.1.0'),
                ('delete_file', 'chl/0.1.1'),
                ('delete_file', 'chl/1.0.0'),
                ('delete_file', 'chl/1.0.1'),
                ('delete_file', 'chl/1.1.0'),
                ('delete_file', 'chl/1.1.1'),
                ('delete_file', 'chl/2.0.0'),
                ('delete_file', 'chl/2.0.1'),
                ('delete_file', 'chl/2.1.0'),
                ('delete_file', 'chl/2.1.1'),
                ('delete_file', 'tsm/.zarray'),
                ('delete_file', 'tsm/.zattrs'),
                ('delete_file', 'tsm/0.0.0'),
                ('delete_file', 'tsm/0.0.1'),
                ('delete_file', 'tsm/1.0.0'),
                ('delete_file', 'tsm/1.0.1'),
                ('delete_file', 'tsm/1.1.0'),
                ('delete_file', 'tsm/1.1.1'),
                ('delete_file', 'tsm/0.1.0'),
                ('delete_file', 'tsm/0.1.1'),
                ('delete_file', 'tsm/2.0.0'),
                ('delete_file', 'tsm/2.0.1'),
                ('delete_file', 'tsm/2.1.0'),
                ('delete_file', 'tsm/2.1.1'),
            },
            set([r[:2] for r in self.records])
        )

    def test_append_zarr(self):
        # TODO implement test for appending a zarr with overlapping chunks
        pass
