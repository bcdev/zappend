# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import xarray as xr

from zappend.fsutil.fileobj import FileObj
from zappend.rollbackstore import RollbackStore
from .helpers import clear_memory_fs
from .helpers import make_test_dataset


class RollbackStoreTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()
        self.records = []

    def handle_rollback_action(self, *args):
        self.records.append(args)

    def test_some_slices(self):
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
                ('delete', '.zmetadata'),
                ('delete', '.zgroup'),
                ('delete', '.zattrs'),
                ('delete', 'x/.zarray'),
                ('delete', 'x/.zattrs'),
                ('delete', 'x/0'),
                ('delete', 'y/.zarray'),
                ('delete', 'y/.zattrs'),
                ('delete', 'y/0'),
                ('delete', 'time/.zarray'),
                ('delete', 'time/.zattrs'),
                ('delete', 'time/0'),
                ('delete', 'chl/.zarray'),
                ('delete', 'chl/.zattrs'),
                ('delete', 'chl/0.0.0'),
                ('delete', 'chl/0.0.1'),
                ('delete', 'chl/0.1.0'),
                ('delete', 'chl/0.1.1'),
                ('delete', 'chl/1.0.0'),
                ('delete', 'chl/1.0.1'),
                ('delete', 'chl/1.1.0'),
                ('delete', 'chl/1.1.1'),
                ('delete', 'chl/2.0.0'),
                ('delete', 'chl/2.0.1'),
                ('delete', 'chl/2.1.0'),
                ('delete', 'chl/2.1.1'),
                ('delete', 'tsm/.zarray'),
                ('delete', 'tsm/.zattrs'),
                ('delete', 'tsm/0.0.0'),
                ('delete', 'tsm/0.0.1'),
                ('delete', 'tsm/1.0.0'),
                ('delete', 'tsm/1.0.1'),
                ('delete', 'tsm/1.1.0'),
                ('delete', 'tsm/1.1.1'),
                ('delete', 'tsm/0.1.0'),
                ('delete', 'tsm/0.1.1'),
                ('delete', 'tsm/2.0.0'),
                ('delete', 'tsm/2.0.1'),
                ('delete', 'tsm/2.1.0'),
                ('delete', 'tsm/2.1.1'),
            },
            set(self.records)
        )
