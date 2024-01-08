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

    def test_to_zarr(self):
        target_dir = FileObj("memory://target.zarr")
        ds = make_test_dataset(
            shape=(1, 50, 100),
            chunks=(2, 50, 50)
        )
        ds.to_zarr(RollbackStore(target_dir.fs.get_mapper(
            root=target_dir.path.strip("/"),
            create=True
        ), self.handle_rollback_action))
        ds = xr.open_zarr(target_dir.fs.get_mapper(
            root=target_dir.path.strip("/"),
            create=False
        ))
        self.assertEqual({'time': 1, 'y': 50, 'x': 100}, ds.dims)
        self.assertEqual(
            {
                ('delete_file', '.zattrs'),
                ('delete_file', '.zgroup'),
                ('delete_file', '.zmetadata'),
                ('delete_file', 'chl/.zarray'),
                ('delete_file', 'chl/.zattrs'),
                ('delete_file', 'chl/0.0.0'),
                ('delete_file', 'chl/0.0.1'),
                ('delete_file', 'time/.zarray'),
                ('delete_file', 'time/.zattrs'),
                ('delete_file', 'time/0'),
                ('delete_file', 'tsm/.zarray'),
                ('delete_file', 'tsm/.zattrs'),
                ('delete_file', 'tsm/0.0.0'),
                ('delete_file', 'tsm/0.0.1'),
                ('delete_file', 'x/.zarray'),
                ('delete_file', 'x/.zattrs'),
                ('delete_file', 'x/0'),
                ('delete_file', 'y/.zarray'),
                ('delete_file', 'y/.zattrs'),
                ('delete_file', 'y/0')
            },
            set([r[:2] for r in self.records])
        )

        #####################################################################
        # Add slice 1

        self.records = []
        slice_1 = make_test_dataset(
            shape=(1, 50, 100),
            chunks=(1, 50, 50)
        )
        for k, v in slice_1.variables.items():
            v.encoding = {}
            v.attrs = {}

        slice_1.to_zarr(RollbackStore(
            target_dir.fs.get_mapper(
                root=target_dir.path.strip("/"),
                create=False
            ),
            self.handle_rollback_action),
            mode="a",
            append_dim="time"
        )
        ds = xr.open_zarr(target_dir.fs.get_mapper(
            root=target_dir.path.strip("/"),
            create=False
        ))
        self.assertEqual({'time': 2, 'y': 50, 'x': 100}, ds.dims)
        self.assertEqual(
            {
                ('replace_file', '.zmetadata'),
                ('replace_file', '.zattrs'),
                ('replace_file', 'x/0'),
                ('replace_file', 'y/0'),
                ('replace_file', 'time/.zarray'),
                ('delete_file', 'time/1'),
                ('replace_file', 'chl/.zarray'),
                ('delete_file', 'chl/1.0.0'),
                ('delete_file', 'chl/1.0.1'),
                ('replace_file', 'tsm/.zarray'),
                ('delete_file', 'tsm/1.0.0'),
                ('delete_file', 'tsm/1.0.1'),
            },
            set([r[:2] for r in self.records])
        )

        #####################################################################
        # Add slice 2

        self.records = []
        slice_2 = make_test_dataset(
            shape=(1, 50, 100),
            chunks=(1, 50, 50)
        )
        for k, v in slice_2.variables.items():
            v.encoding = {}
            v.attrs = {}

        slice_2.to_zarr(RollbackStore(
            target_dir.fs.get_mapper(
                root=target_dir.path.strip("/"),
                create=False
            ),
            self.handle_rollback_action),
            mode="a",
            append_dim="time"
        )
        ds = xr.open_zarr(target_dir.fs.get_mapper(
            root=target_dir.path.strip("/"),
            create=False
        ))
        self.assertEqual({'time': 3, 'y': 50, 'x': 100}, ds.dims)
        self.assertEqual({'time': 3, 'y': 50, 'x': 100},
                         ds.time.encoding)
        self.assertEqual(
            {
                ('replace_file', '.zmetadata'),
                ('replace_file', '.zattrs'),
                ('replace_file', 'x/0'),
                ('replace_file', 'y/0'),
                ('replace_file', 'time/.zarray'),
                ('delete_file', 'time/2'),
                ('replace_file', 'chl/.zarray'),
                ('delete_file', 'chl/2.0.0'),
                ('delete_file', 'chl/2.0.1'),
                ('replace_file', 'tsm/.zarray'),
                ('delete_file', 'tsm/2.0.0'),
                ('delete_file', 'tsm/2.0.1'),
            },
            set([r[:2] for r in self.records])
        )
