# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
from typing import Callable

import pytest
import xarray as xr

from zappend.fsutil.fileobj import FileObj
from zappend.fsutil.transaction import Transaction
from zappend.fsutil.transaction import LOCK_FILE
from zappend.fsutil.transaction import ROLLBACK_FILE
from ..helpers import make_test_dataset


class TransactionTest(unittest.TestCase):
    def test_transaction_success(self):
        target_dir = FileObj("memory://cool-cube.zarr")
        self.run_transaction_test(target_dir, fail=False)
        ds = xr.open_zarr(target_dir.uri,
                          decode_cf=False, consolidated=False)
        self.assertEqual({"_FillValue": 9999}, ds.chl.attrs)
        self.assertEqual({"_FillValue": -9999}, ds.tsm.attrs)

    def test_transaction_with_rollback(self):
        target_dir = FileObj("memory://cool-cube.zarr")
        with pytest.raises(OSError, match="disk full"):
            self.run_transaction_test(target_dir, fail=True)
        ds = xr.open_zarr(target_dir.uri,
                          decode_cf=False, consolidated=False)
        self.assertEqual({'_FillValue': 9999,
                          'add_offset': 0,
                          'scale_factor': 0.2}, ds.chl.attrs)
        self.assertEqual({'_FillValue': -9999,
                          'add_offset': -200,
                          'scale_factor': 0.01}, ds.tsm.attrs)

    def run_transaction_test(self, target_dir: FileObj, fail: bool) \
            -> xr.Dataset:
        rollback_dir = FileObj("memory://rollback")
        rollback_file = rollback_dir / ROLLBACK_FILE
        lock_file = target_dir.parent / LOCK_FILE

        original_ds = make_test_dataset(uri=target_dir.uri)
        (target_dir / ".zmetadata").delete()
        chl_zattrs_file = target_dir / "chl" / ".zattrs"
        self.assertTrue(chl_zattrs_file.exists())
        tsm_zattrs_file = target_dir / "tsm" / ".zattrs"
        self.assertTrue(tsm_zattrs_file.exists())

        def change_file(zattrs_file: FileObj, rollback_cb: Callable):
            original_data = zattrs_file.read()
            zattrs_file.write(b'{"_ARRAY_DIMENSIONS": ["time", "y", "x"]}')
            rollback_cb("replace_file", zattrs_file.path, original_data)

        self.assertFalse(lock_file.exists())
        self.assertFalse(rollback_file.exists())
        with Transaction(target_dir, rollback_dir,
                         create_rollback_subdir=False) as rollback:
            self.assertTrue(lock_file.exists())
            self.assertTrue(rollback_file.exists())
            change_file(chl_zattrs_file, rollback)
            change_file(tsm_zattrs_file, rollback)
            # Not raised -> so we accept the rubbish written
            records = [line.split()[:2]
                       for line in rollback_file.read(mode="rt").split("\n")]
            self.assertEqual(
                [
                    ["replace_file", "/cool-cube.zarr/chl/.zattrs"],
                    ["replace_file", "/cool-cube.zarr/tsm/.zattrs"],
                    []
                ],
                records
            )
            if fail:
                raise OSError("disk full")

        self.assertTrue(target_dir.exists())
        self.assertFalse(lock_file.exists())
        self.assertFalse(rollback_dir.exists())
        return original_ds
