# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

import pytest
import xarray as xr

from zappend.config import Config
from zappend.context import Context
from zappend.fsutil.fileobj import FileObj
from zappend.slice import SliceSource
from ..helpers import clear_memory_fs
from ..helpers import make_test_dataset


class ConfigTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_target_dir(self):
        target_dir = "memory://target.zarr"
        config = Config({"target_dir": target_dir})
        self.assertIsInstance(config.target_dir, FileObj)
        self.assertEqual(target_dir, config.target_dir.uri)

    def test_force_new(self):
        config = Config({"target_dir": "memory://target.zarr"})
        self.assertEqual(False, config.force_new)
        config = Config({"target_dir": "memory://target.zarr", "force_new": True})
        self.assertEqual(True, config.force_new)

    def test_append_dim(self):
        config = Config({"target_dir": "memory://target.zarr"})
        self.assertEqual("time", config.append_dim)
        config = Config({"target_dir": "memory://target.zarr", "append_dim": "depth"})
        self.assertEqual("depth", config.append_dim)

    def test_append_step(self):
        make_test_dataset(uri="memory://target.zarr")
        config = Config({"target_dir": "memory://target.zarr"})
        self.assertEqual(None, config.append_step)
        config = Config({"target_dir": "memory://target.zarr", "append_step": "1D"})
        self.assertEqual("1D", config.append_step)

    def test_attrs(self):
        make_test_dataset(uri="memory://target.zarr")
        config = Config({"target_dir": "memory://target.zarr"})
        self.assertEqual({}, config.attrs)
        self.assertEqual("keep", config.attrs_update_mode)
        self.assertEqual(False, config.permit_eval)
        config = Config(
            {
                "target_dir": "memory://target.zarr",
                "attrs": {"title": "OCC 2024"},
                "attrs_update_mode": "update",
                "permit_eval": True,
            }
        )
        self.assertEqual({"title": "OCC 2024"}, config.attrs)
        self.assertEqual("update", config.attrs_update_mode)
        self.assertEqual(True, config.permit_eval)

    def test_slice_polling(self):
        config = Config({"target_dir": "memory://target.zarr"})
        self.assertEqual((None, None), config.slice_polling)
        config = Config({"target_dir": "memory://target.zarr", "slice_polling": False})
        self.assertEqual((None, None), config.slice_polling)
        config = Config({"target_dir": "memory://target.zarr", "slice_polling": True})
        self.assertEqual((2, 60), config.slice_polling)
        config = Config(
            {
                "target_dir": "memory://target.zarr",
                "slice_polling": {"interval": 1, "timeout": 10},
            }
        )
        self.assertEqual((1, 10), config.slice_polling)

    def test_temp_dir(self):
        config = Config({"target_dir": "memory://target.zarr"})
        self.assertIsInstance(config.temp_dir, FileObj)
        self.assertTrue(config.temp_dir.exists())

    def test_disable_rollback(self):
        config = Config({"target_dir": "memory://target.zarr"})
        self.assertFalse(config.disable_rollback)
        config = Config(
            {"target_dir": "memory://target.zarr", "disable_rollback": True}
        )
        self.assertTrue(config.disable_rollback)

    def test_dry_run(self):
        config = Config({"target_dir": "memory://target.zarr"})
        self.assertEqual(False, config.dry_run)
        config = Config({"target_dir": "memory://target.zarr", "dry_run": True})
        self.assertEqual(True, config.dry_run)

    def test_slice_source_as_name(self):
        config = Config(
            {
                "target_dir": "memory://target.zarr",
                "slice_source": "tests.config.test_config.new_custom_slice_source",
            }
        )
        self.assertEqual(new_custom_slice_source, config.slice_source)

        config = Config(
            {
                "target_dir": "memory://target.zarr",
                "slice_source": "tests.config.test_config.CustomSliceSource",
            }
        )
        self.assertEqual(CustomSliceSource, config.slice_source)

        # staticmethod
        config = Config(
            {
                "target_dir": "memory://target.zarr",
                "slice_source": "tests.config.test_config.CustomSliceSource.new1",
            }
        )
        self.assertEqual(CustomSliceSource.new1, config.slice_source)

        # classmethod
        config = Config(
            {
                "target_dir": "memory://target.zarr",
                "slice_source": "tests.config.test_config.CustomSliceSource.new2",
            }
        )
        self.assertEqual(CustomSliceSource.new2, config.slice_source)

    def test_slice_source_as_type(self):
        config = Config(
            {
                "target_dir": "memory://target.zarr",
                "slice_source": new_custom_slice_source,
            }
        )
        self.assertIs(new_custom_slice_source, config.slice_source)

        config = Config(
            {
                "target_dir": "memory://target.zarr",
                "slice_source": CustomSliceSource,
            }
        )
        self.assertIs(CustomSliceSource, config.slice_source)

        with pytest.raises(
            TypeError,
            match=(
                "slice_source must a callable"
                " or the fully qualified name of a callable"
            ),
        ):
            Config(
                {
                    "target_dir": "memory://target.zarr",
                    "slice_source": 11,
                }
            )


def new_custom_slice_source(ctx: Context, index: int):
    return CustomSliceSource(ctx, index)


class CustomSliceSource(SliceSource):
    def __init__(self, ctx: Context, index: int):
        self.ctx = ctx
        self.index = index

    def get_dataset(self) -> xr.Dataset:
        return make_test_dataset(index=self.index)

    def dispose(self):
        pass

    @staticmethod
    def new1(ctx: Context, index: int):
        return CustomSliceSource(ctx, index)

    @classmethod
    def new2(cls, ctx: Context, index: int):
        return cls(ctx, index)
