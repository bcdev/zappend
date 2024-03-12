# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import copy
import unittest

import pytest
import xarray as xr
import zarr.storage

from zappend.fsutil.fileobj import FileObj
from zappend.rollbackstore import RollbackStore
from .helpers import clear_memory_fs
from .helpers import make_test_dataset


class RollbackStoreImplTest(unittest.TestCase):
    def setUp(self):
        self.mem_store = zarr.storage.MemoryStore()
        self.rb_records = []
        self.rb_store = RollbackStore(self.mem_store, self.add_rb_record)

    def add_rb_record(self, *args):
        self.rb_records.append(args)

    def test_getitem(self):
        with pytest.raises(KeyError):
            # noinspection PyUnusedLocal
            v1 = self.rb_store["k1"]
        self.assertIsNone(self.rb_store.get("k1"))
        self.mem_store["k1"] = b"v1"
        self.assertEqual(b"v1", self.rb_store["k1"])
        self.assertEqual(b"v1", self.rb_store.get("k1"))

    def test_setitem(self):
        self.rb_store["k1"] = b"v1"
        self.assertEqual(b"v1", self.rb_store["k1"])
        self.assertEqual(b"v1", self.rb_store.get("k1"))
        self.assertEqual(b"v1", self.mem_store["k1"])
        self.rb_store["k1"] = b"v2"
        self.assertEqual(b"v2", self.rb_store["k1"])
        self.assertEqual(b"v2", self.rb_store.get("k1"))
        self.assertEqual(b"v2", self.mem_store["k1"])
        self.assertEqual(
            [("delete_file", "k1", None), ("replace_file", "k1", b"v1")],
            self.rb_records,
        )

    def test_delitem(self):
        with pytest.raises(KeyError):
            del self.rb_store["k1"]
        self.rb_store["k1"] = b"v1"
        del self.rb_store["k1"]
        with pytest.raises(KeyError):
            del self.rb_store["k1"]
        self.rb_store["k1"] = b"v1"
        v1 = self.rb_store.pop("k1")
        self.assertEqual(b"v1", v1)
        self.assertNotIn("k1", self.rb_store)
        self.assertEqual(
            [
                ("delete_file", "k1", None),
                ("create_file", "k1", b"v1"),
                ("delete_file", "k1", None),
                ("create_file", "k1", b"v1"),
            ],
            self.rb_records,
        )

    def test_contains(self):
        self.assertFalse("k1" in self.rb_store)
        self.rb_store["k1"] = b"v1"
        self.assertTrue("k1" in self.rb_store)

    def test_len(self):
        self.assertEqual(0, len(self.rb_store))
        self.rb_store["k1"] = b"v1"
        self.assertEqual(1, len(self.rb_store))
        self.rb_store["k2"] = b"v2"
        self.assertEqual(2, len(self.rb_store))
        self.rb_store["k2"] = b"v3"
        self.assertEqual(2, len(self.rb_store))

    def test_iter(self):
        self.rb_store["k1"] = b"v1"
        self.rb_store["k2"] = b"v2"
        self.assertEqual({"k1", "k2"}, set(iter(self.rb_store)))

    def test_eq(self):
        self.rb_store["k1"] = b"v1"
        self.rb_store["k2"] = b"v2"
        self.assertTrue(self.rb_store == self.rb_store)
        self.assertFalse(self.rb_store == {})
        self.assertFalse(self.rb_store == {"k1": b"v1", "k2": b"v2"})
        self.assertTrue(self.rb_store == copy.copy(self.rb_store))

    def test_rename(self):
        self.rb_store["k1"] = b"v1"
        self.rb_store.rename("k1", "k2")
        self.assertEqual(b"v1", self.rb_store.get("k2"))
        self.assertEqual(
            [("delete_file", "k1", None), ("rename_file", "k2", "k1")], self.rb_records
        )

    def test_close(self):
        # Just a smoke test, we have no criteria whether this was successful
        self.rb_store.close()

    def test_rmdir(self):
        self.rb_store["a/k1"] = b"v1"
        self.rb_store["a/k2"] = b"v2"
        self.rb_store.rmdir("a")
        self.assertFalse("a/k1" in self.rb_store)
        self.assertFalse("a/k2" in self.rb_store)
        self.assertEqual(0, len(self.rb_store))
        self.assertEqual(
            [
                ("delete_file", "a/k1", None),
                ("delete_file", "a/k2", None),
                ("delete_dir", "a"),
            ],
            self.rb_records,
        )


class RollbackStoreZarrTest(unittest.TestCase):
    target_dir = FileObj("memory://target.zarr")

    def setUp(self):
        clear_memory_fs()
        self.records = []

    def handle_rollback_action(self, *args):
        self.records.append(args)

    def test_to_zarr(self):
        ds = make_test_dataset(shape=(1, 50, 100), chunks=(2, 50, 50), crs="epsg:4326")
        ds.time.encoding.update(chunks=(10,))
        ds.chl.encoding.update(chunks=(2, 50, 50))
        ds.tsm.encoding.update(chunks=(2, 50, 50))
        # ds.time.encoding.update(chunks=(10,))
        ds.to_zarr(
            RollbackStore(
                self.target_dir.fs.get_mapper(
                    root=self.target_dir.path.strip("/"), create=True
                ),
                self.handle_rollback_action,
            )
        )
        self.assert_dataset_ok(
            {"x": 100, "y": 50, "time": 1},
            {
                "crs": (),
                "x": (100,),
                "y": (50,),
                "time": (10,),
                "chl": (2, 50, 50),
                "tsm": (2, 50, 50),
            },
        )

        self.assertEqual(
            {
                ("delete_file", ".zmetadata"),
                ("delete_file", ".zgroup"),
                ("delete_file", ".zattrs"),
                ("delete_file", "x/.zarray"),
                ("delete_file", "x/.zattrs"),
                ("delete_file", "x/0"),
                ("delete_file", "y/.zarray"),
                ("delete_file", "y/.zattrs"),
                ("delete_file", "y/0"),
                ("delete_file", "time/.zarray"),
                ("delete_file", "time/.zattrs"),
                ("delete_file", "time/0"),
                ("delete_file", "crs/.zarray"),
                ("delete_file", "crs/.zattrs"),
                ("delete_file", "crs/0"),
                ("delete_file", "chl/.zarray"),
                ("delete_file", "chl/.zattrs"),
                ("delete_file", "chl/0.0.0"),
                ("delete_file", "chl/0.0.1"),
                ("delete_file", "tsm/.zarray"),
                ("delete_file", "tsm/.zattrs"),
                ("delete_file", "tsm/0.0.0"),
                ("delete_file", "tsm/0.0.1"),
            },
            set([r[:2] for r in self.records]),
        )

        #####################################################################
        # Add slice 1

        self.records = []
        slice_1 = make_test_dataset(shape=(1, 50, 100), chunks=(1, 50, 50))
        # drop variables w.o. "time" dim
        slice_1 = slice_1.drop_vars(
            [k for k, v in slice_1.variables.items() if "time" not in v.sizes]
        )
        slice_1.attrs = {}
        for k, v in slice_1.variables.items():
            v.encoding = {}
            v.attrs = {}

        slice_1.to_zarr(
            RollbackStore(
                self.target_dir.fs.get_mapper(
                    root=self.target_dir.path.strip("/"), create=False
                ),
                self.handle_rollback_action,
            ),
            mode="a",
            append_dim="time",
        )
        self.assert_dataset_ok(
            {"x": 100, "y": 50, "time": 2},
            {
                "crs": (),
                "x": (100,),
                "y": (50,),
                "time": (10,),
                "chl": (2, 50, 50),
                "tsm": (2, 50, 50),
            },
        )
        self.assertEqual(
            {
                ("replace_file", ".zmetadata"),
                ("replace_file", ".zattrs"),
                ("replace_file", "time/.zarray"),
                ("replace_file", "time/0"),
                ("replace_file", "chl/.zarray"),
                ("replace_file", "chl/0.0.0"),
                ("replace_file", "chl/0.0.1"),
                ("replace_file", "tsm/.zarray"),
                ("replace_file", "tsm/0.0.0"),
                ("replace_file", "tsm/0.0.1"),
            },
            set([r[:2] for r in self.records]),
        )

        #####################################################################
        # Add slice 2

        self.records = []
        slice_2 = make_test_dataset(shape=(1, 50, 100), chunks=(1, 50, 50))
        # drop variables w.o. "time" dim
        slice_2 = slice_2.drop_vars(
            [k for k, v in slice_2.variables.items() if "time" not in v.sizes]
        )
        for k, v in slice_2.variables.items():
            v.encoding = {}
            v.attrs = {}

        slice_2.to_zarr(
            RollbackStore(
                self.target_dir.fs.get_mapper(
                    root=self.target_dir.path.strip("/"), create=False
                ),
                self.handle_rollback_action,
            ),
            mode="a",
            append_dim="time",
        )
        self.assert_dataset_ok(
            {"x": 100, "y": 50, "time": 3},
            {
                "crs": (),
                "x": (100,),
                "y": (50,),
                "time": (10,),
                "chl": (2, 50, 50),
                "tsm": (2, 50, 50),
            },
        )
        self.assertEqual(
            {
                ("replace_file", ".zmetadata"),
                ("replace_file", ".zattrs"),
                ("replace_file", "time/.zarray"),
                ("replace_file", "time/0"),
                ("replace_file", "chl/.zarray"),
                ("delete_file", "chl/1.0.0"),
                ("delete_file", "chl/1.0.1"),
                ("replace_file", "tsm/.zarray"),
                ("delete_file", "tsm/1.0.0"),
                ("delete_file", "tsm/1.0.1"),
            },
            set([r[:2] for r in self.records]),
        )

    def assert_dataset_ok(
        self,
        expected_sizes: dict[str, int],
        expected_chunks: dict[str, tuple[int, ...]],
    ):
        ds = xr.open_zarr(self.target_dir.uri)
        self.assertEqual(expected_sizes, ds.sizes)
        self.assertEqual(
            expected_chunks,
            {k: ds[k].encoding.get("chunks") for k in ds.variables.keys()},
        )
