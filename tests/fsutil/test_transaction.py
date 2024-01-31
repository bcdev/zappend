# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
from typing import Callable

import pytest

from zappend.fsutil.fileobj import FileObj
from zappend.fsutil.transaction import Transaction
from zappend.fsutil.transaction import ROLLBACK_FILE
from ..helpers import clear_memory_fs


# noinspection PyShadowingNames
class TransactionTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    def test_transaction_success(self):
        self._run_transaction_test(fail=False, rollback=True)

    def test_transaction_failure_with_rollback(self):
        self._run_transaction_test(fail=True, rollback=True)

    def test_transaction_failure_without_rollback(self):
        self._run_transaction_test(fail=True, rollback=False)

    def _run_transaction_test(self, fail: bool, rollback: bool):
        test_root = FileObj("memory://test")
        test_root.mkdir()
        test_file_1 = test_root / "file-1.txt"
        test_file_2 = test_root / "file-2.txt"
        test_folder = test_root / "folder"
        test_file_3 = test_folder / "file_3.txt"
        test_file_1.write("A-B-C")
        self.assertTrue(test_root.exists())
        self.assertTrue(test_file_1.exists())
        self.assertFalse(test_file_2.exists())
        self.assertFalse(test_folder.exists())
        self.assertFalse(test_file_3.exists())

        temp_dir = FileObj("memory://temp")
        transaction = Transaction(test_root, temp_dir, disable_rollback=not rollback)

        self.assertEqual(test_root, transaction.target_dir)

        lock_file = transaction.lock_file
        self.assertFalse(lock_file.exists())

        rollback_dir = transaction.rollback_dir
        rollback_file = rollback_dir / ROLLBACK_FILE
        self.assertFalse(rollback_file.exists())

        def change_test_file_1(rollback_cb: Callable):
            original_data = test_file_1.read()
            test_file_1.write("D-E-F")
            rollback_cb("replace_file", test_file_1.filename, original_data)

        def create_test_file_2(rollback_cb: Callable):
            test_file_2.write("1-2-3")
            rollback_cb("delete_file", test_file_2.filename, None)

        def create_test_folder(rollback_cb: Callable):
            test_folder.mkdir()
            test_file_3.write("4-5-6")
            rollback_cb("delete_dir", test_folder.filename, None)

        try:
            with transaction as rollback_cb:
                self.assertTrue(lock_file.exists())
                self.assertEqual(rollback, rollback_file.exists())

                change_test_file_1(rollback_cb)
                create_test_file_2(rollback_cb)
                create_test_folder(rollback_cb)

                if rollback:
                    rollback_data = rollback_file.read(mode="rt")
                    rollback_records = [
                        line.split()[:2] for line in rollback_data.split("\n")
                    ]
                    self.assertEqual(
                        [
                            ["replace_file", "file-1.txt"],
                            ["delete_file", "file-2.txt"],
                            ["delete_dir", "folder"],
                            [],
                        ],
                        rollback_records,
                    )

                if fail:
                    raise OSError("disk full (this is a test!)")
        except OSError as e:
            if fail:
                self.assertEqual("disk full (this is a test!)", f"{e}")
            else:
                raise

        self.assertTrue(test_root.exists())
        self.assertFalse(lock_file.exists())
        self.assertFalse(rollback_dir.exists())

        if fail and rollback:
            self.assertTrue(test_root.exists())
            self.assertTrue(test_file_1.exists())  # replace_file by rollback
            self.assertEqual(b"A-B-C", test_file_1.read())
            self.assertFalse(test_file_2.exists())  # deleted_file by rollback
            self.assertFalse(test_folder.exists())  # deleted_dir by rollback
            self.assertFalse(test_file_3.exists())  # deleted_dir by rollback
        else:
            self.assertTrue(test_root.exists())
            self.assertTrue(test_file_1.exists())
            self.assertEqual(b"D-E-F", test_file_1.read())
            self.assertTrue(test_file_2.exists())
            self.assertEqual(b"1-2-3", test_file_2.read())
            self.assertTrue(test_folder.exists())
            self.assertTrue(test_file_3.exists())
            self.assertEqual(b"4-5-6", test_file_3.read())

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_nested_transaction(self):
        test_root = FileObj("memory://test")
        test_root.mkdir()
        rollback_dir = FileObj("memory://rollback")
        transaction = Transaction(test_root, rollback_dir)
        with transaction:
            with pytest.raises(
                ValueError,
                match="Transaction instance cannot be"
                " used with nested 'with' statements",
            ):
                with transaction:
                    pass

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_locked_target(self):
        test_root = FileObj("memory://test")
        test_root.mkdir()
        rollback_dir = FileObj("memory://rollback")
        with Transaction(test_root, rollback_dir):
            with pytest.raises(OSError, match="Target is locked: memory://test.lock"):
                with Transaction(test_root, rollback_dir):
                    pass

    # noinspection PyMethodMayBeStatic
    def test_it_raises_if_not_used_with_with(self):
        test_root = FileObj("memory://test")
        test_root.mkdir()
        rollback_dir = FileObj("memory://rollback")
        transaction = Transaction(test_root, rollback_dir)
        with pytest.raises(
            ValueError,
            match="Transaction instance must be" " used with the 'with' statement",
        ):
            transaction._add_rollback_action("delete_file", "path", None)

    def test_deletes_lock(self):
        test_root = FileObj("memory://test")
        test_root.mkdir()
        temp_dir = FileObj("memory://temp")
        temp_dir.mkdir()
        transaction = Transaction(test_root, temp_dir)
        self.assertFalse(transaction.lock_file.exists())
        with transaction:
            self.assertTrue(transaction.lock_file.exists())
        self.assertFalse(transaction.lock_file.exists())

    def test_leaves_lock_behind_when_it_cannot_be_deleted(self):
        test_root = FileObj("memory://test")
        test_root.mkdir()
        temp_dir = FileObj("memory://temp")
        temp_dir.mkdir()
        transaction = Transaction(test_root, temp_dir)
        delete_called = False

        def _delete():
            nonlocal delete_called
            delete_called = True
            raise OSError("Bam!")

        transaction.lock_file.delete = _delete
        self.assertFalse(transaction.lock_file.exists())
        with transaction:
            self.assertTrue(transaction.lock_file.exists())
        self.assertEqual(True, delete_called)
        self.assertTrue(transaction.lock_file.exists())

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_illegal_callback_calls(self):
        test_root = FileObj("memory://test")
        test_root.mkdir()
        rollback_dir = FileObj("memory://rollback")
        with pytest.raises(
            TypeError,
            match="Transaction._add_rollback_action\\(\\)"
            " missing 3 required positional arguments:"
            " 'action', 'path', and 'data'",
        ):
            with Transaction(test_root, rollback_dir) as callback:
                callback()

        with pytest.raises(
            TypeError,
            match="Type of 'action' argument must be"
            " <class 'str'>, but was <class 'int'>",
        ):
            with Transaction(test_root, rollback_dir) as callback:
                callback(42, "I/am/the/path", b"I/m/the/data")

        with pytest.raises(
            TypeError,
            match="Type of 'path' argument must be"
            " <class 'str'>, but was <class 'int'>",
        ):
            with Transaction(test_root, rollback_dir) as callback:
                callback("replace_file", 13, b"I/m/the/data")

        with pytest.raises(
            TypeError,
            match="Type of 'data' argument must be"
            " <class 'bytes'>, but was <class 'int'>",
        ):
            with Transaction(test_root, rollback_dir) as callback:
                callback("replace_file", "I/am/the/path", 0)

        with pytest.raises(ValueError, match="Value of 'data' argument must be None"):
            with Transaction(test_root, rollback_dir) as callback:
                callback("delete_file", "I/am/the/path", b"I/m/the/data")

        with pytest.raises(
            ValueError,
            match="Value of 'action' argument must be one of"
            " 'delete_dir',"
            " 'delete_file', 'replace_file',"
            " but was 'replace_ifle'",
        ):
            with Transaction(test_root, rollback_dir) as callback:
                callback("replace_ifle", "I/am/the/path", b"I/m/the/data")

    def test_paths_for_uri(self):
        t = Transaction(FileObj("memory:///target.zarr"), FileObj("memory:///temp"))
        self.assertEqual(FileObj("memory:///target.zarr"), t.target_dir)
        self.assertEqual(FileObj("memory:///target.zarr.lock"), t.lock_file)
        self.assertTrue(t.rollback_dir.uri.startswith("memory:///temp/zappend-"))
        # Note, 2 slashes only!
        t = Transaction(FileObj("memory://target.zarr"), FileObj("memory://temp"))
        self.assertEqual(FileObj("memory://target.zarr"), t.target_dir)
        self.assertEqual(FileObj("memory://target.zarr.lock"), t.lock_file)
        self.assertTrue(t.rollback_dir.uri.startswith("memory://temp/zappend-"))

    def test_paths_for_local(self):
        t = Transaction(FileObj("./target.zarr"), FileObj("temp"))
        self.assertEqual(FileObj("./target.zarr"), t.target_dir)
        self.assertEqual(FileObj("./target.zarr.lock"), t.lock_file)
        self.assertTrue(t.rollback_dir.uri.startswith("temp/zappend-"))

        t = Transaction(FileObj("out/target.zarr"), FileObj("out/temp"))
        self.assertEqual(FileObj("out/target.zarr"), t.target_dir)
        self.assertEqual(FileObj("out/target.zarr.lock"), t.lock_file)
        self.assertTrue(t.rollback_dir.uri.startswith("out/temp/zappend-"))

        t = Transaction(FileObj("/out/target.zarr"), FileObj("/out/temp"))
        self.assertEqual(FileObj("/out/target.zarr"), t.target_dir)
        self.assertEqual(FileObj("/out/target.zarr.lock"), t.lock_file)
        self.assertTrue(t.rollback_dir.uri.startswith("/out/temp/zappend-"))

        t = Transaction(FileObj("target.zarr"), FileObj("temp"))
        self.assertEqual(FileObj("target.zarr"), t.target_dir)
        self.assertEqual(FileObj("target.zarr.lock"), t.lock_file)
        self.assertTrue(t.rollback_dir.uri.startswith("temp/zappend-"))
