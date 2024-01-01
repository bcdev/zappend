# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest
from typing import Callable

import pytest

from zappend.fsutil.fileobj import FileObj
from zappend.fsutil.transaction import Transaction
from zappend.fsutil.transaction import LOCK_EXT
from zappend.fsutil.transaction import ROLLBACK_FILE
from ..helpers import clear_memory_fs


# noinspection PyShadowingNames
class TransactionTest(unittest.TestCase):

    def setUp(self):
        clear_memory_fs()

    def test_transaction_success(self):
        self._run_transaction_test(fail=False)

    def test_transaction_with_rollback(self):
        self._run_transaction_test(fail=True)

    def _run_transaction_test(self, fail: bool):

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

        lock_file = test_root.parent / (test_root.filename + LOCK_EXT)
        self.assertFalse(lock_file.exists())

        rollback_dir = FileObj("memory://rollback")
        rollback_file = rollback_dir / ROLLBACK_FILE
        self.assertFalse(rollback_file.exists())

        def change_test_file_1(rollback_cb: Callable):
            original_data = test_file_1.read()
            test_file_1.write("D-E-F")
            rollback_cb("replace_file", test_file_1.path, original_data)

        def create_test_file_2(rollback_cb: Callable):
            test_file_2.write("1-2-3")
            rollback_cb("delete_file", test_file_2.path, None)

        def create_test_folder(rollback_cb: Callable):
            test_folder.mkdir()
            test_file_3.write("4-5-6")
            rollback_cb("delete_dir", test_folder.path, None)

        try:
            with Transaction(test_root, rollback_dir,
                             create_rollback_subdir=False) as rollback_cb:
                self.assertTrue(rollback_file.exists())
                self.assertTrue(lock_file.exists())

                change_test_file_1(rollback_cb)
                create_test_file_2(rollback_cb)
                create_test_folder(rollback_cb)

                rollback_data = rollback_file.read(mode="rt")
                rollback_records = [line.split()[:2]
                                    for line in rollback_data.split("\n")]
                self.assertEqual(
                    [
                        ["replace_file", "/test/file-1.txt"],
                        ["delete_file", "/test/file-2.txt"],
                        ["delete_dir", "/test/folder"],
                        []
                    ],
                    rollback_records
                )

                if fail:
                    raise OSError("disk full")
        except OSError as e:
            if fail:
                self.assertEqual("disk full", f"{e}")
            else:
                raise

        self.assertTrue(test_root.exists())
        self.assertFalse(lock_file.exists())
        self.assertFalse(rollback_dir.exists())

        if fail:
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
            with pytest.raises(ValueError,
                               match="Transaction instance cannot be"
                                     " used with nested 'with' statements"):
                with transaction:
                    pass

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_locked_target(self):
        test_root = FileObj("memory://test")
        test_root.mkdir()
        rollback_dir = FileObj("memory://rollback")
        with Transaction(test_root, rollback_dir):
            with pytest.raises(OSError,
                               match="Target is locked: memory:///test.lock"):
                with Transaction(test_root, rollback_dir):
                    pass

    # noinspection PyMethodMayBeStatic
    def test_it_raises_if_not_used_with_with(self):
        test_root = FileObj("memory://test")
        test_root.mkdir()
        rollback_dir = FileObj("memory://rollback")
        transaction = Transaction(test_root, rollback_dir)
        with pytest.raises(ValueError,
                           match="Transaction instance must be"
                                 " used with the 'with' statement"):
            transaction._add_rollback_action("delete_file", "path", None)

    def test_deletes_lock(self):
        test_root = FileObj("memory://test")
        test_root.mkdir()
        rollback_dir = FileObj("memory://rollback")
        rollback_dir.mkdir()
        transaction = Transaction(test_root, rollback_dir,
                                  create_rollback_subdir=False)
        self.assertFalse(transaction._lock_file.exists())
        with transaction:
            self.assertTrue(transaction._lock_file.exists())
        self.assertFalse(transaction._lock_file.exists())

    def test_leaves_lock_behind_when_it_cannot_be_deleted(self):
        test_root = FileObj("memory://test")
        test_root.mkdir()
        rollback_dir = FileObj("memory://rollback")
        rollback_dir.mkdir()
        transaction = Transaction(test_root, rollback_dir,
                                  create_rollback_subdir=False)
        delete_called = False

        def _delete():
            nonlocal delete_called
            delete_called = True
            raise OSError("Bam!")

        transaction._lock_file.delete = _delete
        self.assertFalse(transaction._lock_file.exists())
        with transaction:
            self.assertTrue(transaction._lock_file.exists())
        self.assertEqual(True, delete_called)
        self.assertTrue(transaction._lock_file.exists())

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_illegal_callback_calls(self):
        test_root = FileObj("memory://test")
        test_root.mkdir()
        rollback_dir = FileObj("memory://rollback")
        with pytest.raises(TypeError,
                           match="Transaction._add_rollback_action\\(\\)"
                                 " missing 3 required positional arguments:"
                                 " 'action', 'path', and 'data'"):
            with Transaction(test_root, rollback_dir) as callback:
                callback()

        with pytest.raises(TypeError,
                           match="Type of 'action' argument must be"
                                 " <class 'str'>, but was <class 'int'>"):
            with Transaction(test_root, rollback_dir) as callback:
                callback(42, "I/am/the/path", b'I/m/the/data')

        with pytest.raises(TypeError,
                           match="Type of 'path' argument must be"
                                 " <class 'str'>, but was <class 'int'>"):
            with Transaction(test_root, rollback_dir) as callback:
                callback("replace_file", 13, b'I/m/the/data')

        with pytest.raises(TypeError,
                           match="Type of 'data' argument must be"
                                 " <class 'bytes'>, but was <class 'int'>"):
            with Transaction(test_root, rollback_dir) as callback:
                callback("replace_file", "I/am/the/path", 0)

        with pytest.raises(ValueError,
                           match="Value of 'data' argument must be None"):
            with Transaction(test_root, rollback_dir) as callback:
                callback("delete_file", "I/am/the/path", b'I/m/the/data')

        with pytest.raises(ValueError,
                           match="Value of 'action' argument must be one of"
                                 " 'delete_dir',"
                                 " 'delete_file', 'replace_file',"
                                 " but was 'replace_ifle'"):
            with Transaction(test_root, rollback_dir) as callback:
                callback("replace_ifle", "I/am/the/path", b'I/m/the/data')
