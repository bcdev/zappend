# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import uuid
from typing import Callable, Literal

from zappend.fsutil.fileobj import FileObj
from zappend.log import logger

RollbackAction = (Literal["delete_dir"]
                  | Literal["delete_file"]
                  | Literal["replace_file"])

RollbackCallback = Callable[
    [
        RollbackAction,
        # TODO: Check if we should use FileObj instead of path
        str,  # target path
        bytes | None  # original data, if operation is "replace_file"
    ],
    None  # void
]

LOCK_FILE = "__rollback__.lock"
ROLLBACK_FILE = "__rollback__.txt"

ROLLBACK_ACTIONS = {"delete_dir", "delete_file", "replace_file"}


class Transaction:
    """
    A filesystem transaction.

    See https://github.com/zarr-developers/zarr-python/issues/247
    """

    def __init__(self,
                 target_dir: FileObj,
                 rollback_dir: FileObj,
                 create_rollback_subdir: bool = True):
        lock_file = target_dir.parent / LOCK_FILE
        if lock_file.exists():
            raise IOError(f"Target is locked: {lock_file.uri}")
        transaction_id = str(uuid.uuid4())
        if create_rollback_subdir:
            rollback_dir = rollback_dir / transaction_id
        self._id = transaction_id
        self._rollback_dir = rollback_dir
        self._rollback_file = rollback_dir / ROLLBACK_FILE
        self._target_dir = target_dir
        self._lock_file = lock_file
        self._entered_ctx = False

    def __enter__(self):
        if self._entered_ctx:
            raise ValueError("Transaction instance cannot be used"
                             " with nested 'with' statements")
        self._entered_ctx = True

        if not self._rollback_dir.exists():
            self._rollback_dir.mkdir()
        self._rollback_file.write("")  # touch

        lock_file = self._lock_file
        if lock_file.exists():
            raise IOError(f"Target is locked: {lock_file.uri}")
        lock_file.write(self._rollback_dir.uri)

        return self._add_rollback_op

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._assert_entered_ctx()

        if exc_type is not None and self._rollback_file.exists():
            rollback_txt = self._rollback_file.read(mode="r")
            rollback_records = [record
                                for record in [
                                    line.split()
                                    for line in rollback_txt.split("\n")
                                ] if record]

            for record in rollback_records:
                logger.debug(f"Running rollback {record}")
                action = record[0]
                args = record[1:]
                action_method = getattr(self, "_" + action)
                action_method(*args)

        self._rollback_dir.delete(recursive=True)

        lock_file = self._lock_file
        try:
            lock_file.delete()
        except OSError:
            logger.warning(f"Failed to remove target lock: {lock_file.uri}")
            logger.warning("Note, it should be save to delete it manually.")

    def _delete_dir(self, target_path):
        self._target_dir.fs.rm(target_path, recursive=True)

    def _delete_file(self, target_path):
        self._target_dir.fs.rm(target_path)

    def _replace_file(self, target_path, rollback_filename):
        data = (self._rollback_dir / rollback_filename).read()
        with self._target_dir.fs.open(target_path, "wb") as f:
            f.write(data)

    def _add_rollback_op(self,
                         action: RollbackAction,
                         path: str,
                         data: bytes | None):
        self._assert_entered_ctx()
        if not isinstance(action, str):
            raise TypeError(
                f"action must of type str, but was {type(action)}")
        if not isinstance(path, str):
            raise TypeError(f"path must of type str, but was {type(path)}")
        if not isinstance(data, (bytes, type(None))):
            raise TypeError(f"data must be None or of type bytes,"
                            f" but was {type(data)}")
        if action not in ROLLBACK_ACTIONS:
            raise ValueError(f"action must be one of"
                             f" {', '.join(ROLLBACK_ACTIONS)},"
                             f" but was {action}")

        assert hasattr(self, "_" + action)
        if data is not None:
            backup_id = str(uuid.uuid4())
            backup_file = self._rollback_dir.for_path(backup_id)
            backup_file.write(data)
            rollback_entry = f"{action} {path} {backup_id}"
        else:
            rollback_entry = f"{action} {path}"

        logger.debug(f"Recording rollback record: {rollback_entry!r}")
        self._rollback_file.write(rollback_entry + "\n", mode="a")

    def _assert_entered_ctx(self):
        if not self._entered_ctx:
            raise ValueError("Transaction instance"
                             " must be used with the 'with' statement")