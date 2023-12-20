# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import uuid

from .fileobj import FileObj
from .log import logger
from .transmit import RollbackOp

LOCK_FILE = "__rollback__.lock"
ROLLBACK_FILE = "__rollback__.txt"


class Transaction:
    def __init__(self,
                 target_dir: FileObj,
                 rollback_dir: FileObj,
                 create_rollback_subdir: bool = True):
        lock_file = target_dir / LOCK_FILE
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
        self._in_use = False

    def __enter__(self):
        if self._in_use:
            raise ValueError("a Transaction instance can only be used once")

        self._in_use = True

        lock_file = self._lock_file
        if lock_file.exists():
            raise IOError(f"Target is locked: {lock_file.uri}")
        lock_file.write(self._rollback_dir.uri)

        if not self._rollback_dir.exists():
            self._rollback_dir.mkdir()

        self._rollback_file.write("")

        return self._add_rollback_op

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._assert_ctx_mgt()

        if exc_type is not None and self._rollback_file.exists():
            rollback_txt = self._rollback_file.read(mode="r")
            rollback_records = [record
                                for record in [
                                    line.split()
                                    for line in rollback_txt.split("\n")
                                ] if record]

            for record in rollback_records:
                if record:
                    logger.debug(f"Rolling back: {record}")
                    op = record[0]
                    args = record[1:]
                    # print(f"Rolling back: {op} with args {args}")
                    op_method_name = "_" + op
                    assert hasattr(self, op_method_name)
                    op_method = getattr(self, op_method_name)
                    op_method(*args)

        self._rollback_dir.delete(recursive=True)

        lock_file = self._lock_file
        try:
            lock_file.delete()
        except OSError:
            logger.warning(f"Failed to remove target lock: {lock_file.uri}")
            logger.warning("Note, it should be save to delete it manually.")

    def _replace_file(self, target_path, rollback_filename):
        data = (self._rollback_dir / rollback_filename).read()
        with self._target_dir.fs.open(target_path, "wb") as f:
            f.write(data)

    def _add_rollback_op(self,
                         op: RollbackOp,
                         target_path: str,
                         source_data: bytes | None):
        self._assert_ctx_mgt()
        if source_data is not None:
            backup_id = str(uuid.uuid4())
            backup_file = self._rollback_dir.for_path(backup_id)
            backup_file.write(source_data)
            rollback_entry = f"{op} {target_path} {backup_id}"
        else:
            rollback_entry = f"{op} {target_path}"
        self._rollback_file.write(rollback_entry + "\n", mode="a")

    def _assert_ctx_mgt(self):
        if not self._in_use:
            raise ValueError("Transaction instance"
                             " must be used as context manager")
