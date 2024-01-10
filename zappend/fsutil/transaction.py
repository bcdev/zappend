# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import uuid
from typing import Callable, Literal

from zappend.fsutil.fileobj import FileObj
from zappend.log import logger

RollbackAction = (
    Literal["delete_dir"] | Literal["delete_file"] | Literal["replace_file"]
)

RollbackCallback = Callable[
    [
        RollbackAction,
        str,  # target path
        bytes | None,  # original data, if operation is "replace_file"
    ],
    None,  # void
]

LOCK_EXT = ".lock"
ROLLBACK_FILE = "__rollback__.txt"

ROLLBACK_ACTIONS = "delete_dir", "delete_file", "replace_file"


class Transaction:
    """
    A filesystem transaction.

    Its main motivation is implementing transactional Zarr dataset
    modifications, because this does not exist for Zarr yet (2024-01), see
    https://github.com/zarr-developers/zarr-python/issues/247.

    The ``Transaction`` class is used to observe changes to a given target
    directory *target_dir*.

    Changes must be explicitly registered using a "rollback callback"
    function that is provided as the result of using the
    transaction instance as a context manager:::

    with Transaction(target_dir, temp_dir) as rollback_cb:
        # emit rollback actions here

    The following actions are supported:

    * ``rollback_cb("delete_dir", path)`` if a directory has been created.
    * ``rollback_cb("delete_file", path)`` if a file has been created.
    * ``rollback_cb("replace_file", path, original_data)`` if a directory has
        been changed.

    Reported paths must be relative to *target_dir*. The empty path ``""``
    refers to *target_dir* itself.

    When entering the context, a lock file will be created which prevents
    other transactions to modify *target_dir*. The lock file will be placed
    next to *target_dir* and its name is the filename of *target_dir* with a
    ``.lock`` extension. The lock file will be removed on context exit.

    :param target_dir: The target directory that is subject to this
        transaction. All paths emitted to the rollback callback must be
        relative to *target_dir*. The directory may or may not exist yet.
    :param temp_dir: Temporary directory in which a unique subdirectory
        will be created that will be used to collect
        rollback data during the transaction. The directory must exist.
    :param disable_rollback: Disable rollback entirely.
        No rollback data will be written, however a lock file will still
        be created for the duration of the transaction.
    """

    def __init__(
        self, target_dir: FileObj, temp_dir: FileObj, disable_rollback: bool = False
    ):
        transaction_id = f"zappend-{uuid.uuid4()}"
        rollback_dir = temp_dir / transaction_id
        lock_file = target_dir.parent / (target_dir.filename + LOCK_EXT)
        self._id = transaction_id
        self._rollback_dir = rollback_dir
        self._rollback_file = rollback_dir / ROLLBACK_FILE
        self._target_dir = target_dir
        self._lock_file = lock_file
        self._disable_rollback = disable_rollback
        self._entered_ctx = False

    @property
    def target_dir(self) -> FileObj:
        """Target directory that is subject to this transaction."""
        return self._target_dir

    @property
    def lock_file(self) -> FileObj:
        """Temporary lock file used during the transaction."""
        return self._lock_file

    @property
    def rollback_dir(self) -> FileObj:
        """Temporary directory containing rollback data."""
        return self._rollback_dir

    def __enter__(self):
        if self._entered_ctx:
            raise ValueError(
                "Transaction instance cannot be used" " with nested 'with' statements"
            )
        self._entered_ctx = True

        lock_file = self._lock_file
        if lock_file.exists():
            raise IOError(f"Target is locked: {lock_file.uri}")
        lock_file.write(self._rollback_dir.uri)

        if not self._disable_rollback:
            self._rollback_dir.mkdir()
            self._rollback_file.write("")  # touch

        return self._add_rollback_action

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._assert_entered_ctx()

        if exc_type is not None and self._rollback_file.exists():
            logger.error("Error in transaction", exc_info=True)

            rollback_txt = self._rollback_file.read(mode="r")
            rollback_records = [
                record
                for record in [line.split() for line in rollback_txt.split("\n")]
                if record
            ]

            if rollback_records:
                logger.info(f"Rolling back {len(rollback_records)} action(s)")
                for record in rollback_records:
                    logger.debug(f"Running rollback {record}")
                    action = record[0]
                    args = record[1:]
                    action_method = getattr(self, "_" + action)
                    action_method(*args)

        if not self._disable_rollback:
            self._rollback_dir.delete(recursive=True)

        lock_file = self._lock_file
        try:
            lock_file.delete()
        except OSError:
            logger.warning(f"Failed to remove target lock: {lock_file.uri}")
            logger.warning("Note, it should be save to delete it manually.")

    def _delete_dir(self, target_path):
        _dir = self._target_dir / target_path
        _dir.delete(recursive=True)

    def _delete_file(self, target_path):
        _file = self._target_dir / target_path
        _file.delete()

    def _replace_file(self, target_path, rollback_filename):
        _file = self._target_dir / target_path
        data = (self._rollback_dir / rollback_filename).read()
        _file.write(data)

    def _add_rollback_action(
        self, action: RollbackAction, path: str, data: bytes | None
    ):
        self._assert_entered_ctx()
        if not isinstance(action, str):
            raise TypeError(
                f"Type of 'action' argument must be {str}," f" but was {type(action)}"
            )
        if action not in ROLLBACK_ACTIONS:
            actions = ", ".join(map(repr, ROLLBACK_ACTIONS))
            raise ValueError(
                f"Value of 'action' argument must be one of"
                f" {actions}, but was {action!r}"
            )
        if not isinstance(path, str):
            raise TypeError(
                f"Type of 'path' argument must be {str}," f" but was {type(path)}"
            )
        if action == "replace_file":
            if not isinstance(data, bytes):
                raise TypeError(
                    f"Type of 'data' argument must be {bytes}," f" but was {type(data)}"
                )
        else:
            if data is not None:
                raise ValueError(f"Value of 'data' argument must be None")

        if self._disable_rollback:
            return

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
            raise ValueError(
                "Transaction instance" " must be used with the 'with' statement"
            )
