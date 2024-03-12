# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from collections.abc import MutableMapping
from typing import Any, Sequence, Mapping

import zarr.context
import zarr.storage

from zappend.fsutil.transaction import RollbackCallback


class RollbackStore(zarr.storage.Store):
    def __init__(self, store: MutableMapping, rollback_cb: RollbackCallback):
        self._store = store
        self._rollback_cb = rollback_cb

    def _delegate_call(self, fn_name, *args, **kwargs) -> Any:
        if hasattr(self._store, fn_name):
            return getattr(self._store, fn_name)(*args, **kwargs)
        else:
            return getattr(super(), fn_name)(*args, **kwargs)

    ###########################################################################
    # collections.abc.Mapping implementations

    def __getitem__(self, key: str):
        return self._store[key]

    def __len__(self):
        return len(self._store)

    def __iter__(self):
        return iter(self._store)

    ###########################################################################
    # collections.abc.Mapping overrides

    def __contains__(self, key: str):
        return key in self._store

    def __eq__(self, other: Any):
        return self is other or (
            isinstance(other, RollbackStore)
            and self._rollback_cb is other._rollback_cb
            and self._store == other._store
        )

    ###########################################################################
    # collections.abc.MutableMapping implementations

    def __setitem__(self, key: str, value: bytes):
        old_value = self._store.get(key)
        self._store[key] = value
        if old_value is not None:
            self._rollback_cb("replace_file", key, old_value)
        else:
            self._rollback_cb("delete_file", key, None)

    def __delitem__(self, key: str):
        old_value = self._store.get(key)
        del self._store[key]
        if old_value is not None:
            self._rollback_cb("create_file", key, old_value)

    ###########################################################################
    # zarr.storage.BaseStore overrides

    # noinspection SpellCheckingInspection
    def getitems(
        self, keys: Sequence[str], *, contexts: Mapping[str, zarr.context.Context]
    ) -> Mapping[str, Any]:
        return self._delegate_call("getitems", keys, contexts)

    def rename(self, src_path: str, dst_path: str) -> None:
        self._delegate_call("rename", src_path, dst_path)
        self._rollback_cb("rename_file", dst_path, src_path)

    def close(self) -> None:
        self._delegate_call("close")

    ###########################################################################
    # zarr.storage.Store overrides

    def listdir(self, path: str = "") -> list[str]:
        return self._delegate_call("listdir", path)

    def rmdir(self, path: str = "") -> None:
        self._delegate_call("rmdir", path)
        self._rollback_cb("delete_dir", path)
