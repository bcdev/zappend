#  Copyright © 2024-2026 Brockmann Consult and contributors
#  Permissions are hereby granted under the terms of the MIT License:
#  https://opensource.org/licenses/MIT.

"""Transactional wrapper for Zarr-Python v3 stores."""

from collections.abc import MutableMapping

import fsspec.implementations.local
from zarr.abc.store import Store
from zarr.core.buffer import default_buffer_prototype
from zarr.storage import FsspecStore, WrapperStore

from zappend.fsutil.transaction import RollbackCallback


class RollbackStore(WrapperStore[Store]):
    """Record each key's pre-transaction bytes before it is changed.

    This is a Zarr-Python v3 ``Store`` wrapper. The configured Zarr format
    (v2 or v3) is intentionally unrelated to this API implementation.
    """

    def __init__(self, store: Store, rollback_cb: RollbackCallback):
        super().__init__(store)
        self._rollback_cb = rollback_cb
        self._recorded_keys: set[str] = set()

    def _with_store(self, store: Store):
        return type(self)(store, self._rollback_cb)

    async def _record_before_change(self, key: str) -> None:
        if key in self._recorded_keys:
            return
        self._recorded_keys.add(key)
        old_value = await self._store.get(key, default_buffer_prototype())
        if old_value is None:
            self._rollback_cb("delete_file", key, None)
        else:
            self._rollback_cb("replace_file", key, old_value.to_bytes())

    async def set(self, key: str, value) -> None:
        await self._record_before_change(key)
        await self._store.set(key, value)

    async def set_if_not_exists(self, key: str, value) -> None:
        await self._record_before_change(key)
        await self._store.set_if_not_exists(key, value)

    async def _set_many(self, values) -> None:
        # Zarr batches metadata and chunk writes through this method.
        values = list(values)
        for key, _value in values:
            await self._record_before_change(key)
        await self._store._set_many(values)

    async def delete(self, key: str) -> None:
        await self._record_before_change(key)
        await self._store.delete(key)

    async def delete_dir(self, prefix: str) -> None:
        keys = [key async for key in self._store.list_prefix(prefix)]
        for key in keys:
            await self._record_before_change(key)
        await self._store.delete_dir(prefix)


def make_zarr_store(store: MutableMapping) -> Store:
    """Adapt an fsspec mapper to Zarr-Python v3's asynchronous Store API."""
    # Zarr v3 chunk keys are nested paths (for example ``time/c/1``). The
    # FsspecStore adapter writes them through LocalFileSystem.pipe_file(),
    # which only creates intermediate directories when auto_mkdir is enabled.
    # The mapper obtained from FileObj has it disabled by default.
    if isinstance(store.fs, fsspec.implementations.local.LocalFileSystem):
        store.fs.auto_mkdir = True
    return FsspecStore.from_mapper(store)


def make_rollback_store(store: MutableMapping, rollback_cb: RollbackCallback):
    """Return a transaction-recording wrapper around an fsspec mapper."""
    return RollbackStore(make_zarr_store(store), rollback_cb)
