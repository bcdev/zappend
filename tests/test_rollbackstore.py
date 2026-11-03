#  Copyright © 2024-2026 Brockmann Consult and contributors
#  Permissions are hereby granted under the terms of the MIT License:
#  https://opensource.org/licenses/MIT.

import asyncio
import unittest

from zarr.core.buffer import default_buffer_prototype
from zarr.storage import MemoryStore

from zappend.rollbackstore import RollbackStore


class RollbackStoreTest(unittest.TestCase):
    def test_records_only_the_initial_value_of_a_changed_key(self):
        async def run_test():
            records = []
            store = MemoryStore()
            prototype = default_buffer_prototype()
            await store.set("existing", prototype.buffer.from_bytes(b"before"))
            rollback_store = RollbackStore(store, lambda *args: records.append(args))

            await rollback_store.set("existing", prototype.buffer.from_bytes(b"one"))
            await rollback_store.set("existing", prototype.buffer.from_bytes(b"two"))
            await rollback_store.set("new", prototype.buffer.from_bytes(b"new"))

            self.assertEqual(
                [
                    ("replace_file", "existing", b"before"),
                    ("delete_file", "new", None),
                ],
                records,
            )

        asyncio.run(run_test())

    def test_records_batch_sets_and_deletions(self):
        async def run_test():
            records = []
            store = MemoryStore()
            prototype = default_buffer_prototype()
            await store.set("existing", prototype.buffer.from_bytes(b"before"))
            await store.set("dir/a", prototype.buffer.from_bytes(b"a"))
            await store.set("dir/b", prototype.buffer.from_bytes(b"b"))
            rollback_store = RollbackStore(store, lambda *args: records.append(args))

            await rollback_store.set_if_not_exists(
                "existing", prototype.buffer.from_bytes(b"ignored")
            )
            await rollback_store._set_many(
                [("batch", prototype.buffer.from_bytes(b"batch"))]
            )
            await rollback_store.delete("existing")
            await rollback_store.delete_dir("dir")

            self.assertEqual(
                [
                    ("replace_file", "existing", b"before"),
                    ("delete_file", "batch", None),
                    ("replace_file", "dir/a", b"a"),
                    ("replace_file", "dir/b", b"b"),
                ],
                records,
            )

        asyncio.run(run_test())
