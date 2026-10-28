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
            rollback_store = RollbackStore(store, records.append)

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
