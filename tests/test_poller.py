# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import unittest

from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
import random

from zappend.poller import Poller


def is_subset_available(path: str) -> bool:
    success = random.randint(1, 2) == 1
    print(f"Checking {path}, success = {success}")
    return success


class PollerTest(unittest.TestCase):
    def test_with_thread_pool_executor(self):
        self.assert_poller_ok(ThreadPoolExecutor(max_workers=4))

    def test_with_process_pool_executor(self):
        self.assert_poller_ok(ProcessPoolExecutor(max_workers=4))

    def assert_poller_ok(self, executor_cm):
        poller = Poller(is_subset_available, delay=0.1, timeout=1)
        paths = [f"s{i}.zarr" for i in range(10)]
        with executor_cm as executor:
            futures = [executor.submit(poller.poll, path)
                       for path in paths]
        results = [f.result() for f in futures]
        self.assertEqual(10 * [True], results)
