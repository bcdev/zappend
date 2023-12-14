# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import time
from typing import Callable, Any, Sequence


class Poller:
    def __init__(self,
                 fn: Callable,
                 delay: float = 0.1,
                 timeout: float = 1):
        self.fn = fn
        self.delay = delay
        self.timeout = timeout

    def poll(self, *args, **kwargs) -> Any:
        t0 = time.monotonic()
        while True:
            result = self.fn(*args, **kwargs)
            if result is not None and result is not False:
                return result
            if (time.monotonic() - t0 + self.delay) > time.monotonic():
                raise TimeoutError()
            time.sleep(self.delay)
