# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import time
from typing import Callable, Any, Tuple


class Poller:
    def __init__(self,
                 fn: Callable,
                 delay: float = 0.1,
                 timeout: float = 1,
                 invalids: Tuple[Any, ...] = (None, False)):
        self.fn = fn
        self.delay = delay
        self.timeout = timeout
        self.invalids = set(invalids)

    def poll(self, *args, **kwargs) -> Any:
        t0 = time.monotonic()
        while True:
            result = self.fn(*args, **kwargs)
            if not self.invalids or result not in self.invalids:
                return result

            # Make sure we no longer refer to result while sleeping
            # noinspection PyUnusedLocal
            result = None
            del result

            if (time.monotonic() - t0 + self.delay) > self.timeout:
                raise TimeoutError()
            time.sleep(self.delay)
