# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import cProfile
import io
import pstats
from typing import Any

from .log import get_log_level
from .log import logger


class Profiler:
    def __init__(self, profiling_config: dict[str, Any] | str | bool | None):
        """A simple profiler that uses the `cProfile` module.

        Intended to be used as context manager.
        For internal use only; this class does not belong to the API.

        Args:
             profiling_config: The validated(!) "profiling" setting from
             the configuration.
        """

        # Defaults
        path = None
        log_level = "INFO"
        keys = ["tottime"]
        restrictions = []

        if isinstance(profiling_config, str):
            enabled = True
            path = profiling_config
        elif isinstance(profiling_config, dict):
            enabled = profiling_config.get("enabled", True)
            log_level = profiling_config.get("log_level", log_level)
            path = profiling_config.get("path", path)
            keys = profiling_config.get("keys", keys)
            restrictions = profiling_config.get("restrictions", restrictions)
        else:
            assert profiling_config in (True, False, None)
            enabled = bool(profiling_config)

        self.enabled = enabled and (bool(path) or log_level != "NOTSET")
        self.path = path
        self.log_level = log_level
        self.keys = keys
        self.restrictions = restrictions
        self._profile: cProfile.Profile | None = None

    def __enter__(self):
        if not self.enabled:
            return
        logger.info(f"Profiling...")
        self._profile = cProfile.Profile()
        self._profile.enable()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._profile is None:
            return
        self._profile.disable()
        results = io.StringIO()
        stats = pstats.Stats(self._profile, stream=results)
        stats.sort_stats(*self.keys).print_stats(*self.restrictions)
        if self.log_level != "NOTSET":
            log_level = get_log_level(self.log_level)
            logger.log(log_level, "Profiling result:\n" + results.getvalue())
        if self.path:
            with open(self.path, "w") as f:
                f.write(results.getvalue())
            logger.info(f"Profiling output written to {self.path}")
