# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import logging
import os
import unittest

from zappend.profiler import Profiler


class ProfilerTest(unittest.TestCase):
    def test_disabled(self):
        profiler = Profiler(None)
        self.assert_profiler_config(profiler, False)
        with profiler:
            pass

        profiler = Profiler(False)
        self.assert_profiler_config(profiler, False)
        with profiler:
            pass

        profiler = Profiler({"enabled": False})
        self.assert_profiler_config(profiler, False)
        with profiler:
            pass

        profiler = Profiler({"log_level": "NOTSET"})
        self.assert_profiler_config(profiler, False, "NOTSET")
        with profiler:
            pass

        profiler = Profiler({"enabled": True, "log_level": "NOTSET"})
        self.assert_profiler_config(profiler, False, "NOTSET")
        with profiler:
            pass

    def test_enabled(self):
        profiler = Profiler(True)
        self.assert_profiler_config(profiler, True, "INFO")
        with profiler:
            pass

        profiler = Profiler("prof.out")
        self.assert_profiler_config(profiler, True, "INFO", "prof.out")
        try:
            with profiler:
                pass
            self.assertTrue(os.path.exists("prof.out"))
        finally:
            if os.path.exists("prof.out"):
                os.remove("prof.out")

        profiler = Profiler({"path": "prof.out", "log_level": "DEBUG"})
        self.assert_profiler_config(profiler, True, "DEBUG", "prof.out")
        try:
            with profiler:
                pass
            self.assertTrue(os.path.exists("prof.out"))
        finally:
            if os.path.exists("prof.out"):
                os.remove("prof.out")

    def assert_profiler_config(
        self,
        profiler: Profiler,
        expected_enabled,
        expected_log_level="INFO",
        expected_path=None,
        expected_keys=None,
        expected_restrictions=None,
    ):
        if expected_keys is None:
            expected_keys = ["tottime"]
        if expected_restrictions is None:
            expected_restrictions = []
        self.assertEqual(expected_enabled, profiler.enabled)
        self.assertEqual(expected_log_level, profiler.log_level)
        self.assertEqual(expected_path, profiler.path)
        self.assertEqual(expected_keys, profiler.keys)
        self.assertEqual(expected_restrictions, profiler.restrictions)
