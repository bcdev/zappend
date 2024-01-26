# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import logging
import os
import unittest

from zappend.profiler import Profiler


class ProfilerTest(unittest.TestCase):
    def test_disabled(self):
        profiler = Profiler(None)
        self.assertEqual(False, profiler.enabled)
        with profiler:
            pass

        profiler = Profiler(False)
        self.assertEqual(False, profiler.enabled)
        with profiler:
            pass

        profiler = Profiler({"enabled": False})
        self.assertEqual(False, profiler.enabled)
        with profiler:
            pass

        profiler = Profiler({"log_level": "NOTSET"})
        self.assertEqual(False, profiler.enabled)
        with profiler:
            pass

    def test_enabled(self):
        profiler = Profiler(True)
        self.assertEqual(True, profiler.enabled)
        self.assertEqual("INFO", profiler.log_level)
        self.assertEqual(None, profiler.path)
        with profiler:
            pass

        profiler = Profiler("prof.out")
        self.assertEqual(True, profiler.enabled)
        self.assertEqual("INFO", profiler.log_level)
        self.assertEqual("prof.out", profiler.path)
        try:
            with profiler:
                pass
            self.assertTrue(os.path.exists("prof.out"))
        finally:
            if os.path.exists("prof.out"):
                os.remove("prof.out")

        profiler = Profiler({"path": "prof.out", "log_level": "DEBUG"})
        self.assertEqual(True, profiler.enabled)
        self.assertEqual("DEBUG", profiler.log_level)
        self.assertEqual("prof.out", profiler.path)
        try:
            with profiler:
                pass
            self.assertTrue(os.path.exists("prof.out"))
        finally:
            if os.path.exists("prof.out"):
                os.remove("prof.out")
