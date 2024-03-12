# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import logging
import unittest

from zappend.log import configure_logging
from zappend.log import get_log_level
from zappend.log import logger


class LogTest(unittest.TestCase):
    old_level = None
    old_handlers = None

    @classmethod
    def setUpClass(cls):
        cls.old_level = logger.level
        cls.old_handlers = list(logger.handlers)
        for h in cls.old_handlers:
            logger.removeHandler(h)

    def tearDown(self):
        logger.setLevel(self.old_level)
        for h in self.old_handlers:
            logger.addHandler(h)

    def test_logger(self):
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logging.NOTSET, logger.level)
        self.assertEqual(1, len(logger.handlers))

    def test_configure_logging_dict(self):
        logging_config = {
            "version": 1,
            "formatters": {
                "normal": {
                    "format": "%(asctime)s %(levelname)s %(message)s",
                    "style": "%",
                }
            },
            "handlers": {
                "console": {"class": "logging.StreamHandler", "formatter": "normal"}
            },
            "loggers": {"zappend": {"level": "INFO", "handlers": ["console"]}},
        }

        configure_logging(logging_config)
        self.assertEqual(logging.INFO, logger.level)
        self.assertEqual(1, len(logger.handlers))
        self.assertIsInstance(logger.handlers[0], logging.StreamHandler)

    def test_configure_logging_false(self):
        logging_config = False
        configure_logging(logging_config)
        self.assertEqual(logging.NOTSET, logger.level)
        self.assertEqual(1, len(logger.handlers))
        self.assertIsInstance(logger.handlers[0], logging.StreamHandler)

    def test_configure_logging_true(self):
        logging_config = True
        configure_logging(logging_config)
        self.assertEqual(logging.INFO, logger.level)
        self.assertEqual(1, len(logger.handlers))
        self.assertIsInstance(logger.handlers[0], logging.StreamHandler)

    def test_configure_logging_level(self):
        logging_config = "WARNING"
        configure_logging(logging_config)
        self.assertEqual(logging.WARNING, logger.level)
        self.assertEqual(1, len(logger.handlers))
        self.assertIsInstance(logger.handlers[0], logging.StreamHandler)

    def test_get_log_level(self):
        self.assertEqual(logging.DEBUG, get_log_level("DEBUG"))
        self.assertEqual(logging.INFO, get_log_level("INFO"))
        self.assertEqual(logging.WARNING, get_log_level("WARN"))
        self.assertEqual(logging.WARNING, get_log_level("WARNING"))
        self.assertEqual(logging.ERROR, get_log_level("ERROR"))
        self.assertEqual(logging.CRITICAL, get_log_level("CRITICAL"))
        self.assertEqual(logging.NOTSET, get_log_level("NOTSET"))
        self.assertEqual(logging.NOTSET, get_log_level("CRASS"))
