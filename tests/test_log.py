# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
import logging
import unittest
from click.testing import CliRunner

from zappend.log import logger
from zappend.log import configure_logging


class LogTest(unittest.TestCase):
    def test_logger(self):
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logging.NOTSET, logger.level)
        self.assertEqual(1, len(logger.handlers))

    def test_configure_logging(self):
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

        old_level = logger.level
        old_handlers = list(logger.handlers)
        for h in old_handlers:
            logger.removeHandler(h)

        try:
            configure_logging(logging_config)
            self.assertEqual(logging.INFO, logger.level)
            self.assertEqual(1, len(logger.handlers))
        finally:
            logger.setLevel(old_level)
            for h in old_handlers:
                logger.addHandler(h)
