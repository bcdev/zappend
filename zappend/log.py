# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import logging
import logging.config
from typing import Any

logger = logging.getLogger("zappend")


_nameToLevel = {
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.FATAL,
    "ERROR": logging.ERROR,
    "WARN": logging.WARNING,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
    "NOTSET": logging.NOTSET,
}


def get_log_level(level_name: str) -> int:
    return _nameToLevel.get(level_name, logging.NOTSET)


def configure_logging(logging_config: dict[str, Any] | str | bool | None):
    if not logging_config:
        return
    if isinstance(logging_config, (str, bool)):
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
            "loggers": {
                "zappend": {
                    "level": _nameToLevel.get(logging_config, logging.INFO),
                    "handlers": ["console"],
                }
            },
        }
    logging.config.dictConfig(logging_config)
