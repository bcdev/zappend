# Copyright © 2024 Norman Fomferra
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


def configure_logging(logging_config: dict[str, Any] | None):
    if logging_config:
        logging.config.dictConfig(logging_config)
