# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import logging
import logging.config
from typing import Any

logger = logging.getLogger("zappend")


def configure_logging(logging_config: dict[str, Any] | None):
    if logging_config:
        logging.config.dictConfig(logging_config)
