# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import logging
from typing import Literal

from ..log import logger
from ..outline import DatasetOutline


def check_compliance(
    target_outline: DatasetOutline,
    slice_outline: DatasetOutline,
    slice_name: str = "",
    on_error: Literal["warn"] | Literal["raise"] = "raise"
) -> bool:
    messages = target_outline.get_noncompliance(slice_outline)
    if not messages:
        return True
    log_level = logging.ERROR if on_error == "raise" else logging.WARNING
    start_message = f"Incompatible slice dataset {slice_name}"
    logger.log(log_level, start_message)
    for message in messages:
        logger.info(message)
    if on_error == "raise":
        raise ValueError(start_message.rstrip() + ", see logs for details")
    return False
