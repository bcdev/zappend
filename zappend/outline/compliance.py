# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import logging
from ..log import logger
from ..outline import DatasetOutline


def check_compliance(target_outline: DatasetOutline,
                     slice_outline: DatasetOutline,
                     slice_uri: str,
                     error: bool = False) -> bool:
    messages = target_outline.get_noncompliance(slice_outline)
    if not messages:
        return True
    log_level = logging.ERROR if error else logging.WARNING
    logger.log(log_level, f"Incompatible slice dataset {slice_uri}")
    for message in messages:
        logger.info(message)
    return False
