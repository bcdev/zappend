# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import logging

from ..log import logger
from ..outline import DatasetOutline


def assert_compliance(target_outline: DatasetOutline,
                      slice_outline: DatasetOutline,
                      slice_name: str = ""):
    """Assert that the given *slice_outline* is compatible with
    given *target_outline*."""
    messages = target_outline.get_noncompliance(slice_outline)
    if messages:
        start_message = f"Incompatible slice dataset {slice_name}"
        logger.log(logging.ERROR, start_message)
        for message in messages:
            logger.info(message)
        raise ValueError(start_message.rstrip() + ", see logs for details")
