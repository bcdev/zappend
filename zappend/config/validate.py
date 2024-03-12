# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Any

import jsonschema
import jsonschema.exceptions

from .normalize import ConfigLike
from .normalize import normalize_config
from .schema import CONFIG_SCHEMA_V1


def validate_config(config_like: ConfigLike) -> dict[str, Any]:
    """Validate configuration and return normalized form.

    First normalizes the configuration-like value `config_like`
    using [normalize_config()][zappend.config.config.normalize_config],
    then validates and returns the result.

    Args:
        config_like: A configuration-like value.

    Returns:
        The normalized and validated configuration dictionary.
    """
    config = normalize_config(config_like)
    try:
        jsonschema.validate(config, CONFIG_SCHEMA_V1)
    except jsonschema.exceptions.ValidationError as e:
        raise ValueError(
            f"Invalid configuration: {e.message}" f" for {'.'.join(map(str, e.path))}"
        )
    return config
