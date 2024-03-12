# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import contextlib
import io
import json
import os.path
import string
from typing import Any

import yaml

from zappend.fsutil.fileobj import FileObj
from zappend.log import logger

ConfigItem = FileObj | str | dict[str, Any]
"""The possible types used to represent zappend configuration."""

ConfigList = list[ConfigItem] | tuple[ConfigItem]
"""A sequence of possible zappend configuration types."""

ConfigLike = ConfigItem | ConfigList | None
"""Type for a zappend configuration-like object."""


def normalize_config(config_like: ConfigLike) -> dict[str, Any]:
    """Normalize the configuration-like value `config_like`
    into a configuration dictionary.

    The configuration-like value `config_like`

    * can be a dict (the configuration itself),
    * a str or a FileObj (configuration loaded from URI),
    * a sequence of configuration-like values.
    * or None.

    The values of a sequence will be normalized first, then all
    resulting configuration dictionaries will be merged in to one.

    Args:
        config_like: A configuration-like value.

    Returns:
        The normalized configuration dictionary.
    """
    if isinstance(config_like, dict):
        return config_like
    if config_like is None:
        return {}
    if isinstance(config_like, FileObj):
        return load_config(config_like)
    if isinstance(config_like, str):
        return load_config(FileObj(config_like))
    if isinstance(config_like, (list, tuple)):
        return merge_configs(*[normalize_config(c) for c in config_like])
    raise TypeError(
        "config_like must of type NoneType, FileObj, dict,"
        " str, or a sequence of such values"
    )


def load_config(config_file: FileObj) -> dict[str, Any]:
    yaml_extensions = {".yml", ".yaml", ".YML", ".YAML"}
    logger.info(f"Reading configuration {config_file.uri}")
    _, ext = os.path.splitext(config_file.path)
    with config_file.fs.open(config_file.path, "rt") as f:
        source = f.read()
    source = string.Template(source).safe_substitute(os.environ)
    stream = io.StringIO(source)
    if ext in yaml_extensions:
        config = yaml.safe_load(stream)
    else:
        config = json.load(stream)
    if not isinstance(config, dict):
        raise TypeError(
            f"Invalid configuration:" f" {config_file.uri}: object expected"
        )
    return config


def merge_configs(*configs: dict[str, Any]) -> dict[str, Any]:
    if not configs:
        return {}
    merged_config = dict(configs[0])
    for config in configs[1:]:
        merged_config = _merge_dicts(merged_config, config)
    return merged_config


def _merge_dicts(dict_1: dict[str, Any], dict_2: dict[str, Any]) -> dict[str, Any]:
    merged = dict(dict_1)
    for key in dict_2.keys():
        if key in merged:
            merged[key] = _merge_values(merged[key], dict_2[key])
        else:
            merged[key] = dict_2[key]
    return merged


# noinspection PyUnusedLocal
def _merge_lists(list_1: list[Any], list_2: list[Any]) -> list[Any]:
    # alternative strategies:
    # return list(set(list_1) + set(list_2))  # unite
    # return list_1 + list_2  # concat
    # return list_1  # keep
    return list_2  # replace


def _merge_values(value_1: Any, value_2: Any) -> Any:
    if isinstance(value_1, dict) and isinstance(value_2, dict):
        return _merge_dicts(value_1, value_2)
    if isinstance(value_1, (list, tuple)) and isinstance(value_2, (list, tuple)):
        return _merge_lists(value_1, value_2)
    return value_2


@contextlib.contextmanager
def exclude_from_config(config: dict[str, Any], *keys: str) -> dict[str, Any]:
    yield {k: v for k, v in config.items() if k not in keys}
