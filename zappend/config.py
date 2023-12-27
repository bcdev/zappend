# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
import os.path
from typing import Any

import jsonschema
import jsonschema.exceptions
import yaml

from .fsutil.fileobj import FileObj
from .log import logger

DEFAULT_ZARR_VERSION = 2
DEFAULT_APPEND_DIM = "time"

DEFAULT_SLICE_POLLING_INTERVAL = 2
DEFAULT_SLICE_POLLING_TIMEOUT = 60

_NON_EMPTY_STRING_SCHEMA = {"type": "string", "minLength": 1}
_ORDINAL_SCHEMA = {"type": "integer", "minimum": 1}
_ANY_OBJECT_SCHEMA = {"type": "object", "additionalProperties": True}

_SLICE_POLLING_SCHEMA = {
    "oneOf": [
        {"type": "boolean"},
        {
            "type": "object",
            "properties": dict(
                interval={
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "default": DEFAULT_SLICE_POLLING_INTERVAL
                },
                timeout={
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "default": DEFAULT_SLICE_POLLING_TIMEOUT
                },
            )
        }
    ]
}

CONFIG_V1_SCHEMA = {
    "type": "object",
    "properties": dict(
        version={"const": 1},

        target_uri=_NON_EMPTY_STRING_SCHEMA,

        slice_engine=_NON_EMPTY_STRING_SCHEMA,

        target_storage_options={
            "type": "object",
            "additionalProperties": True
        },

        slice_storage_options={
            "type": "object",
            "additionalProperties": True
        },
        slice_polling=_SLICE_POLLING_SCHEMA,

        temp_dir=_NON_EMPTY_STRING_SCHEMA,
        temp_storage_options={"type": "object", "additionalProperties": True},

        zarr_version={"const": DEFAULT_ZARR_VERSION},

        fixed_dims={
            "type": "object",
            "additionalProperties": _ORDINAL_SCHEMA
        },

        append_dim={
            **_NON_EMPTY_STRING_SCHEMA,
            "default": DEFAULT_APPEND_DIM
        },

        # Define layout and encoding for variables.
        # Object property names refer to variable names.
        # Special name "*" refers to all variables, useful
        # to define default values.
        variables={
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": dict(
                    dtype={
                        "enum": ["int8", "uint8",
                                 "int16", "uint16",
                                 "int32", "uint32",
                                 "int64", "uint64",
                                 "float32", "float64"]
                    },
                    dims={
                        "type": "array",
                        "items": _NON_EMPTY_STRING_SCHEMA
                    },
                    chunks={
                        "type": ["array", "null"],
                        "items": _ORDINAL_SCHEMA
                    },
                    fill_value={
                        "oneOf": [
                            {"type": "null"},
                            {"type": "number"},
                            {"const": "NaN"},
                        ]
                    },
                    scale_factor={
                        "type": "number"
                    },
                    add_offset={
                        "type": "number"
                    },
                    compressor=_ANY_OBJECT_SCHEMA,
                    filters={
                        "type": "array",
                        "items": _ANY_OBJECT_SCHEMA
                    },
                    attrs=_ANY_OBJECT_SCHEMA,
                    encoding=_ANY_OBJECT_SCHEMA,
                ),
                "additionalProperties": False,
            },
        },

    ),
    # "required": ["version", "fixed_dims", "append_dim"],
    "additionalProperties": False,
}

Config = dict[str, Any]
ConfigLikeOne = FileObj | str | Config
ConfigLikeMany = list[ConfigLikeOne] | tuple[ConfigLikeOne]
ConfigLike = ConfigLikeMany | ConfigLikeOne | None


def validate_config(config_like: ConfigLike) -> Config:
    """Validate configuration and return normalized form.

    First normalizes the configuration-like value *config_like*
    using ``normalize_config()``, then validates and returns the result.

    :param config_like: A configuration-like value.
    :return: The normalized and validated configuration dictionary.
    """
    config = normalize_config(config_like)
    try:
        jsonschema.validate(config, CONFIG_V1_SCHEMA)
    except jsonschema.exceptions.ValidationError as e:
        raise ValueError(f"Invalid configuration: {e.message}"
                         f" for {'.'.join(map(str, e.path))}")
    return config


def normalize_config(config_like: ConfigLike) -> Config:
    """Normalize the configuration-like value *config_like*
    into a configuration dictionary.

    The configuration-like value *config_like*

    * can be a dict (the configuration itself),
    * a str or a FileObj (configuration loaded from URI),
    * a sequence of configuration-like values.
    * or None.

    The values of a sequence will be normalized first, then all
    resulting configuration dictionaries will be merged in to one.

    :param config_like: A configuration-like value.
    :return: The normalized configuration dictionary.
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
        return _merge_configs([normalize_config(c) for c in config_like])
    raise TypeError("config_like must of type NoneType, FileObj, dict,"
                    " str, or a sequence of such values")


def load_config(config_file: FileObj) -> Config:
    yaml_extensions = {".yml", ".yaml", ".YML", ".YAML"}
    logger.info(f"Reading configuration {config_file.uri}")
    _, ext = os.path.splitext(config_file.path)
    with config_file.fs.open(config_file.path, "rt") as f:
        if ext in yaml_extensions:
            config = yaml.safe_load(f)
        else:
            config = json.load(f)
    if not isinstance(config, dict):
        raise TypeError(f"Invalid configuration:"
                        f" {config_file.uri}: object expected")
    return config


def _merge_configs(configs: list[Config]) -> Config:
    merged_config = dict(configs[0])
    for config in configs[1:]:
        merged_config = _merge_dicts(merged_config, config)
    return merged_config


def _merge_dicts(dict_1: dict[str, Any],
                 dict_2: dict[str, Any]) -> dict[str, Any]:
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
    if value_1 is None:
        return value_2
    if value_2 is None:
        return value_1
    if isinstance(value_1, dict) and isinstance(value_2, dict):
        return _merge_dicts(value_1, value_2)
    if (isinstance(value_1, (list, tuple)) and isinstance(value_2,
                                                          (list, tuple))):
        return _merge_lists(value_1, value_2)
    return value_1
