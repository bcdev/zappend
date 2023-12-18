# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
import os.path
from typing import Any

import fsspec
import yaml
import jsonschema

from .log import logger


DEFAULT_ZARR_VERSION = 2
DEFAULT_APPEND_DIM = "time"

DEFAULT_SLICE_POLLING_INTERVAL = 2
DEFAULT_SLICE_POLLING_TIMEOUT = 60

# Write slice to temp, then read from temp (default).
SLICE_ACCESS_MODE_TEMP = "temp"
# Read directly from slice source, skip compatibility check.
SLICE_ACCESS_MODE_SOURCE = "source"
# Read directly from slice source if slice is compatible,
# otherwise fallback to "temp".
SLICE_ACCESS_MODE_SOURCE_SAFE = "source_safe"

# Access modes if slice is persistent and given as path
SLICE_ACCESS_MODES = [
    SLICE_ACCESS_MODE_TEMP,
    SLICE_ACCESS_MODE_SOURCE,
    SLICE_ACCESS_MODE_SOURCE_SAFE
]
DEFAULT_SLICE_ACCESS_MODE = SLICE_ACCESS_MODE_TEMP

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

_CONFIG_V1_SCHEMA = {
    "type": "object",
    "properties": dict(
        version={"const": 1},
        zarr_version={"const": DEFAULT_ZARR_VERSION},
        fixed_dims={
            "type": "object",
            "additionalProperties": _ORDINAL_SCHEMA
        },
        append_dim={
            "type": "string",
            "default": DEFAULT_APPEND_DIM,
            "minLength": 1
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
                    shape={
                        "type": "array",
                        "items": _ORDINAL_SCHEMA
                    },
                    chunks={
                        "type": "array",
                        "items": _ORDINAL_SCHEMA
                    },
                    fill_value={
                        "type": ["number", "null"]
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

        target_fs_options={"type": "object", "additionalProperties": True},

        slice_fs_options={"type": "object", "additionalProperties": True},
        slice_polling=_SLICE_POLLING_SCHEMA,
        slice_access_mode={
            "enum": SLICE_ACCESS_MODES,
            "default": DEFAULT_SLICE_ACCESS_MODE
        },

        temp_path={"type": "string", "minLength": 1},
        temp_fs_options={"type": "object", "additionalProperties": True},
    ),
    # "required": ["version", "fixed_dims", "append_dim"],
    "additionalProperties": False,
}


def normalize_config(
        config: tuple[str, ...] | str | dict[str, Any] | None
) -> dict[str, Any]:
    """Normalizes and validates configuration."""
    if config is None:
        config = {}
    elif isinstance(config, dict):
        validate_config(config)
    elif isinstance(config, str):
        config = load_configs([config])
    elif isinstance(config, (list, tuple)):
        config = load_configs(config)
    else:
        raise TypeError("config must be a dict, a str, or a list of str")
    return config


def validate_config(config: dict[str, Any]):
    jsonschema.validate(config, _CONFIG_V1_SCHEMA)


def load_configs(config_paths: tuple[str, ...] | list[str, ...]) \
        -> dict[str, Any]:
    config = {}
    if not config_paths:
        return config
    if len(config_paths) == 1:
        config = load_config(config_paths[0])
    for config_path in config_paths:
        config = merge_dicts(config, load_config(config_path))
    validate_config(config)
    return config


def load_config(config_path: str) -> dict[str, Any]:
    logger.info(f"Reading configuration {config_path}")
    _, ext = os.path.splitext(config_path)
    with fsspec.open(config_path) as f:
        if ext in (".json", ".JSON"):
            config = json.load(f)
        else:
            config = yaml.safe_load(f)
    if not isinstance(config, dict):
        raise ValueError(f"Invalid configuration:"
                         f" {config_path}:"
                         f" object expected")
    return config


def merge_dicts(dict_1: dict[str, Any], dict_2: dict[str, Any]) \
        -> dict[str, Any]:
    merged = dict(dict_1)
    for key in dict_2.keys():
        if key in merged:
            merged[key] = merge_values(merged[key],
                                       dict_2[key])
        else:
            merged[key] = dict_2[key]
    return merged


def merge_lists(list_1: list[Any], list_2: list[Any]) -> list[Any]:
    return list_1 + list_2


def merge_values(value_1: Any, value_2: Any) -> Any:
    if value_1 is None:
        return value_2
    if value_2 is None:
        return value_1
    if isinstance(value_1, dict) and isinstance(value_2, dict):
        return merge_dicts(value_1, value_2)
    if isinstance(value_1, list) and isinstance(value_2, list):
        return merge_lists(value_1, value_2)
    return value_1


ZARR_V2_DEFAULT_COMPRESSOR = {
    "id": "blosc",
    "cname": "lz4",
    "clevel": 5,
    "shuffle": 1,
    "blocksize": 0,
}
