# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
import os.path
from typing import Any

import fsspec
import yaml
import jsonschema

from .log import LOG

NON_EMPTY_STRING = {"type": "string", "minLength": 1}
ORDINAL_INT = {"type": "integer", "minimum": 1}
ANY_OBJECT = {"type": "object", "additionalProperties": True}

ENCODING_SCHEMA = {
    "type": "object",
    "properties": dict(
        chunks={
            "type": "array",
            "items": ORDINAL_INT
        },
        fill_value={"oneOf": [
            {"type": "number"},
            {"const": "NaN"},
        ]},
        dtype={"enum": ["int8", "uint8",
                        "int16", "uint16",
                        "int32", "uint32",
                        "int64", "uint64",
                        "float32", "float64"]},
        compressor=ANY_OBJECT,
        filters={"type": "array", "items": ANY_OBJECT},
    ),
    "additionalProperties": False,
}

CONFIG_V1_SCHEMA = {
    "type": "object",
    "properties": dict(
        version={"const": 1},
        fixed_dims={
            "type": "object",
            "additionalProperties": ORDINAL_INT
        },
        append_dim=NON_EMPTY_STRING,
        variables={
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": dict(
                    dims={"type": "array", "items": NON_EMPTY_STRING},
                    attrs=ANY_OBJECT,
                    encoding=ENCODING_SCHEMA,
                ),
                "additionalProperties": False,
            },
        },
        target_fs_options={"type": "object", "additionalProperties": True},
        slice_fs_options={"type": "object", "additionalProperties": True},
        temp_path={"type": "string", "minLength": 1},
        temp_fs_options={"type": "object", "additionalProperties": True},
    ),
    # "required": ["version", "fixed_dims", "append_dim"],
    "additionalProperties": False,
}


def load_configs(config_paths: tuple[str, ...]) -> dict[str, Any]:
    config = {}
    if not config_paths:
        return config
    if len(config_paths) == 1:
        config = load_config(config_paths[0])
    for config_path in config_paths:
        config = merge_dicts(config, load_config(config_path))
    jsonschema.validate(config, CONFIG_V1_SCHEMA)
    return config


def load_config(config_path: str) -> dict[str, Any]:
    LOG.info(f"Reading configuration {config_path}")
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
