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

_SLICE_POLLING_SCHEMA = {
    "description": "Defines how to poll for contributing datasets.",
    "oneOf": [
        {
            "description": "No polling, fail immediately if dataset"
                           " is not available.",
            "const": False
        },
        {
            "description": "Poll using default values.",
            "const": True
        },
        {
            "description": "Polling parameters.",
            "type": "object",
            "properties": dict(
                interval={
                    "description": "Polling interval in seconds.",
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "default": DEFAULT_SLICE_POLLING_INTERVAL
                },
                timeout={
                    "description": "Polling timeout in seconds.",
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "default": DEFAULT_SLICE_POLLING_TIMEOUT
                },
            )
        }
    ]
}

_VAR_ENCODING_SCHEMA = {
    "description": "TODO",
    "type": "object",
    "properties": dict(
        dtype={
            "description": "Storage data type",
            "enum": ["int8", "uint8",
                     "int16", "uint16",
                     "int32", "uint32",
                     "int64", "uint64",
                     "float32", "float64"]
        },
        chunks={
            "description": "Chunk sizes in dimension order."
                           " Set to 'null' to not chunk at all.",
            "type": ["array", "null"],
            "items": {"type": "integer", "minimum": 1}
        },
        fill_value={
            "description": "Storage fill value.",
            "oneOf": [
                {"type": "null"},
                {"type": "number"},
                {"const": "NaN"},
            ]
        },
        scale_factor={
            "description": "Scale factor."
                           " memory_value = scale_factor * storage_value"
                           " + add_offset.",
            "type": "number"
        },
        add_offset={
            "description": "Add offset."
                           " memory_value = scale_factor * storage_value"
                           " + add_offset.",
            "type": "number"
        },
        units={
            "description": "Units of the storage data type"
                           " if memory data type is date/time.",
            "type": "string"
        },
        calendar={
            "description": "The calendar to be used"
                           " if memory data type is date/time.",
            "type": "string"
        },
        compressor={
            "description": "Compressor."
                           " Set to 'null' to disable data compression.",
            "type": ["array", "null"],
            "properties": {
                "id": {"type": "string"},
            },
            "required": ["id"],
            "additionalProperties": True
        },
        filters={
            "description": "Filters. Set to 'null' to not use filters.",
            "type": ["array", "null"],
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                },
                "required": ["id"],
                "additionalProperties": True
            }
        },
    ),
}

# TODO: configure logging

CONFIG_V1_SCHEMA = {
    "description": "Configuration for the zappend tool.",
    "type": "object",
    "properties": dict(
        version={
            "description": "Configuration version.",
            "const": 1
        },

        target_uri={
            "description": "The URI or local path of the target Zarr dataset."
                           " Must be a directory.",
            "type": "string",
            "minLength": 1
        },

        target_storage_options={
            "description": "Options for the filesystem given by"
                           " the URI of 'target_uri'.",
            "type": "object",
            "additionalProperties": True
        },

        slice_engine={
            "description": "The name of the engine to be used for opening"
                           " contributing datasets."
                           " Refer to the 'engine' argument of the function"
                           " xarray.open_dataset().",
            "type": "string",
            "minLength": 1
        },

        slice_storage_options={
            "description": "Options for the filesystem given by"
                           " the protocol of the URIs of"
                           " contributing datasets.",
            "type": "object",
            "additionalProperties": True
        },

        slice_polling=_SLICE_POLLING_SCHEMA,

        temp_dir={
            "description": "The URI or local path of the directory that"
                           " will be used to temporarily store rollback"
                           " information.",
            "type": "string",
            "minLength": 1
        },

        temp_storage_options={
            "description": "Options for the filesystem given by"
                           " the protocol of 'temp_dir'.",
            "type": "object",
            "additionalProperties": True
        },

        zarr_version={
            "description": "The Zarr version to be used.",
            "const": DEFAULT_ZARR_VERSION
        },

        fixed_dims={
            "description": "Specifies the fixed dimensions of the"
                           " target dataset. Keys are dimension names, values"
                           " are dimension sizes.",
            "type": "object",
            "additionalProperties": {"type": "integer", "minimum": 1}
        },

        append_dim={
            "description": "The name of the variadic append dimension.",
            "type": "string",
            "minLength": 1,
            "default": DEFAULT_APPEND_DIM
        },

        variables={
            "description": "Defines dimensions, encoding, and attributes"
                           " for variables. Object property names refer to"
                           " variable names.  Special name '*' refers to all"
                           " variables, useful for defining default values.",
            "type": "object",
            "additionalProperties": {
                "type": "object",
                "properties": dict(

                    dims={
                        "description": "The dimensions of the variable in the"
                                       " given order. Each dimension must"
                                       " exist in contributing datasets.",
                        "type": "array",
                        "items": {
                            "type": "string",
                            "minLength": 1
                        }
                    },

                    encoding=_VAR_ENCODING_SCHEMA,

                    attrs={
                        "description": "Arbitrary variable metadata"
                                       " attributes.",
                        "type": "object",
                        "additionalProperties": True
                    },

                ),
                "additionalProperties": False,
            },
        },

        included_variables={
            "description": "Specifies the variables to be included in"
                           " the target dataset. Defaults to all variables"
                           " found in the first contributing dataset.",
            "type": "array",
            "items": {"type": "string", "minLength": 1}
        },

        excluded_variables={
            "description": "Specifies individual variables to be excluded"
                           " from all contributing datasets.",
            "type": "array",
            "items": {"type": "string", "minLength": 1}
        },

        dry_run={
            "description": "If 'true', log only what would have been done,"
                           " but don't apply any changes.",
            "type": "boolean",
            "default": False
        }
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
        return merge_configs(*[normalize_config(c) for c in config_like])
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


def merge_configs(*configs: Config) -> Config:
    if not configs:
        return {}
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
    if isinstance(value_1, dict) and isinstance(value_2, dict):
        return _merge_dicts(value_1, value_2)
    if (isinstance(value_1, (list, tuple)) and isinstance(value_2,
                                                          (list, tuple))):
        return _merge_lists(value_1, value_2)
    return value_2
