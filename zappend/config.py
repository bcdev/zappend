# Copyright © 2024 Norman Fomferra
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
            "description": (
                "No polling, fail immediately if dataset" " is not available."
            ),
            "const": False,
        },
        {"description": "Poll using default values.", "const": True},
        {
            "description": "Polling parameters.",
            "type": "object",
            "properties": dict(
                interval={
                    "description": "Polling interval in seconds.",
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "default": DEFAULT_SLICE_POLLING_INTERVAL,
                },
                timeout={
                    "description": "Polling timeout in seconds.",
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "default": DEFAULT_SLICE_POLLING_TIMEOUT,
                },
            ),
            "required": ["interval", "timeout"],
        },
    ],
}

_VARIABLE_ENCODING_SCHEMA = {
    "description": "Variable storage encoding. Settings given here overwrite"
    " the encoding settings of the first"
    " contributing dataset.",
    "type": "object",
    "properties": dict(
        dtype={
            "description": "Storage data type",
            "enum": [
                "int8",
                "uint8",
                "int16",
                "uint16",
                "int32",
                "uint32",
                "int64",
                "uint64",
                "float32",
                "float64",
            ],
        },
        chunks={
            "description": "Storage chunking.",
            "oneOf": [
                {
                    "description": "Chunk sizes in the order of the dimensions.",
                    "type": "array",
                    "items": {"type": "integer", "minimum": 1},
                },
                {"description": "Disable chunking.", "const": None},
            ],
        },
        fill_value={
            "description": "Storage fill value.",
            "oneOf": [
                {
                    "description": (
                        "A number of type and unit of the" " given storage `dtype`."
                    ),
                    "type": "number",
                },
                {
                    "description": (
                        "Not-a-number. Can be used only if storage"
                        " `dtype` is `float32` or `float64`."
                    ),
                    "const": "NaN",
                },
                {"description": "No fill value.", "const": None},
            ],
        },
        scale_factor={
            "description": (
                "Scale factor for computing the in-memory value:"
                " `memory_value = scale_factor * storage_value"
                " + add_offset`."
            ),
            "type": "number",
        },
        add_offset={
            "description": (
                "Add offset for computing the in-memory value:"
                " `memory_value = scale_factor * storage_value"
                " + add_offset`."
            ),
            "type": "number",
        },
        units={
            "description": (
                "Units of the storage data type" " if memory data type is date/time."
            ),
            "type": "string",
        },
        calendar={
            "description": (
                "The calendar to be used" " if memory data type is date/time."
            ),
            "type": "string",
        },
        compressor={
            "description": "Compressor. Set to `null` to disable data compression.",
            "type": ["array", "null"],
            "properties": {
                "id": {"type": "string"},
            },
            "required": ["id"],
            "additionalProperties": True,
        },
        filters={
            "description": "Filters. Set to `null` to not use filters.",
            "type": ["array", "null"],
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "string"},
                },
                "required": ["id"],
                "additionalProperties": True,
            },
        },
    ),
}

_VARIABLES_SCHEMA = {
    "description": (
        "Defines dimensions, encoding, and attributes"
        " for variables in the target dataset."
        " Object property names refer to variable names."
        " The special name `*` refers to"
        " all variables, which is useful for defining"
        " common values."
    ),
    "type": "object",
    "additionalProperties": {
        "description": "Variable metadata.",
        "type": "object",
        "properties": dict(
            dims={
                "description": (
                    "The names of the variable's dimensions"
                    " in the given order. Each dimension"
                    " must exist in contributing datasets."
                ),
                "type": "array",
                "items": {"type": "string", "minLength": 1},
            },
            encoding=_VARIABLE_ENCODING_SCHEMA,
            attrs={
                "description": "Arbitrary variable metadata" " attributes.",
                "type": "object",
                "additionalProperties": True,
            },
        ),
        "additionalProperties": False,
    },
}

_LOG_REF_URL = (
    "https://docs.python.org/3/library/logging.config.html" "#logging-config-dictschema"
)
_LOG_HDL_CLS_URL = "https://docs.python.org/3/library/logging.handlers.html"

_LOG_LEVELS = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]

_LOGGING_SCHEMA = {
    "description": f"Logging configuration. For details refer to the"
    f" [dictionary schema]({_LOG_REF_URL})"
    f" of the Python module `logging.config`.",
    "type": "object",
    "properties": dict(
        version={"description": "Logging schema version.", "const": 1},
        formatters={
            "description": (
                "Formatter definitions."
                " Each key is a formatter id and each value is an"
                " object describing how to configure the"
                " corresponding formatter."
            ),
            "type": "object",
            "additionalProperties": {
                "description": "Formatter configuration.",
                "type": "object",
                "properties": dict(
                    format={
                        "description": "Format string in the given `style`.",
                        "type": "string",
                        "default": "%(message)s",
                    },
                    datefmt={
                        "description": (
                            "Format string in the given `style`"
                            " for the date/time portion."
                        ),
                        "type": "string",
                        "default": "%Y-%m-%d %H:%M:%S,uuu",
                    },
                    style={
                        "description": "Determines how the format string"
                        " will be merged with its data.",
                        "enum": ["%", "{", "$"],
                    },
                ),
                "additionalProperties": False,
            },
        },
        filters={
            "description": (
                "Filter definitions."
                " Each key is a filter id and each value is a dict"
                " describing how to configure the corresponding"
                " filter."
            ),
            "type": "object",
            "additionalProperties": {
                "description": "Filter configuration.",
                "type": "object",
                "additionalProperties": True,
            },
        },
        handlers={
            "description": (
                "Handler definitions."
                " Each key is a handler id and each value is an"
                " object describing how to configure the"
                " corresponding handler."
            ),
            "type": "object",
            "additionalProperties": {
                "description": (
                    "Handler configuration. All keys other than the following are"
                    " passed through as keyword arguments to the handler's"
                    " constructor."
                ),
                "type": "object",
                "properties": {
                    "class": {
                        "description": (
                            f"The fully qualified name of the handler class. See"
                            f" [logging handlers]({_LOG_HDL_CLS_URL})."
                        ),
                        "type": "string",
                        "examples": ["logging.StreamHandler", "logging.FileHandler"],
                    },
                    "level": {
                        "description": "The level of the handler.",
                        "enum": _LOG_LEVELS,
                    },
                    "formatter ": {
                        "description": "The id of the formatter" " for this handler.",
                        "type": "string",
                    },
                    "filters": {
                        "description": "A list of ids of the filters"
                        " for this logger.",
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": ["class"],
                "additionalProperties": True,
            },
        },
        loggers={
            "description": (
                "Logger definitions."
                " Each key is a logger name and each value is an"
                " object describing how to configure the"
                " corresponding logger. The tool's logger"
                " has the id `'zappend'`."
            ),
            "type": "object",
            "additionalProperties": {
                "description": "Logger configuration.",
                "type": "object",
                "properties": {
                    "level": {
                        "description": "The level of the logger.",
                        "enum": _LOG_LEVELS,
                    },
                    "propagate ": {
                        "description": "The propagation setting of the logger.",
                        "type": "boolean",
                    },
                    "filters": {
                        "description": (
                            "A list of ids of the filters" " for this logger."
                        ),
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "handlers": {
                        "description": (
                            "A list of ids of the handlers" " for this logger."
                        ),
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "additionalProperties": True,
            },
        },
    ),
    "required": ["version"],
    "additionalProperties": True,
}

CONFIG_V1_SCHEMA = {
    "description": "Configuration for the `zappend` tool",
    "type": "object",
    "properties": dict(
        version={
            "description": (
                "Configuration schema version."
                " Allows the schema to evolve while still"
                " preserving backwards compatibility."
            ),
            "const": 1,
        },
        target_dir={
            "description": (
                "The URI or local path of the target Zarr dataset."
                " Must be a directory."
            ),
            "type": "string",
            "minLength": 1,
        },
        target_storage_options={
            "description": (
                "Options for the filesystem given by" " the URI of `target_dir`."
            ),
            "type": "object",
            "additionalProperties": True,
        },
        slice_engine={
            "description": (
                "The name of the engine to be used for opening"
                " contributing datasets."
                " Refer to the `engine` argument of the function"
                " `xarray.open_dataset()`."
            ),
            "type": "string",
            "minLength": 1,
        },
        slice_storage_options={
            "description": (
                "Options for the filesystem given by"
                " the protocol of the URIs of"
                " contributing datasets."
            ),
            "type": "object",
            "additionalProperties": True,
        },
        slice_polling=_SLICE_POLLING_SCHEMA,
        temp_dir={
            "description": (
                "The URI or local path of the directory that"
                " will be used to temporarily store rollback"
                " information."
            ),
            "type": "string",
            "minLength": 1,
        },
        temp_storage_options={
            "description": (
                "Options for the filesystem given by" " the protocol of `temp_dir`."
            ),
            "type": "object",
            "additionalProperties": True,
        },
        zarr_version={
            "description": "The Zarr version to be used.",
            "const": DEFAULT_ZARR_VERSION,
        },
        fixed_dims={
            "description": (
                "Specifies the fixed dimensions of the"
                " target dataset. Keys are dimension names, values"
                " are dimension sizes."
            ),
            "type": "object",
            "additionalProperties": {"type": "integer", "minimum": 1},
        },
        append_dim={
            "description": "The name of the variadic append dimension.",
            "type": "string",
            "minLength": 1,
            "default": DEFAULT_APPEND_DIM,
        },
        variables=_VARIABLES_SCHEMA,
        included_variables={
            "description": (
                "Specifies the names of variables to be included in"
                " the target dataset. Defaults to all variables"
                " found in the first contributing dataset."
            ),
            "type": "array",
            "items": {"type": "string", "minLength": 1},
        },
        excluded_variables={
            "description": (
                "Specifies the names of individual variables"
                " to be excluded  from all contributing datasets."
            ),
            "type": "array",
            "items": {"type": "string", "minLength": 1},
        },
        disable_rollback={
            "description": (
                "Disable rolling back dataset changes on failure."
                " Effectively disables transactional dataset"
                " modifications, so use this setting with care."
            ),
            "type": "boolean",
            "default": False,
        },
        dry_run={
            "description": (
                "If 'true', log only what would have been done,"
                " but don't apply any changes."
            ),
            "type": "boolean",
            "default": False,
        },
        logging=_LOGGING_SCHEMA,
    ),
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
        raise ValueError(
            f"Invalid configuration: {e.message}" f" for {'.'.join(map(str, e.path))}"
        )
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
    raise TypeError(
        "config_like must of type NoneType, FileObj, dict,"
        " str, or a sequence of such values"
    )


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
        raise TypeError(
            f"Invalid configuration:" f" {config_file.uri}: object expected"
        )
    return config


def merge_configs(*configs: Config) -> Config:
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


def schema_to_json() -> str:
    return json.dumps(CONFIG_V1_SCHEMA, indent=2)


def schema_to_md() -> str:
    lines = []
    _schema_to_md(CONFIG_V1_SCHEMA, [], lines)
    return "\n".join(lines)


def _schema_to_md(schema: dict[str, Any], path: list[str], lines: list[str]):
    undefined = object()
    is_root = len(path) == 0

    _type = schema.get("type")
    if _type and not is_root:
        if isinstance(_type, str):
            _type = [_type]
        value = " | ".join([f"_{name}_" for name in _type])
        lines.append(f"Type {value}.")

    description = schema.get("description")
    if description:
        prefix = "## " if is_root else ""
        lines.append(prefix + description)

    one_of = schema.get("oneOf")
    if one_of:
        lines.append(f"Must be one of the following:")
        for sub_schema in one_of:
            sub_lines = []
            _schema_to_md(sub_schema, path, sub_lines)
            if sub_lines:
                lines.append("* " + sub_lines[0])
                for sub_line in sub_lines[1:]:
                    lines.append("  " + sub_line)

    const = schema.get("const", undefined)
    if const is not undefined:
        value = json.dumps(const)
        lines.append(f"Its value is `{value}`.")

    default = schema.get("default", undefined)
    if default is not undefined:
        value = json.dumps(default)
        lines.append(f"Defaults to `{value}`.")

    enum = schema.get("enum")
    if enum:
        values = ", ".join([json.dumps(v) for v in enum])
        lines.append(f"Must be one of `{values}`.")

    required = schema.get("required")
    if required:
        names = [f"`{name}`" for name in required]
        if len(names) > 1:
            lines.append(f"The keys {', '.join(names)} are required.")
        else:
            lines.append(f"The key {names[0]} is required.")

    properties = schema.get("properties")
    if properties:
        lines.append("")
        for name, property_schema in properties.items():
            if is_root:
                lines.append("")
                lines.append(f"### `{name}`")
                lines.append("")
                _schema_to_md(property_schema, path + [name], lines)
            else:
                lines.append(f"* `{name}`:")
                sub_lines = []
                _schema_to_md(property_schema, path + [name], sub_lines)
                for sub_line in sub_lines:
                    lines.append("  " + sub_line)
        lines.append("")

    additional_properties = schema.get("additionalProperties")
    if isinstance(additional_properties, dict):
        lines.append("Object values are:")
        _schema_to_md(additional_properties, path, lines)
