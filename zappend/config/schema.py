# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
from typing import Any, Literal

from .defaults import DEFAULT_APPEND_DIM
from .defaults import DEFAULT_SLICE_POLLING_INTERVAL
from .defaults import DEFAULT_SLICE_POLLING_TIMEOUT
from .defaults import DEFAULT_ZARR_VERSION


SLICE_POLLING_SCHEMA = {
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

VARIABLE_ENCODING_SCHEMA = {
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
            "description": (
                "Compressor definition. Set to `null` to disable data compression."
            ),
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
                "description": "Filter definition.",
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

VARIABLES_SCHEMA = {
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
            encoding=VARIABLE_ENCODING_SCHEMA,
            attrs={
                "description": "Arbitrary variable metadata" " attributes.",
                "type": "object",
                "additionalProperties": True,
            },
        ),
        "additionalProperties": False,
    },
}

LOG_REF_URL = (
    "https://docs.python.org/3/library/logging.config.html" "#logging-config-dictschema"
)
LOG_HDL_CLS_URL = "https://docs.python.org/3/library/logging.handlers.html"

LOG_LEVELS = ["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"]

LOGGING_SCHEMA = {
    "description": f"Logging configuration. For details refer to the"
    f" [dictionary schema]({LOG_REF_URL})"
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
                            f" [logging handlers]({LOG_HDL_CLS_URL})."
                        ),
                        "type": "string",
                        "examples": ["logging.StreamHandler", "logging.FileHandler"],
                    },
                    "level": {
                        "description": "The level of the handler.",
                        "enum": LOG_LEVELS,
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
                        "enum": LOG_LEVELS,
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

CONFIG_SCHEMA_V1 = {
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
        slice_polling=SLICE_POLLING_SCHEMA,
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
        variables=VARIABLES_SCHEMA,
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
        persist_mem_slices={
            "description": (
                "Persist in-memory slices and reopen from a temporary Zarr before"
                " appending them to the target dataset."
                " This can prevent expensive re-computation of dask chunks at the"
                " cost of additional i/o."
            ),
            "type": "boolean",
            "default": False,
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
        logging=LOGGING_SCHEMA,
    ),
    "additionalProperties": False,
}


# noinspection PyShadowingBuiltins
def get_config_schema(
    format: Literal["md"] | Literal["json"] | Literal["dict"] = "dict",
) -> str | dict[str, Any]:
    """Get the configuration schema in the given format.

    Args:
        format: One of "md" (markdown), "json" (JSON Schema), or
            "dict" (JSON Schema object).
    """
    if format == "json":
        return json.dumps(CONFIG_SCHEMA_V1, indent=2)
    elif format == "md":
        lines = []
        _schema_to_md(CONFIG_SCHEMA_V1, [], lines)
        return "\n".join(lines)
    else:
        return dict(CONFIG_SCHEMA_V1)


def _schema_to_md(
    schema: dict[str, Any],
    path: list[str],
    lines: list[str],
    type_prefix: str | None = None,
):
    undefined = object()
    is_root = len(path) == 0

    _type = schema.get("type")
    if _type and not is_root:
        if isinstance(_type, str):
            _type = [_type]
        value = " | ".join([f"_{name}_" for name in _type])
        lines.append(f"{type_prefix or 'Type'} {value}.")

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
        lines.append("")
        _schema_to_md(
            additional_properties,
            path,
            lines,
            type_prefix="The object's values are of type",
        )

    items = schema.get("items")
    if isinstance(items, dict):
        lines.append("")
        _schema_to_md(
            items,
            path,
            lines,
            type_prefix="The array's items are of type",
        )
