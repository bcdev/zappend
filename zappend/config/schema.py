# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
from typing import Any, Literal

from .defaults import DEFAULT_APPEND_DIM
from .defaults import DEFAULT_APPEND_STEP
from .defaults import DEFAULT_ATTRS_UPDATE_MODE
from .defaults import DEFAULT_SLICE_POLLING_INTERVAL
from .defaults import DEFAULT_SLICE_POLLING_TIMEOUT
from .defaults import DEFAULT_ZARR_VERSION
from .markdown import schema_to_markdown


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
    "description": (
        "Variable Zarr storage encoding."
        " Settings given here overwrite the encoding settings of the"
        " first contributing dataset."
    ),
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
                    "description": "Chunk sizes for each dimension of the variable.",
                    "type": "array",
                    "items": {
                        "oneOf": [
                            {
                                "description": "Dimension is chunked using given size.",
                                "type": "integer",
                                "minimum": 1,
                            },
                            {
                                "description": "Disable chunking in this dimension.",
                                "const": None,
                            },
                        ]
                    },
                },
                {"description": "Disable chunking in all dimensions.", "const": None},
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
                "description": "Arbitrary variable metadata attributes.",
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

PROFILING_SCHEMA = {
    "description": (
        "Profiling configuration. Allows for runtime profiling of the" " processing."
    ),
    "oneOf": [
        {
            "type": "boolean",
            "description": (
                "If set, profiling is enabled and output is logged using"
                ' level `"INFO"`. Otherwise, profiling is disabled.'
            ),
        },
        {
            "type": "string",
            "description": (
                "Profile path. Enables profiling and writes a profiling"
                " report to given path."
            ),
        },
        {
            "description": "Detailed profiling configuration.",
            "type": "object",
            "properties": {
                "enabled": {
                    "description": "Enable or disable profiling.",
                    "type": "boolean",
                },
                "path": {
                    "description": "Local file path for profiling output.",
                    "type": "string",
                },
                "log_level": {
                    "description": 'Log level. Use `"NOTSET"` to disable logging.',
                    "default": "INFO",
                    "enum": LOG_LEVELS,
                },
                "keys": {
                    "description": (
                        "Sort output according to the supplied column names."
                        " Refer to [Stats.sort_stats(*keys)]"
                        "(https://docs.python.org/3/library/profile.html"
                        "#pstats.Stats.sort_stats)."
                    ),
                    "default": ["tottime"],
                    "type": "array",
                    "items": {
                        "enum": [
                            "calls",
                            "cumulative",
                            "cumtime",
                            "file",
                            "filename",
                            "module",
                            "ncalls",
                            "pcalls",
                            "line",
                            "name",
                            "nfl",
                            "stdname",
                            "time",
                            "tottime",
                        ]
                    },
                },
                "restrictions": {
                    "description": (
                        "Used to limit the list down to the significant entries"
                        " in the profiling report."
                        " Refer to [Stats.print_stats(*restrictions)]"
                        "(https://docs.python.org/3/library/profile.html"
                        "#pstats.Stats.print_stats)."
                    ),
                    "type": "array",
                    "items": {
                        "oneOf": [
                            {
                                "description": "Select a count of lines.",
                                "type": "integer",
                                "minimum": 1,
                            },
                            {
                                "description": "Select a percentage of lines.",
                                "type": "number",
                                "minimum": 0.0,
                                "maximum": 1.0,
                            },
                            {
                                "description": (
                                    "Pattern-match the standard name"
                                    " that is printed."
                                ),
                                "type": "string",
                            },
                        ]
                    },
                },
            },
            "additionalProperties": False,
        },
    ],
}


DETAILED_LOGGING_SCHEMA = {
    "description": (
        f"Detailed logging configuration. For details refer to the"
        f" [dictionary schema]({LOG_REF_URL})"
        f" of the Python module `logging.config`."
    ),
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

LOGGING_SCHEMA = {
    "description": "Logging configuration.",
    "oneOf": [
        {
            "description": (
                "Shortform that enables logging to the console"
                ' using log level `"INFO"`.'
            ),
            "type": "boolean",
        },
        {
            "description": (
                "Shortform that enables logging to the console"
                " using the specified log level."
            ),
            "enum": LOG_LEVELS,
        },
        DETAILED_LOGGING_SCHEMA,
    ],
}

CONFIG_SCHEMA_V1 = {
    "title": "Configuration Reference",
    "type": "object",
    "properties": dict(
        version={
            "description": (
                "Configuration schema version."
                " Allows the schema to evolve while still"
                " preserving backwards compatibility."
            ),
            "const": 1,
            "default": 1,
        },
        zarr_version={
            "description": "The Zarr version to be used.",
            "const": DEFAULT_ZARR_VERSION,
            "default": DEFAULT_ZARR_VERSION,
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
        append_step={
            "description": (
                "If set, enforces a step size in the append dimension between two"
                " slices or just enforces a direction."
            ),
            "anyOf": [
                {
                    "description": "Arbitrary step size or not applicable.",
                    "const": None,
                },
                {"description": "Monotonically increasing.", "const": "+"},
                {"description": "Monotonically decreasing.", "const": "-"},
                {
                    "description": (
                        "A positive or negative time delta value,"
                        " such as `12h`, `2D`, `-1D`."
                    ),
                    "type": "string",
                    "not": {"const": ""},
                },
                {
                    "description": "A positive or negative numerical delta value.",
                    "type": "number",
                    "not": {"const": 0},
                },
            ],
            "default": DEFAULT_APPEND_STEP,
        },
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
        variables=VARIABLES_SCHEMA,
        attrs={
            "description": (
                "Arbitrary dataset attributes."
                " If `permit_eval` is set to `true`,"
                " string values may include Python expressions"
                " enclosed in `{{` and `}}` to dynamically compute"
                " attribute values; in the expression, the current dataset "
                " is named `ds`."
                " Refer to the user guide for more information."
            ),
            "type": "object",
            "additionalProperties": True,
        },
        attrs_update_mode={
            "description": (
                "The mode used update target attributes from slice"
                " attributes. Independently of this setting, extra attributes"
                " configured by the `attrs` setting will finally be used to"
                " update the resulting target attributes."
            ),
            "oneOf": [
                {
                    "description": (
                        "Use attributes from first slice dataset and keep them."
                    ),
                    "const": "keep",
                },
                {
                    "description": (
                        "Replace existing attributes by attributes"
                        " of last slice dataset."
                    ),
                    "const": "replace",
                },
                {
                    "description": (
                        "Update existing attributes by attributes"
                        " of last slice dataset."
                    ),
                    "const": "update",
                },
                {
                    "description": "Ignore attributes from slice datasets.",
                    "const": "ignore",
                },
            ],
            "default": DEFAULT_ATTRS_UPDATE_MODE,
        },
        permit_eval={
            "description": (
                "Allow for dynamically computed values in dataset attributes"
                " `attrs` using the syntax `{{ expression }}`. "
                " Executing arbitrary Python expressions is a security"
                " risk, therefore this must be explicitly enabled."
                " Refer to the user guide for more information."
            ),
            "type": "boolean",
            "default": False,
        },
        target_dir={
            "description": (
                "The URI or local path of the target Zarr dataset."
                " Must specify a directory whose parent directory must exist."
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
        slice_source={
            "description": (
                "The fully qualified name of a class or function that provides"
                " a slice source for each slice item. If a class is given, it must be "
                " derived from `zappend.api.SliceSource`."
                " If a function is given, it must return an instance of "
                " `zappend.api.SliceSource`."
                " Refer to the user guide for more information."
            ),
            "type": "string",
            "minLength": 1,
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
        force_new={
            "description": (
                "Force creation of a new target dataset. "
                " An existing target dataset (and its lock) will be"
                " permanently deleted before appending of slice datasets"
                " begins. WARNING: the deletion cannot be rolled back."
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
        profiling=PROFILING_SCHEMA,
        logging=LOGGING_SCHEMA,
        dry_run={
            "description": (
                "If `true`, log only what would have been done,"
                " but don't apply any changes."
            ),
            "type": "boolean",
            "default": False,
        },
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
        return schema_to_markdown(CONFIG_SCHEMA_V1)
    else:
        return dict(CONFIG_SCHEMA_V1)
