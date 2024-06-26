# Configuration Reference

Given here are all configuration settings for `zappend`.

## Target Outline

### `append_dim`

Type _string_.
The name of the variadic append dimension.
Defaults to `"time"`.

### `append_step`

If set, enforces a step size in the append dimension between two slices or just enforces a direction.
Must be one of the following:

  - Arbitrary step size or not applicable.
    Its value is `null`.

  - Monotonically increasing.
    Its value is `"+"`.

  - Monotonically decreasing.
    Its value is `"-"`.

  - Type _string_.
    A positive or negative time delta value, such as `12h`, `2D`, `-1D`.

  - Type _number_.
    A positive or negative numerical delta value.

Defaults to `null`.

### `fixed_dims`

Type _object_.
Specifies the fixed dimensions of the target dataset. Keys are dimension names, values are dimension sizes.
The object's values are of type _integer_.

### `included_variables`

Type _array_.
Specifies the names of variables to be included in the target dataset. Defaults to all variables found in the first contributing dataset.
The items of the list are of type _string_.

### `excluded_variables`

Type _array_.
Specifies the names of individual variables to be excluded  from all contributing datasets.
The items of the list are of type _string_.

### `variables`

Type _object_.
Defines dimensions, encoding, and attributes for variables in the target dataset. Object property names refer to variable names. The special name `*` refers to all variables, which is useful for defining common values.
The object's values are of type _object_.
Variable metadata.

  - `dims`:
    The names of the variable's dimensions in the given order. Each dimension must exist in contributing datasets.
    The items of the list are of type _string_.

  - `encoding`:
    Variable Zarr storage encoding. Settings given here overwrite the encoding settings of the first contributing dataset.
      - `dtype`:
        Must be one of `"int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64", "float32", "float64"`.
      - `chunks`:
        Must be one of the following:
          - Type _array_.
            Chunk sizes for each dimension of the variable.
            The items of the list must be one of the following:
              - Type _integer_.
                Dimension is chunked using given size.
              - Disable chunking in this dimension.
                Its value is `null`.
          - Disable chunking in all dimensions.
            Its value is `null`.
      - `fill_value`:
        Must be one of the following:
          - Type _number_.
            A number of type and unit of the given storage `dtype`.
          - Not-a-number. Can be used only if storage `dtype` is `float32` or `float64`.
            Its value is `"NaN"`.
          - No fill value.
            Its value is `null`.
      - `scale_factor`:
        Scale factor for computing the in-memory value: `memory_value = scale_factor * storage_value + add_offset`.
      - `add_offset`:
        Add offset for computing the in-memory value: `memory_value = scale_factor * storage_value + add_offset`.
      - `units`:
        Units of the storage data type if memory data type is date/time.
      - `calendar`:
        The calendar to be used if memory data type is date/time.
      - `compressor`:
        Compressor definition. Set to `null` to disable data compression. Allowed parameters depend on the value of `id`.
        The key `id` is required.
          - `id`:
            Compressor identifier
      - `filters`:
        List of filters. Set to `null` to not use filters.
        The items of the list are of type _object_.
        Filter definition. Allowed parameters depend on the value of `id`.
        The key `id` is required.
          - `id`:
            Compressor identifier

  - `attrs`:
    Arbitrary variable metadata attributes.

### `attrs`

Type _object_.
Arbitrary dataset attributes. If `permit_eval` is set to `true`, string values may include Python expressions enclosed in `{{` and `}}` to dynamically compute attribute values; in the expression, the current dataset  is named `ds`. Refer to the user guide for more information.

### `attrs_update_mode`

The mode used update target attributes from slice attributes. Independently of this setting, extra attributes configured by the `attrs` setting will finally be used to update the resulting target attributes.
Must be one of the following:

  - Use attributes from first slice dataset and keep them.
    Its value is `"keep"`.

  - Replace existing attributes by attributes of last slice dataset.
    Its value is `"replace"`.

  - Update existing attributes by attributes of last slice dataset.
    Its value is `"update"`.

  - Ignore attributes from slice datasets.
    Its value is `"ignore"`.

Defaults to `"keep"`.

### `zarr_version`

The Zarr version to be used.
Its value is `2`.
Defaults to `2`.

## Data I/O - Target

### `target_dir`

Type _string_.
The URI or local path of the target Zarr dataset. Must specify a directory whose parent directory must exist.

### `target_storage_options`

Type _object_.
Options for the filesystem given by the URI of `target_dir`.

### `force_new`

Type _boolean_.
Force creation of a new target dataset.  An existing target dataset (and its lock) will be permanently deleted before appending of slice datasets begins. WARNING: the deletion cannot be rolled back.
Defaults to `false`.

## Data I/O - Slices

### `slice_storage_options`

Type _object_.
Options for the filesystem given by the protocol of the URIs of contributing datasets.

### `slice_engine`

Type _string_.
The name of the engine to be used for opening contributing datasets. Refer to the `engine` argument of the function `xarray.open_dataset()`.

### `slice_polling`

Defines how to poll for contributing datasets.
Must be one of the following:

  - No polling, fail immediately if dataset is not available.
    Its value is `false`.

  - Poll using default values.
    Its value is `true`.

  - Type _object_.
    Polling parameters.
    The keys `interval`, `timeout` are required.
      - `interval`:
        Polling interval in seconds.
        Defaults to `2`.
      - `timeout`:
        Polling timeout in seconds.
        Defaults to `60`.

### `slice_source`

Type _string_.
The fully qualified name of a class or function that receives a slice item as argument(s) and provides the slice dataset. If a class is given, it must be derived from `zappend.api.SliceSource`. If the function is a context manager, it must yield an `xarray.Dataset`. If a plain function is given, it must return any valid slice item type. Refer to the user guide for more information.

### `slice_source_kwargs`

Type _object_.
Extra keyword-arguments passed to a configured `slice_source` together with each slice item.

### `persist_mem_slices`

Type _boolean_.
Persist in-memory slices and reopen from a temporary Zarr before appending them to the target dataset. This can prevent expensive re-computation of dask chunks at the cost of additional i/o.
Defaults to `false`.

## Data I/O - Transactions

### `temp_dir`

Type _string_.
The URI or local path of the directory that will be used to temporarily store rollback information.

### `temp_storage_options`

Type _object_.
Options for the filesystem given by the protocol of `temp_dir`.

### `disable_rollback`

Type _boolean_.
Disable rolling back dataset changes on failure. Effectively disables transactional dataset modifications, so use this setting with care.
Defaults to `false`.

## Miscellaneous

### `version`

Configuration schema version. Allows the schema to evolve while still preserving backwards compatibility.
Its value is `1`.
Defaults to `1`.

### `dry_run`

Type _boolean_.
If `true`, log only what would have been done, but don't apply any changes.
Defaults to `false`.

### `permit_eval`

Type _boolean_.
Allow for dynamically computed values in dataset attributes `attrs` using the syntax `{{ expression }}`.  Executing arbitrary Python expressions is a security risk, therefore this must be explicitly enabled. Refer to the user guide for more information.
Defaults to `false`.

### `extra`

Type _object_.
Extra settings. Intended use is by a `slice_source` that expects an argument named `ctx` to access the extra settings and other configuration.

### `profiling`

Profiling configuration. Allows for runtime profiling of the processing.
Must be one of the following:

  - Type _boolean_.
    If set, profiling is enabled and output is logged using level `"INFO"`. Otherwise, profiling is disabled.

  - Type _string_.
    Profile path. Enables profiling and writes a profiling report to given path.

  - Type _object_.
    Detailed profiling configuration.
      - `enabled`:
        Enable or disable profiling.
      - `path`:
        Local file path for profiling output.
      - `log_level`:
        Defaults to `"INFO"`.
        Must be one of `"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"`.
      - `keys`:
        Sort output according to the supplied column names. Refer to [Stats.sort_stats(*keys)](https://docs.python.org/3/library/profile.html#pstats.Stats.sort_stats).
        Defaults to `["tottime"]`.
        Must be one of `"calls", "cumulative", "cumtime", "file", "filename", "module", "ncalls", "pcalls", "line", "name", "nfl", "stdname", "time", "tottime"`.
      - `restrictions`:
        Used to limit the list down to the significant entries in the profiling report. Refer to [Stats.print_stats(*restrictions)](https://docs.python.org/3/library/profile.html#pstats.Stats.print_stats).
        The items of the list must be one of the following:
          - Type _integer_.
            Select a count of lines.
          - Type _number_.
            Select a percentage of lines.
          - Type _string_.
            Pattern-match the standard name that is printed.

### `logging`

Logging configuration.
Must be one of the following:

  - Type _boolean_.
    Shortform that enables logging to the console using log level `"INFO"`.

  - Shortform that enables logging to the console using the specified log level.
    Must be one of `"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"`.

  - Type _object_.
    Detailed logging configuration. For details refer to the [dictionary schema](https://docs.python.org/3/library/logging.config.html#logging-config-dictschema) of the Python module `logging.config`.
    The key `version` is required.
      - `version`:
        Its value is `1`.
      - `formatters`:
        Formatter definitions. Each key is a formatter id and each value is an object describing how to configure the corresponding formatter.
        The object's values are of type _object_.
        Formatter configuration.
          - `format`:
            Format string in the given `style`.
            Defaults to `"%(message)s"`.
          - `datefmt`:
            Format string in the given `style` for the date/time portion.
            Defaults to `"%Y-%m-%d %H:%M:%S,uuu"`.
          - `style`:
            Must be one of `"%", "{", "$"`.
      - `filters`:
        Filter definitions. Each key is a filter id and each value is a dict describing how to configure the corresponding filter.
        The object's values are of type _object_.
        Filter configuration.
      - `handlers`:
        Handler definitions. Each key is a handler id and each value is an object describing how to configure the corresponding handler.
        The object's values are of type _object_.
        Handler configuration. All keys other than the following are passed through as keyword arguments to the handler's constructor.
        The key `class` is required.
          - `class`:
            The fully qualified name of the handler class. See [logging handlers](https://docs.python.org/3/library/logging.handlers.html).
          - `level`:
            Must be one of `"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"`.
          - `formatter `:
            The id of the formatter for this handler.
          - `filters`:
            A list of ids of the filters for this logger.
            The items of the list are of type _string_.
      - `loggers`:
        Logger definitions. Each key is a logger name and each value is an object describing how to configure the corresponding logger. The tool's logger has the id `'zappend'`.
        The object's values are of type _object_.
        Logger configuration.
          - `level`:
            Must be one of `"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"`.
          - `propagate `:
            The propagation setting of the logger.
          - `filters`:
            A list of ids of the filters for this logger.
            The items of the list are of type _string_.
          - `handlers`:
            A list of ids of the handlers for this logger.
            The items of the list are of type _string_.


