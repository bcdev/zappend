## Configuration for the `zappend` tool


### `version`

Configuration schema version. Allows the schema to evolve while still preserving backwards compatibility.
It's value is `1`.

### `target_uri`

Type _string_.
The URI or local path of the target Zarr dataset. Must be a directory.

### `target_storage_options`

Type _object_.
Options for the filesystem given by the URI of `target_uri`.

### `slice_engine`

Type _string_.
The name of the engine to be used for opening contributing datasets. Refer to the `engine` argument of the function `xarray.open_dataset()`.

### `slice_storage_options`

Type _object_.
Options for the filesystem given by the protocol of the URIs of contributing datasets.

### `slice_polling`

Defines how to poll for contributing datasets.
Must be one of the following:
* No polling, fail immediately if dataset is not available.
  It's value is `false`.
* Poll using default values.
  It's value is `true`.
* Type _object_.
  Polling parameters.
  The keys `interval`, `timeout` are required.
  
  * `interval`:
    Type _number_.
    Polling interval in seconds.
    Defaults to `2`.
  * `timeout`:
    Type _number_.
    Polling timeout in seconds.
    Defaults to `60`.
  

### `temp_dir`

Type _string_.
The URI or local path of the directory that will be used to temporarily store rollback information.

### `temp_storage_options`

Type _object_.
Options for the filesystem given by the protocol of `temp_dir`.

### `zarr_version`

The Zarr version to be used.
It's value is `2`.

### `fixed_dims`

Type _object_.
Specifies the fixed dimensions of the target dataset. Keys are dimension names, values are dimension sizes.
Object values are:
Type _integer_.

### `append_dim`

Type _string_.
The name of the variadic append dimension.
Defaults to `"time"`.

### `variables`

Type _object_.
Defines dimensions, encoding, and attributes for variables in the target dataset. Object property names refer to variable names. The special name `*` refers to all variables, which is useful for defining common values.
Object values are:
Type _object_.
Variable metadata.

* `dims`:
  Type _array_.
  The names of the variable's dimensions in the given order. Each dimension must exist in contributing datasets.
* `encoding`:
  Type _object_.
  Variable storage encoding. Settings given here overwrite the encoding settings of the first contributing dataset.
  
  * `dtype`:
    Storage data type
    Must be one of `"int8", "uint8", "int16", "uint16", "int32", "uint32", "int64", "uint64", "float32", "float64"`.
  * `chunks`:
    Storage chunking.
    Must be one of the following:
    * Type _array_.
      Chunk sizes in the order of the dimensions.
    * Disable chunking.
      It's value is `null`.
  * `fill_value`:
    Storage fill value.
    Must be one of the following:
    * Type _number_.
      A number of type and unit of the given storage `dtype`.
    * Not-a-number. Can be used only if storage `dtype` is `float32` or `float64`.
      It's value is `"NaN"`.
    * No fill value.
      It's value is `null`.
  * `scale_factor`:
    Type _number_.
    Scale factor for computing the in-memory value: `memory_value = scale_factor * storage_value + add_offset`.
  * `add_offset`:
    Type _number_.
    Add offset for computing the in-memory value: `memory_value = scale_factor * storage_value + add_offset`.
  * `units`:
    Type _string_.
    Units of the storage data type if memory data type is date/time.
  * `calendar`:
    Type _string_.
    The calendar to be used if memory data type is date/time.
  * `compressor`:
    Type _array_ | _null_.
    Compressor. Set to `null` to disable data compression.
    The key `id` is required.
    
    * `id`:
      Type _string_.
    
  * `filters`:
    Type _array_ | _null_.
    Filters. Set to `null` to not use filters.
  
* `attrs`:
  Type _object_.
  Arbitrary variable metadata attributes.


### `included_variables`

Type _array_.
Specifies the names of variables to be included in the target dataset. Defaults to all variables found in the first contributing dataset.

### `excluded_variables`

Type _array_.
Specifies the names of individual variables to be excluded  from all contributing datasets.

### `dry_run`

Type _boolean_.
If 'true', log only what would have been done, but don't apply any changes.
Defaults to `false`.

### `logging`

Type _object_.
Logging configuration. For details refer to the [dictionary schema](https://docs.python.org/3/library/logging.config.html#logging-config-dictschema) of the Python module `logging.config`.
The key `version` is required.

* `version`:
  Logging schema version.
  It's value is `1`.
* `formatters`:
  Type _object_.
  Formatter definitions. Each key is a formatter id and each value is an object describing how to configure the corresponding formatter.
  Object values are:
  Type _object_.
  Formatter configuration.
  
  * `format`:
    Type _string_.
    Format string in the given `style`.
    Defaults to `"%(message)s"`.
  * `datefmt`:
    Type _string_.
    Format string in the given `style` for the date/time portion.
    Defaults to `"%Y-%m-%d %H:%M:%S,uuu"`.
  * `style`:
    Determines how the format string will be merged with its data.
    Must be one of `"%", "{", "$"`.
  
* `filters`:
  Type _object_.
  Filter definitions. Each key is a filter id and each value is a dict describing how to configure the corresponding filter.
  Object values are:
  Type _object_.
  Filter configuration.
* `handlers`:
  Type _object_.
  Handler definitions. Each key is a handler id and each value is an object describing how to configure the corresponding handler.
  Object values are:
  Type _object_.
  Handler configuration. All keys other than the following are passed through as keyword arguments to the handler's constructor. 
  The key `class` is required.
  
  * `class`:
    Type _string_.
    The fully qualified name of the handler class. See [logging handlers](https://docs.python.org/3/library/logging.handlers.html).
  * `level`:
    The level of the handler.
    Must be one of `"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"`.
  * `formatter `:
    Type _string_.
    The id of the formatter for this handler.
  * `filters`:
    Type _array_.
    A list of ids of the filters for this logger.
  
* `loggers`:
  Type _object_.
  Logger definitions. Each key is a logger name and each value is an object describing how to configure the corresponding logger. The tool's logger has the id `'zappend'`.
  Object values are:
  Type _object_.
  Logger configuration.
  
  * `level`:
    The level of the logger.
    Must be one of `"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG", "NOTSET"`.
  * `propagate `:
    Type _boolean_.
    The propagation setting of the logger.
  * `filters`:
    Type _array_.
    A list of ids of the filters for this logger.
  * `handlers`:
    Type _array_.
    A list of ids of the handlers for this logger.
  



