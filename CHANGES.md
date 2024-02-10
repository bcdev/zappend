## Version 0.4.1 (in development)

### Fixes

* Global metadata attributes of target dataset is no longer empty. [#56]

## Version 0.4.0 (from 2024-02-08)

### Enhancements

* A new configuration setting `append_step` can be used to validate
  the step sizes between the labels of a coordinate variable associated with
  the append dimension. Its value can be a number for numerical labels
  or a time delta value of the form `8h` (8 hours) or `2D` (two days) for
  date/time labels. The value can also be negative. [#21] 

* The configuration setting `append_step` can take the special values
  `"+"` and `"-"` which are used to verify that the labels are monotonically 
  increasing and decreasing, respectively. [#20]

* It is now possible to reference environment variables
  in configuration files using the syntax `${ENV_VAR}`. [#36]

* Added a demo Notebook `examples/zappend-demo.ipynb` and linked 
  it by a binder badge in README.md. [#47] 

### Fixes

* When `slice_source` was given as class or function and passed  
  to the `zappend()` function either as configuration entry or as keyword 
  argument, a `ValidationError` was accidentally raised. [#49]

* Fixed an issue where an absolute lock file path was computed if the target 
  Zarr path was relative in the local filesystem, and had no parent directory.
  [#45]

## Version 0.3.0 (from 2024-01-26)

### Enhancements

* Allow for passing custom slice sources via the configuration.
  The new configuration setting `slice_source` is the name of a class 
  derived from `zappend.api.SliceSource` or a function that creates an instance 
  of `zappend.api.SliceSource`. If `slice_source` is given, slices passed to 
  the zappend function or CLI command will be interpreted as parameter(s) 
  passed to the constructor of the specified class or the factory function. 
  [#27]

* It is now possible to configure runtime profiling of the `zappend`
  processing using the new configuration setting `profiling`. [#39]

* Added `--version` option to CLI. [#42]

* Using `sizes` instead of `dims` attribute of `xarray.Dataset` in 
  implementation code. [#25] 

* Enhanced documentation including docstrings of several Python API objects.

### Fixes

* Fixed a problem where the underlying i/o stream of a persistent slice dataset 
  was closed immediately after opening the dataset. [#31]
  
* Now logging ignored encodings on level DEBUG instead of WARNING because they 
  occur very likely when processing NetCDF files.

## Version 0.2.0 (from 2024-01-18)

### Enhancements

* Introduced _slice factories_
    - Allow passing slice object factories to the `zappend()` function.
      Main use case is to return instances of a custom `zappend.api.SliceSource` 
      implemented by users. [#13]

    - The utility functions `to_slice_factories` and `to_slice_factory`
      exported by `zappend.api` ease passing inputs  specific for a custom
      `SliceSource` or other callables that can produce a slice object. [#22]

* Introduced new configuration flag `persist_mem_slices`. 
  If set, in-memory `xr.Dataset` instances will be first persisted to a 
  temporary Zarr, then reopened, and then appended to the target dataset. [#11]

* Added initial documentation. [#17]

* Improved readability of generated configuration documentation.

* Using `requirements-dev.txt` for development package dependencies.

### Fixes

* Fixed problem when passing slices opened from NetCDF files. The error was 
  `TypeError: VariableEncoding.__init__() got an unexpected keyword argument 'chunksizes'`. 
  [#14]

* Fixed problem where info about closing slice was logged twice. [#9]


## Version 0.1.1 (from 2024-01-10)

Metadata fixes in `setup.cfg`. No actual code changes.

## Version 0.1.0 (from 2024-01-10)

*The initial release.*
