## Version 0.7.0 (in development)

* Made writing custom slice sources easier and more flexible: (#82)

  - Slice items can now be a `contextlib.AbstractContextManager` 
    so custom slice functions can now be used with
    [@contextlib.contextmanager](https://docs.python.org/3/library/contextlib.html#contextlib.contextmanager).
  
  - Introduced `SliceSource.close()` so
    [contextlib.closing()](https://docs.python.org/3/library/contextlib.html#contextlib.closing)
    is applicable. Deprecated `SliceSource.dispose()`.
    
  - Introduced new optional configuration setting `slice_source_kwargs` that
    contains keyword-arguments, which are passed to a configured `slice_source` together with 
    each slice item.


  - Introduced optional configuration setting `slice_source_kwargs` that
    contains keyword-arguments passed to a configured `slice_source` together with 
    each slice item.

  - Introduced optional configuration setting `extra` that holds additional 
    configuration not validated by default. Intended use is by a `slice_source` that 
    expects an argument named `ctx` and therefore can access the configuration.

* Improve the configuration reference Introduced configuration schema categories


## Version 0.6.0 (from 2024-03-12)

### Enhancements

* Added configuration setting `force_new`, which forces creation of a new 
  target dataset. An existing target dataset (and its lock) will be
  permanently deleted before appending of slice datasets begins. (#72)

* Chunk sizes can now be `null` for a given dimension. In this case the actual 
  chunk size used is the size of the array's shape in that dimension. (#77)

### API Changes

* Simplified writing of custom slice sources for users. The configuration setting
  `slice_source` can now be a `SliceSource` class or any function that returns a
  _slice item_: a local file path or URI, an `xarray.Dataset`, 
  a `SliceSource` object. 
  Dropped concept of _slice factories_ entirely, including functions
  `to_slice_factory()` and `to_slice_factories()`. (#78)

* Extracted `Config` class out of `Context` and made available via new
  `Context.config: Config` property. The change concerns any usages of the
  `ctx: Context` argument passed to user slice factories. (#74)

## Version 0.5.1 (from 2024-02-23)

* Fixed rollback for situations where writing to Zarr fails shortly after the
  Zarr directory has been created. (#69)
  
  In this case the error message was
  ```TypeError: Transaction._delete_dir() missing 1 required positional argument: 'target_path'```. 


## Version 0.5.0 (from 2024-02-19)

### Enhancements

* The configuration setting  `attrs` can now be used to define dynamically 
  computed dataset attributes using the syntax `{{ expression }}`. (#60)
  
  Example:
  ```yaml
  permit_eval: true
  attrs:
    title: HROC Ocean Colour Monthly Composite
    time_coverage_start: {{ lower_bound(ds.time) }}
    time_coverage_end: {{ upper_bound(ds.time) }}
  ```

* Introduced new configuration setting `attrs_update_mode` that controls 
  how dataset attributes are updated. (#59)

* Simplified logging to console. You can now set configuration setting 
  `logging` to a log level which will implicitly enable console logging with
  given log level. (#64)

* Added a section in the notebook `examples/zappend-demo.ipynb`
  that demonstrates transaction rollbacks.

* Added CLI option `--traceback`. (#57)

* Added a section in the notebook `examples/zappend-demo.ipynb`
  that demonstrates transaction rollbacks.

### Fixes

* Fixed issue where a NetCDF package was missing to run the 
  demo Notebook `examples/zappend-demo.ipynb` in 
  [Binder](https://mybinder.readthedocs.io/). (#47)

## Version 0.4.1 (from 2024-02-13)

### Fixes

* Global metadata attributes of target dataset is no longer empty. (#56)

* If the target _parent_ directory did not exist, an exception was raised 
  reporting that the lock file to be written does not exist. Changed this to
  report that the target parent directory does not exist. (#55)

### Enhancements

* Added missing documentation of the `append_step` setting in the 
  [configuration reference](https://bcdev.github.io/zappend/config/).

## Version 0.4.0 (from 2024-02-08)

### Enhancements

* A new configuration setting `append_step` can be used to validate
  the step sizes between the labels of a coordinate variable associated with
  the append dimension. Its value can be a number for numerical labels
  or a time delta value of the form `8h` (8 hours) or `2D` (two days) for
  date/time labels. The value can also be negative. (#21) 

* The configuration setting `append_step` can take the special values
  `"+"` and `"-"` which are used to verify that the labels are monotonically 
  increasing and decreasing, respectively. (#20)

* It is now possible to reference environment variables
  in configuration files using the syntax `${ENV_VAR}`. (#36)

* Added a demo Notebook `examples/zappend-demo.ipynb` and linked 
  it by a binder badge in README.md. (#47) 

### Fixes

* When `slice_source` was given as class or function and passed  
  to the `zappend()` function either as configuration entry or as keyword 
  argument, a `ValidationError` was accidentally raised. (#49)

* Fixed an issue where an absolute lock file path was computed if the target 
  Zarr path was relative in the local filesystem, and had no parent directory.
  (#45)

## Version 0.3.0 (from 2024-01-26)

### Enhancements

* Allow for passing custom slice sources via the configuration.
  The new configuration setting `slice_source` is the name of a class 
  derived from `zappend.api.SliceSource` or a function that creates an instance 
  of `zappend.api.SliceSource`. If `slice_source` is given, slices passed to 
  the zappend function or CLI command will be interpreted as parameter(s) 
  passed to the constructor of the specified class or the factory function. 
  (#27)

* It is now possible to configure runtime profiling of the `zappend`
  processing using the new configuration setting `profiling`. (#39)

* Added `--version` option to CLI. (#42)

* Using `sizes` instead of `dims` attribute of `xarray.Dataset` in 
  implementation code. (#25) 

* Enhanced documentation including docstrings of several Python API objects.

### Fixes

* Fixed a problem where the underlying i/o stream of a persistent slice dataset 
  was closed immediately after opening the dataset. (#31)
  
* Now logging ignored encodings on level DEBUG instead of WARNING because they 
  occur very likely when processing NetCDF files.

## Version 0.2.0 (from 2024-01-18)

### Enhancements

* Introduced _slice factories_
    - Allow passing slice object factories to the `zappend()` function.
      Main use case is to return instances of a custom `zappend.api.SliceSource` 
      implemented by users. (#13)

    - The utility functions `to_slice_factories` and `to_slice_factory`
      exported by `zappend.api` ease passing inputs  specific for a custom
      `SliceSource` or other callables that can produce a slice object. (#22)

* Introduced new configuration flag `persist_mem_slices`. 
  If set, in-memory `xr.Dataset` instances will be first persisted to a 
  temporary Zarr, then reopened, and then appended to the target dataset. (#11)

* Added initial documentation. (#17)

* Improved readability of generated configuration documentation.

* Using `requirements-dev.txt` for development package dependencies.

### Fixes

* Fixed problem when passing slices opened from NetCDF files. The error was 
  `TypeError: VariableEncoding.__init__() got an unexpected keyword argument 'chunksizes'`. 
  (#14)

* Fixed problem where info about closing slice was logged twice. (#9)


## Version 0.1.1 (from 2024-01-10)

Metadata fixes in `setup.cfg`. No actual code changes.

## Version 0.1.0 (from 2024-01-10)

*The initial release.*
