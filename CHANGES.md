## Version 0.2.1 (in development)

* Allow for passing custom slice sources via the configuration.
  The new configuration setting `slice_source` is the name of a class 
  derived from `zappend.api.SliceSource` or a function that creates an instance 
  of `zappend.api.SliceSource`. If `slice_source` is given, slices passed to 
  the zappend function or CLI command will be interpreted as parameter(s) 
  passed to the constructor of the specified class or the factory function. 
  [#27]

* Using `sizes` instead of `dims` attribute of `xarray.Dataset` in 
  implementation code. [#25] 

* Enhanced documentation including docstrings of several Python API objects.
  Included example NB in the documentation (`docs/example.ipynb`).


## Version 0.2.0 (from 2024-18-01)

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
