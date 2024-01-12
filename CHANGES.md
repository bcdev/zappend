## Version 0.1.2 (in development)

### Enhancements

* Introduced new configuration flag `persist_mem_slices`. 
  If set, in-memory `xr.Dataset` instances will be first persisted to a 
  temporary Zarr, then reopened, and then appended to the target dataset. [#11]
* Improved readability of generated configuration documentation.
* Using `requirements-dev.txt` for development package dependencies.

### Fixes

* Fixed problem when passing slices opened from NetCDF files. The error was 
  `TypeError: VariableEncoding.__init__() got an unexpected keyword argument 'chunksizes'`. 
  [#14]

* Fixed problem where info about closing slice was logged twice. [#9]


## Version 0.1.1

Metadata fixes in `setup.cfg`. No actual code changes.

## Version 0.1.0

*Initial version from 2024-01-10.*
