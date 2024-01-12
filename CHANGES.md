## Version 0.1.2 (in development)

* Introduced new configuration flag `persist_mem_slices`. 
  If set, in-memory `xr.Dataset` instances will be first persisted to a 
  temporary Zarr, then reopened, and then appended to the target dataset. [#11]
* Fixed problem where info about closing slice was logged twice. [#9]
* Improved readability of generated configuration documentation.
* Using `requirements-dev.txt` for development package dependencies.

## Version 0.1.1

Metadata fixes in `setup.cfg`. No actual code changes.

## Version 0.1.0

*Initial version from 2024-01-10.*
