# zappend

Tool to create and update a Zarr dataset from smaller subsets

## Requirements

### Essential

* Create a target Zarr dataset by appending subset Zarr datasets 
  along a given dimension.
* Don't care how the subset datasets are created.
* If the target does not exist, it will be created from a 
  copy of the first subset. 
* Subsets are appended in the order they are provided.
* If a subset is not yet available, wait until it 
  - exists, and
  - is complete.
* Check for each subset that it is valid. A valid subset
  - is consistent, and
  - has the same structure as target.
* Detect coordinates similar to xarray.
* Target and subsets are allowed live in different filesystems.
* The configuration defines 
  - the append dimension;
  - the target and the target filesystem;
  - the subsets and the subsets filesystem;
* While appending, create a lock file.
* Fail with an error, if a lock file exists
* Should the processing fail, rollback to the last valid state.
  This may include:
  - Create backup copies of all changed files
  - Remove all files
* After each successful append of a subset, store the state 
  in the lock file and the target. 

### Nice to have

* Try getting along without using `xarray`, use `zarr` only,
  but honor the xarray `__ARRAY_DIMENSIONS__` attribute. 
