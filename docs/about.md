# About zappend

## Change Log

You can find the complete `zappend` changelog 
[here](https://github.com/bcdev/zappend/blob/main/CHANGES.md). 

## Reporting

If you have suggestions, ideas, feature requests, or if you have identified
a malfunction or error, then please 
[post an issue](https://github.com/bcdev/zappend/issues). 

## Contributions

The `zappend` project welcomes contributions of any form
as long as you respect our 
[code of conduct](https://github.com/bcdev/zappend/blob/main/CODE_OF_CONDUCT.md)
and follow our 
[contribution guide](https://github.com/bcdev/zappend/blob/main/CONTRIBUTING.md).

If you'd like to submit code or documentation changes, we ask you to provide a 
pull request (PR) 
[here](https://github.com/bcdev/zappend/pulls). 
For code and configuration changes, your PR must be linked to a 
corresponding issue. 

## Development

Setup development environment:

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
pip install -r requirements-docs.txt
```

### Testing and Coverage

`zappend` uses [pytest](https://docs.pytest.org/) for unit-level testing 
and code coverage analysis.

```bash
pytest --cov=zappend tests
```

### Code Style

`zappend` source code is formatted using the [black](https://black.readthedocs.io/) tool.

```bash
black zappend
```

### Documentation

`zappend` documentation is build using the [mkdocs](https://www.mkdocs.org/) tool.

```bash
pip install -r requirements-doc.txt

mkdocs build
mkdocs serve
mkdocs gh-deploy
```

## Original Requirements

### Core Requirements

* Create a target Zarr dataset by appending Zarr dataset slices along a 
  given *append dimension*, usually `time`.   
* The tool takes care of modifying the target dataset using the slices,
  but doesn't care how the slice datasets are created.
* Slice datasets may be given as URIs with storage options or as 
  in-memory datasets of type 
  [xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html)
  or 
  [xcube.core.mldataset.MultiLevelDataset](https://xcube.readthedocs.io/en/latest/mldatasets.html).
* Target and slices are allowed to live in different filesystems.
* The tool is configurable. The configuration defines 
  - the append dimension;
  - optional target encoding for all or individual target variables;
  - the target path into the target filesystem;
  - optional target storage options;
  - optional slice storage options.
* The target chunking of the append dimension equals the size of the append 
  dimension in each slice and vice versa. 
* The target encoding should allow for specifying the target storage chunking, 
  data type, and compression. 
* The target encoding should also allow for packing floating point data into 
  integer data with fewer bits using scaling factor and offset.
* Detect coordinate variables and allow them to stay un-chunked.
  This is important for coordinate variables containing or corresponding 
  to the append-dimension.
* If the target does not exist, it will be created from a copy of the first 
  slice. This first slice will specify any not-yet-configured properties
  of the target dataset, e.g., the append dimension chunking.
* If the target exists, the slice will be appended. Check if the slice to be 
  appended is last. If not, refuse to append (alternative: insert but this is 
  probably difficult or error prone).
* Slices are appended in the order they are provided.
* If a slice is not yet available, wait and retry until it 
  - exists, and
  - is complete.
* Check for each slice that it is valid. A valid slice
  - is self-consistent, 
  - has the same structure as target, and
  - has an append dimension whose size is equal to the target chunking of
    this dimension.
* Before appending a slice, lock the target so that another tool invocation 
  can recognize it, e.g., write a lock file.
* If the target is locked, either wait until it becomes available or exit 
  with an error. The behaviour is controlled by a tool option.
* After successfully appending a slice, remove the lock from the target.
* Appending a slice shall be an atomic operation to ensure target dataset 
  integrity. That is, in case a former append step failed, a rollback must
  be performed to restore the last valid state of the target. Rolling back  
  shall take place after an append failed, or before a new slice is appended,
  or to sanitize a target to make it usable again. Rolling back shall 
  include restoring all changed files, removing all added files, 
  and removing any locks. 
* The tool shall allow for continuing appending slices at the point
  it failed.
* The tool shall offer a CLI and a Python API.
  - Using the CLI, slices are given as a variadic argument that provides the 
    file paths into the slice filesystem.
  - Using the Python API, it shall be possible to provide the slices by 
    specifying a function that generates the slice datasets and an
    iterable providing the arguments for the function.
    This is similar how the Python `map()` built-in works.

### Further Ideas

* Allow for inserting and deleting slices.
* Allow specifying a constant delta between coordinates of the append dimension.
* Verify append dimension coordinates increase or decrease monotonically. 
* Verify coordinate deltas of append dimension to be constant. 
* Integration with [xcube](https://github.com/dcs4cop/xcube):
  * Add xcube server API: Add endpoint to xcube server that works similar 
    to the CLI and also uses a similar request parameters.
  * Use it in xcube data stores for the `write_data()` method, as a parameter 
    to enforce sequential writing of Zarr datasets as a robust option when a 
    plain write fails.


## License

`zappend` is made available under the terms and conditions of the MIT License:

Copyright (c) 2023 Brockmann Consult Development

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
