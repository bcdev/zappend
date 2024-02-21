<h1 align="center">
    <img src="docs/assets/logo.png" width="64" title="zappend">
    &nbsp;zappend
</h1>

<div align="center">

[![CI](https://github.com/bcdev/zappend/actions/workflows/tests.yml/badge.svg)](https://github.com/bcdev/zappend/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/bcdev/zappend/graph/badge.svg?token=B3R6bNmAUp)](https://codecov.io/gh/bcdev/zappend)
[![PyPI Version](https://img.shields.io/pypi/v/zappend)](https://pypi.org/project/zappend/)
[![Conda Version](https://anaconda.org/conda-forge/zappend/badges/version.svg)](https://anaconda.org/conda-forge/zappend)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/bcdev/zappend/HEAD?labpath=examples%2Fzappend-demo.ipynb)
[![GitHub License](https://img.shields.io/github/license/bcdev/zappend)](https://github.com/bcdev/zappend)

</div>

<!--- Align following sections with docs/index.md -->

---

`zappend` is a tool written in Python that is used for robustly creating and 
updating Zarr datacubes from smaller dataset slices. It is built on top of the 
awesome Python packages [xarray](https://docs.xarray.dev/) and [zarr](https://zarr.readthedocs.io/).

## Motivation

The objective of `zappend` is enabling geodata scientists and developers to 
robustly create large data cubes. The tool performs transaction-based dataset 
appends to existing data cubes in the 
[Zarr format](https://zarr.readthedocs.io/en/stable/spec/v2.html). If an error 
occurs during an append step — typically due to I/O problems or out-of-memory 
conditions — `zappend` will automatically roll back the operation, ensuring that 
the existing data cube maintains its structural integrity. The design drivers 
behind zappend are first ease of use and secondly, high configurability 
regarding filesystems, data source types, data cube outline and encoding. 

The tool comprises a command-line interface, a Python API for programmatic 
control, and a comprehensible documentation to guide users effectively. 
You can easily install `zappend` as a plain Python package using either 
`pip install zappend` or `conda install -conda-forge zappend`.

## Features

The `zappend` tool provides the following features:

* **Locking**: While the target dataset is being modified, a file lock is 
  created, effectively preventing concurrent dataset modifications.
* **Transaction-based dataset appends**: On failure during an append step, 
  the transaction is rolled back, so that the target dataset remains valid and 
  preserves its integrity.
* **Filesystem transparency**: The target dataset may be generated and updated 
  in any writable filesystems supported by the 
  [fsspec](https://filesystem-spec.readthedocs.io/) package. 
  The same holds for the slice datasets to be appended.
* **Dataset polling**: The tool can be configured to wait for slice datasets to 
  become available. 
* **Dynamic attributes**: Use syntax `{{ expression }}` to update the target 
  dataset with dynamically computed attribute values. 
* **CLI and Python API**: The tool can be used in a shell using the 
  [`zappend`](cli.md) command or from Python. When used from Python using the 
  [`zappend()`](api.md) function, slice datasets can be passed as local file 
  paths, URIs, as datasets of type 
  [xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html), or as custom 
  [zappend.api.SliceSource](https://bcdev.github.io/zappend/api/#class-slicesource) objects.

  
More about zappend can be found in its 
[documentation](https://bcdev.github.io/zappend/).
