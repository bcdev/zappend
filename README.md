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

`zappend` is a tool written in Python that is used for robustly creating and updating 
Zarr datacubes from smaller dataset slices. It is built on top of the awesome Python
packages [xarray](https://docs.xarray.dev/) and [zarr](https://zarr.readthedocs.io/).

## Motivation

The objective of `zappend` is to address recurring memory issues when generating large 
geospatial datacubes using the [Zarr format](https://zarr.readthedocs.io/en/stable/spec/v2.html) 
by subsequently concatenating data slices along an append dimension, e.g., `time` 
(the default) for geospatial satellite observations. 
Each append step is atomic, that is, the append operation is a transaction that can be 
rolled back, in case the append operation fails. This ensures integrity of the target 
data cube. 

## Features

The `zappend` tool provides the following features:

* **Locking**: While the target dataset is being modified, a file lock is created, 
  effectively preventing concurrent dataset modifications.
* **Transaction-based dataset appends**: On failure during an append step, 
  the transaction is rolled back, so that the target dataset remains valid and 
  preserves its integrity.
* **Filesystem transparency**: The target dataset may be generated and updated in 
  any writable filesystems supported by the 
  [fsspec](https://filesystem-spec.readthedocs.io/) package. 
  The same holds for the slice datasets to be appended.
* **Dataset polling**: The tool can be configured to wait for slice datasets to 
  become available. 
* **CLI and Python API**: The tool can be used in a shell using the [`zappend`](cli.md)
  command or from Python. When used from Python using the 
  [`zappend()`](api.md) function, slice datasets can be passed as local file paths, 
  URIs, as datasets of type 
  [xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html), or as custom 
  [zappend.api.SliceSource](https://bcdev.github.io/zappend/api/#class-slicesource) objects.



More about zappend can be found in its 
[documentation](https://bcdev.github.io/zappend/).
