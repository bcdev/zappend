[![CI](https://github.com/bcdev/zappend/actions/workflows/tests.yml/badge.svg)](https://github.com/bcdev/zappend/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/bcdev/zappend/graph/badge.svg?token=B3R6bNmAUp)](https://codecov.io/gh/bcdev/zappend)
[![PyPI - Version](https://img.shields.io/pypi/v/zappend)](https://pypi.org/project/zappend/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![GitHub License](https://img.shields.io/github/license/bcdev/zappend)](https://github.com/bcdev/zappend)


# zappend

A tool written in Python for robustly creating and updating Zarr datacubes 
from smaller slices.

The objective of zappend is to address recurring memory issues when 
generating large geospatial data cubes using the 
[Zarr format](https://zarr.readthedocs.io/) by subsequently concatenating data
slices along an append dimension, usually `time`. Each append step is atomic, 
that is, the append operation is a transaction that can be rolled back, 
in case the append operation fails. This ensures integrity of the target 
data cube. 

More about zappend can be found in its 
[documentation](https://bcdev.github.io/zappend/).
