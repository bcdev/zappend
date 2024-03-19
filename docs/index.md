<!--- Align following section with README.md -->

# zappend Documentation

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
  [slice sources](guide.md#slice-sources).

## How It Works

At its core, `zappend` calls the [to_zarr()](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.to_zarr.html#xarray-dataset-to-zarr) method of [xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html) 
for each dataset slice it receives and either creates the target dataset if it does 
not exist yet or updates it with the current slice, if it already exists.

If there is no target dataset yet, `zappend` does the following:

* get target dataset outline and encoding from configuration and first slice dataset;
* tailor first slice dataset according to target dataset outline;
* write target from first slice using target dataset encoding.

If target dataset exists, then `zappend` will:

* get target dataset outline and encoding configuration and existing target dataset;
* for each subsequent slice dataset:
    - verify target and slice dataset are compatible;
    - tailor slice dataset according to target dataset outline;
    - remove all metadata including encoding and attributes from slice dataset;
    - concatenate the "naked" slice dataset with the target dataset.
