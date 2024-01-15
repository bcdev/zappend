# The zappend Tool

`zappend` is a tool written in Python that is used for creating and updating 
a Zarr dataset from smaller dataset slices.

## Motivation

The objective of `zappend` is to address recurring memory issues when 
generating large geospatial datacubes using the 
[Zarr format](https://zarr.readthedocs.io/) by subsequently concatenating data
slices along an append dimension, usually `time`. Each append step is atomic, 
that is, the append operation is a transaction that can be rolled back, 
in case the append operation fails. This ensures integrity of the target 
data cube. 

## Features

The `zappend` tool provides the following features

* **Transaction-based dataset appends**: On failure during an append step, 
  the transaction is rolled back, so that the target dataset remains valid and 
  preserves its integrity.
* **Filesystem transparency**: The target dataset may be generated and updated in 
  any writable filesystems supported by the 
  [fsspec](https://filesystem-spec.readthedocs.io/) package. 
  The same holds for the slice datasets to be appended.
* **Slices polling**: The tool can be configured to wait for slice datasets to 
  become available. 
* **CLI and Python API**: The tool can be used in a shell using the [`zappend`](cli.md)
  command or from Python. When used from Python using the 
  [`zappend()`](api.md) function, slice datasets can be passed as local file paths, 
  URIs, or as in-memory datasets of type 
  [xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html).
  Users can implement their own _slice sources_ and provide them to the that provide 
  slice dataset objects and are disposed after each slice has been processed.

## How It Works

At its core, `zappend` calls the 
[to_zarr()](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.to_zarr.html#xarray-dataset-to-zarr)
method of 
[xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html) 
for each dataset slice it receives and either creates the target dataset if it does not 
exist yet or updates it with the current slice, if it already exists.

If there is no target dataset yet, `zappend` does the following:

* create target metadata from configuration and slice dataset;
* tailor slice according to target metadata and configuration;
* set encoding and attributes in slice according to target metadata;
* write target from slice.

If target dataset exists, then `zappend` will:

* create target metadata from configuration and target dataset;
* create slice metadata from configuration and slice dataset;
* verify target and slice metadata are compatible;
* tailor slice according to target metadata and configuration;
* remove encoding and attributes from slice;
* update target from slice.
