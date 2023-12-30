# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Any

import dask.array
import numpy as np
import xarray as xr

from .metadata import get_effective_variables
from .log import logger


def tailor_target_dataset(
    dataset: xr.Dataset,
    included_var_names: set[str],
    excluded_var_names: set[str],
    target_variables: dict[str, dict[str, Any]],
    target_attrs: dict[str, Any]
) -> xr.Dataset:
    dataset = _strip_dataset(dataset,
                             included_var_names,
                             excluded_var_names)
    variables = get_effective_variables(target_variables, dataset)
    dataset = _complete_dataset(dataset, variables)

    # Complement dataset attributes and set
    # variable encoding and attributes
    dataset.attrs.update(target_attrs)
    for var_name, var_config in variables.items():
        variable = dataset.variables[var_name]
        variable.encoding = var_config["encoding"]
        variable.attrs = var_config["attrs"]
    return dataset


def tailor_slice_dataset(
    dataset: xr.Dataset,
    included_var_names: set[str],
    excluded_var_names: set[str],
    target_variables: dict[str, dict[str, Any]]
) -> xr.Dataset:
    dataset = _strip_dataset(dataset,
                             included_var_names,
                             excluded_var_names)
    variables = get_effective_variables(target_variables, dataset)
    dataset = _complete_dataset(dataset, variables)

    # Remove any encoding and attributes from slice,
    # since both are prescribed by target
    dataset.attrs.clear()
    for variable in dataset.variables.values():
        variable.encoding = {}
        variable.attrs = {}
    return dataset


def _strip_dataset(dataset: xr.Dataset,
                   included_var_names: set[str],
                   excluded_var_names: set[str]) -> xr.Dataset:
    dataset_var_names = set(map(str, dataset.variables.keys()))
    if not included_var_names:
        included_var_names = set(dataset_var_names)
    if excluded_var_names:
        included_var_names -= excluded_var_names
    drop_var_names = dataset_var_names - included_var_names
    return dataset.drop_vars(drop_var_names)


def _complete_dataset(dataset: xr.Dataset,
                      variables: dict[str, dict[str, Any]]) -> xr.Dataset:
    for var_name, var_config in variables.items():
        var = dataset.variables.get(var_name)
        if var is None:
            logger.warning(
                f"Variable {var_name!r} not found in slice dataset;"
                f" creating it."
            )

            var_dims = var_config.get("dims")
            if var_dims is None:
                raise ValueError(f"Cannot create variable {var_name!r}"
                                 f" because its dimensions are not specified")

            def get_dim_size(dim_name: str) -> int:
                return dataset.dims[dim_name]

            try:
                shape = tuple(map(get_dim_size, var_dims))
            except KeyError:
                raise ValueError(f"Cannot create variable {var_name!r}"
                                 f" because at least one of its dimensions"
                                 f" {var_dims!r} does not exist in the dataset")

            var_encoding = var_config.get("encoding") or {}
            chunks = var_encoding.get("chunks")
            if chunks is None:
                chunks = shape

            if ("_FillValue" in var_encoding
                and var_encoding["_FillValue"] is not None):
                memory_dtype = "float64"
                memory_fill_value = float("NaN")
            else:
                memory_dtype = var_encoding.get("dtype", "float32")
                memory_fill_value = var_encoding.get("_FillValue")
                if memory_fill_value is None:
                    if memory_dtype in ("float32", "float64"):
                        memory_fill_value = float("NaN")
                    else:
                        memory_fill_value = 0
            var = xr.DataArray(
                dask.array.full(shape,
                                memory_fill_value,
                                chunks=chunks,
                                dtype=np.dtype(memory_dtype)),
                dims=var_dims
            )
            dataset[var_name] = var
    return dataset
