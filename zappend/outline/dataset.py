# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Any

import xarray as xr

from ..config import DEFAULT_APPEND_DIM
from .variable import VariableOutline
from zappend.fsutil.fileobj import FileObj


class DatasetOutline:
    def __init__(self,
                 dims: dict[str, int],
                 variables: dict[str, "VariableOutline"]):
        self.dims = dims
        self.variables = variables

    @classmethod
    def from_zarr(cls, zarr_file: FileObj) -> "DatasetOutline":
        # Check: Because we expect a Zarr directory structure, we can
        #   directly load dataset Zarr metadata files to construct
        #   the dataset outline. This potentially be faster.
        #   For time being we hope, xr.open_zarr() is equally fast.
        with xr.open_zarr(zarr_file.uri,
                          storage_options=zarr_file.storage_options,
                          decode_cf=False) as dataset:
            return DatasetOutline.from_dataset(dataset)

    @classmethod
    def from_dataset(cls, ds: xr.Dataset) -> "DatasetOutline":
        return DatasetOutline(
            {str(k): v for k, v in ds.dims.items()},
            {str(k): VariableOutline.from_dataset(v)
             for k, v in ds.variables.items()}
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DatasetOutline":
        return DatasetOutline(
            dict(**config.get("fixed_dims", {}),
                 **{config.get("append_dim", "time"): -1}),
            {k: VariableOutline.from_config(v)
             for k, v in config.get("variables", {}).items()}
        )

    def get_noncompliance(self,
                          other: "DatasetOutline",
                          append_dim: str = DEFAULT_APPEND_DIM) -> list[str]:
        messages: list[str] = []
        if append_dim not in other.dims:
            messages.append(f"Append dimension {append_dim!r} not found")
        elif other.dims[append_dim] <= 0:
            messages.append(f"Non-positive size of"
                            f" append dimension {append_dim!r}:"
                            f" {other.dims[append_dim]}")
        for dim_name, dim_size in self.dims.items():
            if dim_name != append_dim:
                if dim_name not in self.dims:
                    messages.append(f"Missing dimension {dim_name!r}")
                elif self.dims[dim_name] != dim_size:
                    messages.append(f"Wrong size for dimension {dim_name!r},"
                                    f" expected {dim_size},"
                                    f" got {self.dims[dim_name]}")
        for var_name, var_schema in self.variables.items():
            if var_name not in other.variables:
                messages.append(f"Missing variable {var_name!r}")
            else:
                other_variable = other.variables[var_name]
                var_noncompliance = var_schema.get_noncompliance(
                    other_variable)
                if var_noncompliance:
                    messages.append(f"Non-compliant variable {var_name!r}:")
                    for m in var_noncompliance:
                        messages.append("  " + m)
        return messages
