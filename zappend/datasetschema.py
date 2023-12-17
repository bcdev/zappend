from typing import Any

import xarray as xr


class DatasetSchema:
    def __init__(self,
                 dims: dict[str, int],
                 variables: dict[str, "VariableSchema"]):
        self.dims = dims
        self.variables = variables

    @classmethod
    def from_dataset(cls, ds: xr.Dataset) -> "DatasetSchema":
        return DatasetSchema(
            {str(k): v for k, v in ds.dims.items()},
            {str(k): VariableSchema.from_dataset(v)
             for k, v in ds.variables.items()}
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DatasetSchema":
        return DatasetSchema(
            dict(**config.get("fixed_dims", {}),
                 **{config.get("append_dim", "time"): -1}),
            {k: VariableSchema.from_config(v)
             for k, v in config.get("variables", {})}
        )

    def get_noncompliance(self, other: "DatasetSchema", append_dim: str) \
            -> list[str]:
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
                var_noncompliance = var_schema.get_noncompliance(other_variable)
                if var_noncompliance:
                    messages.append(f"Non-compliant variable {var_name!r}:")
                    for m in var_noncompliance:
                        messages.append("  " + m)
        return messages


class VariableSchema:
    def __init__(self,
                 dtype: str,
                 dims: tuple[str],
                 shape: tuple[int],
                 chunks: tuple[int],
                 fill_value: int | float | None = None,
                 scale_factor: int | float | None = None,
                 add_offset: int | float | None = None,
                 compressor: dict[str, Any] | None = None,
                 filters: tuple[Any] | None = None):
        self.dtype = dtype
        self.dims = dims
        self.shape = shape
        self.chunks = chunks
        self.fill_value = fill_value
        self.scaling_factor = scale_factor
        self.add_offset = add_offset
        self.compressor = compressor
        self.filters = filters

    @classmethod
    def from_dataset(cls, var: xr.Variable) -> "VariableSchema":
        return VariableSchema(
            dtype=str(var.dtype),
            dims=tuple(map(str, var.dims)),
            shape=tuple(var.shape),
            chunks=tuple(var.chunks or var.shape),
            fill_value=var.encoding.get("fill_value"),
            scale_factor=var.attrs.get("scale_factor"),
            add_offset=var.attrs.get("add_offset"),
            compressor=var.encoding.get("compressor"),
            filters=var.encoding.get("filters")
        )

    @classmethod
    def from_config(cls, var: dict[str, Any]) -> "VariableSchema":
        return VariableSchema(
            dtype=var["dtype"],
            dims=var["dims"],
            shape=var["shape"],
            chunks=var["chunks"],
            fill_value=var["fill_value"],
            scale_factor=var["scale_factor"],
            add_offset=var["add_offset"],
            compressor=var["compressor"],
            filters=var["filters"]
        )

    def get_noncompliance(self, other: "VariableSchema") -> list[str]:
        messages: list[str] = []
        for k, v in self.__dict__:
            if not k.startswith("_"):
                other_v = getattr(other, k)
                if v != other_v:
                    messages.append(f"Incompatible {k}, expected {v},"
                                    f" got {other_v}")
        return messages
