# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Any, Callable

import numcodecs
import numcodecs.abc
import numpy as np
import xarray as xr

from .config import merge_configs, DEFAULT_APPEND_DIM


class Undefined:
    pass


UNDEFINED = Undefined()

Codec = numcodecs.abc.Codec


class VariableEncoding:
    # noinspection PyPep8Naming
    def __init__(
        self,
        dtype: np.dtype | Undefined = UNDEFINED,
        chunks: tuple[int] | None | Undefined = UNDEFINED,
        fill_value: int | float | None | Undefined = UNDEFINED,
        scale_factor: int | float | Undefined = UNDEFINED,
        add_offset: int | float | Undefined = UNDEFINED,
        units: str | Undefined = UNDEFINED,
        calendar: str | Undefined = UNDEFINED,
        compressor: Codec | None | Undefined = UNDEFINED,
        filters: list[Codec] | None | Undefined = UNDEFINED,
    ):
        """All arguments default to UNDEFINED, so they can be distinguished
        from None, which is has a special meaning for some values.
        """
        self.dtype = dtype
        self.chunks = chunks
        self.fill_value = fill_value
        self.scale_factor = scale_factor
        self.add_offset = add_offset
        self.units = units
        self.calendar = calendar
        self.compressor = compressor
        self.filters = filters

    def to_dict(self):
        d = {
            k: v
            for k, v in self.__dict__.items()
            if not isinstance(v, Undefined) and not k.startswith("_")
        }
        if "fill_value" in d:
            d["_FillValue"] = d.pop("fill_value")
        return d


class VariableMetadata:
    def __init__(
        self,
        dims: tuple[str],
        shape: tuple[int],
        encoding: VariableEncoding,
        attrs: dict[str, Any],
    ):
        self.dims = dims
        self.shape = shape
        self.encoding = encoding
        self.attrs = attrs

    def to_dict(self):
        return dict(
            dims=self.dims,
            shape=self.shape,
            encoding=self.encoding.to_dict(),
            attrs=self.attrs,
        )


class DatasetMetadata:
    def __init__(
        self,
        dims: dict[str, int],
        variables: dict[str, VariableMetadata],
        attrs: dict[str, Any],
    ):
        self.dims = dims
        self.variables = variables
        self.attrs = attrs

    def to_dict(self):
        return dict(
            dims=self.dims,
            variables={k: v.to_dict() for k, v in self.variables.items()},
            attrs=self.attrs,
        )

    def assert_compatible_slice(
        self, slice_metadata: "DatasetMetadata", append_dim: str
    ):
        for dim_name, dim_size in self.dims.items():
            if dim_name not in slice_metadata.dims:
                raise ValueError(f"Missing dimension" f" {dim_name!r} in slice dataset")
            slice_dim_size = slice_metadata.dims[dim_name]
            if dim_name != append_dim and dim_size != slice_dim_size:
                raise ValueError(
                    f"Wrong size for dimension {dim_name!r}"
                    f" in slice dataset:"
                    f" expected {dim_size},"
                    f" but found {slice_dim_size}"
                )
        for var_name, var_metadata in self.variables.items():
            slice_var_metadata = slice_metadata.variables.get(var_name)
            if (
                slice_var_metadata is not None
                and var_metadata.dims != slice_var_metadata.dims
            ):
                raise ValueError(
                    f"Wrong dimensions for variable {var_name!r}"
                    f" in slice dataset:"
                    f" expected {var_metadata.dims},"
                    f" but found {slice_var_metadata.dims}"
                )

    @classmethod
    def from_dataset(cls, dataset: xr.Dataset, config: dict[str, Any] | None = None):
        config = config or {}

        dims = _get_effective_dims(
            dataset,
            config.get("fixed_dims"),
            config.get("append_dim") or DEFAULT_APPEND_DIM,
        )

        variables = _get_effective_variables(
            dataset,
            config.get("included_variables") or [],
            config.get("excluded_variables") or [],
            config.get("variables") or {},
        )

        attrs = merge_configs(dataset.attrs, config.get("attrs") or {})

        return DatasetMetadata(dims=dims, variables=variables, attrs=attrs)


def _get_effective_dims(
    dataset: xr.Dataset,
    config_fixed_dims: dict[str, int] | None,
    config_append_dim: str,
) -> dict[str, int]:
    if config_fixed_dims:
        if config_fixed_dims.get(config_append_dim) is not None:
            raise ValueError(
                f"Size of append dimension" f" {config_append_dim!r} must not be fixed"
            )
        for dim_name, fixed_dim_size in config_fixed_dims.items():
            if dim_name not in dataset.dims:
                raise ValueError(
                    f"Fixed dimension {dim_name!r}" f" not found in dataset"
                )
            ds_dim_size = dataset.dims[dim_name]
            if fixed_dim_size != ds_dim_size:
                raise ValueError(
                    f"Wrong size for fixed dimension {dim_name!r}"
                    f" in dataset: expected {fixed_dim_size},"
                    f" found {ds_dim_size}"
                )
    if config_append_dim not in dataset.dims:
        raise ValueError(
            f"Append dimension" f" {config_append_dim!r} not found in dataset"
        )

    return {str(k): v for k, v in dataset.dims.items()}


def _get_effective_variables(
    dataset: xr.Dataset,
    config_included_variables: list[str] | None,
    config_excluded_variables: list[str] | None,
    config_variables: dict[str, dict[str, Any]] | None,
) -> dict[str, VariableMetadata]:
    config_variables = dict(config_variables or {})
    # Get and remove defaults for all variables
    defaults = config_variables.pop("*", {})
    all_var_names = set(map(str, dataset.variables.keys())).union(
        set(config_variables.keys())
    )
    if config_included_variables:
        selected_var_names = set(config_included_variables)
    else:
        selected_var_names = set(all_var_names)
    if config_excluded_variables:
        selected_var_names -= set(config_excluded_variables)

    unknown_var_names = selected_var_names - all_var_names
    if unknown_var_names:
        raise ValueError(
            f"The following variables are neither configured"
            f" nor contained in the dataset:"
            f" {', '.join(sorted(unknown_var_names))}"
        )

    variables = {}

    for var_name in selected_var_names:
        # "*" is default for all variables
        config_var_def: dict = merge_configs(
            defaults, config_variables.get(var_name) or {}
        )
        ds_var = dataset.variables.get(var_name)
        if ds_var is not None:
            # Variable found in dataset: use dataset variable to complement
            # variable definition from configuration (if any)
            ds_var_def = dict(
                dims=tuple(map(str, ds_var.dims)),
                shape=ds_var.shape,
                encoding=dict(ds_var.encoding),
                attrs=dict(ds_var.attrs),
            )
            ds_var_dims = ds_var_def["dims"]
            config_var_dims = config_var_def.get("dims")
            if config_var_dims is not None:
                config_var_dims = tuple(config_var_dims)
                if config_var_dims != ds_var_dims:
                    raise ValueError(
                        f"Dimension mismatch for"
                        f" variable {var_name!r}:"
                        f" expected {config_var_dims},"
                        f" found {ds_var_dims} in dataset"
                    )
            config_var_def = merge_configs(ds_var_def, config_var_def)
        else:
            # Variable not found in dataset: make sure configuration
            # is complete to create new dataset variables later
            config_var_dims = config_var_def.get("dims")
            if config_var_dims is None:
                raise ValueError(f"Missing dimensions" f" of variable {var_name!r}")
            for dim in config_var_dims:
                if dim not in dataset.dims:
                    raise ValueError(
                        f"Dimension {dim!r} of variable"
                        f" {var_name!r} not found in dataset"
                    )
            config_var_def["shape"] = tuple(dataset.dims[k] for k in config_var_dims)
            encoding: dict | None = config_var_def.get("encoding")
            if encoding is None or encoding.get("dtype") is None:
                raise ValueError(
                    f"Missing 'dtype' in encoding configuration"
                    f" of variable {var_name!r}"
                )

        # Normalize encoding / attrs
        dims = tuple(config_var_def.get("dims"))
        shape = tuple(config_var_def.get("shape"))
        encoding = dict(config_var_def.get("encoding") or {})
        attrs = dict(config_var_def.get("attrs") or {})
        for prop_name, normalize_value in _ENCODING_PROPS.items():
            if prop_name in attrs:
                if prop_name not in encoding:
                    encoding[prop_name] = attrs.pop(prop_name)
            if prop_name in encoding:
                encoding[prop_name] = normalize_value(encoding[prop_name])
        if "_FillValue" in encoding:
            fill_value = encoding.pop("_FillValue")
            if encoding.get("fill_value") is None:
                encoding["fill_value"] = fill_value
        if "preferred_chunks" in encoding:
            encoding.pop("preferred_chunks")
        variables[var_name] = VariableMetadata(
            dims=dims, shape=shape, encoding=VariableEncoding(**encoding), attrs=attrs
        )

    return variables


def _normalize_dtype(value: Any) -> np.dtype:
    if isinstance(value, str):
        return np.dtype(value)
    assert isinstance(value, np.dtype)
    return value


def _normalize_chunks(value: Any) -> tuple[int, ...] | None:
    if not value:
        return None
    assert isinstance(value, (tuple, list))
    return tuple((v if isinstance(v, int) else v[0]) for v in value)


def _normalize_number(value: Any) -> int | float | None:
    if isinstance(value, str):
        return float(value)
    return value


def _normalize_compressor(value: Any) -> Codec | None:
    if not value:
        return None
    return _normalize_codec(value)


def _normalize_filters(value: Any) -> list[Codec] | None:
    if not value:
        return None
    return [_normalize_codec(f) for f in value]


def _normalize_codec(value: Any) -> Codec:
    if isinstance(value, dict):
        return numcodecs.get_codec(value)
    assert isinstance(value, Codec)
    return value


_ENCODING_PROPS: dict[str, Callable[[Any], Any]] = {
    "dtype": _normalize_dtype,
    "chunks": _normalize_chunks,
    "fill_value": _normalize_number,
    "_FillValue": _normalize_number,
    "scale_factor": _normalize_number,
    "add_offset": _normalize_number,
    "compressor": _normalize_compressor,
    "filters": _normalize_filters,
}
