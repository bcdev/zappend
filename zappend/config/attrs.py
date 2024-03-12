# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import datetime
import math
from typing import Any, Literal

import numpy as np
import xarray as xr


def has_dyn_config_attrs(attrs: dict[str, Any]) -> bool:
    """Check if given configuration `attrs` contain dynamically computed values.
    Note, the check is currently performed only on top level attributes.

    Args:
        attrs: The dictionary of attributes from configuration.
    Returns:
         `True`, if so.
    """
    return any(isinstance(v, str) and "{{" in v and "}}" in v for v in attrs.values())


def eval_dyn_config_attrs(attrs: dict[str, Any], env: dict[str, Any]):
    """Replace any expression found in string-valued configuration attributes
    `attrs` by dynamically computed values.
    Note, the replacement is currently performed only on top level attributes.

    WARNING: This method uses Python's evil `eval()` function. The caller is
    responsible for avoiding that `env` causes a potential vulnerability
    and prohibits insecure operations!

    Args:
        attrs: The dictionary of attributes from configuration.
        env: The environment used for the evaluation.
    Returns:
         `True`, if so.
    """
    return {k: eval_attr_value(v, env) for k, v in attrs.items()}


def eval_attr_value(attr_value: Any, env: dict[str, Any]) -> Any:
    if isinstance(attr_value, str):
        parts = attr_value.split("{{")
        if len(parts) > 0:
            values = []
            for part in parts[1:]:
                sub_parts = part.split("}}")
                if len(sub_parts) == 2:
                    expr, rest = sub_parts
                    value = eval_expr(expr, env)
                    if rest:
                        value = str(value) + rest
                    values.append(value)
            if values:
                if parts[0]:
                    values[0] = parts[0] + str(values[0])
                if len(values) == 1:
                    return values[0]
                else:
                    return "".join(map(str, values))
    return attr_value


def eval_expr(expr: str, env: dict[str, Any]) -> Any:
    value = eval(expr, env)
    return to_json(value)


def to_json(value) -> Any:
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        else:
            return str(value)
    if isinstance(value, (bool, int, str, type(None))):
        return value
    if isinstance(value, datetime.date):
        if isinstance(value, datetime.datetime):
            return value.replace(microsecond=0).isoformat()
        else:
            return value.isoformat()

    try:
        if value.ndim == 0:
            try:
                # xarray.DataArray case
                value = value.values
            except AttributeError:
                pass
            if np.issubdtype(value.dtype, np.floating):
                if np.isfinite(value):
                    return float(value)
                else:
                    return str(value)
            if np.issubdtype(value.dtype, np.bool_):
                return bool(value)
            if np.issubdtype(value.dtype, np.str_):
                return str(value)
            if np.issubdtype(value.dtype, np.integer):
                return int(value)
            if np.issubdtype(value.dtype, np.datetime64):
                return np.datetime_as_string(value, unit="s")
            raise ValueError(
                f"cannot serialize 0-d array of type {type(value)}, dtype={value.dtype!r}"
            )
    except AttributeError:
        pass

    if isinstance(value, dict):
        return {k: to_json(v) for k, v in value.items()}

    try:
        values = iter(value)
    except TypeError:
        raise ValueError(f"cannot serialize value of type {type(value)}")

    return [to_json(v) for v in values]


def get_dyn_config_attrs_env(ds: xr.Dataset, **kwargs):
    return dict(
        ds=ds,
        **{
            k: v
            for k, v in ConfigAttrsUserFunctions.__dict__.items()
            if not k.startswith("_")
        },
        **kwargs,
    )


_CellRef = Literal["lower"] | Literal["center"] | Literal["upper"]


class ConfigAttrsUserFunctions:
    """User functions that can be used within attribute expressions."""

    @staticmethod
    def lower_bound(array: xr.DataArray | np.ndarray, ref: _CellRef = "lower"):
        """Get the lower bound of one-dimensional `array`.

        Args:
            array: numpy ndarray-like array
            ref: cell reference
                 - `"lower"` the cell lower bound
                 - `"upper"` the cell upper bound
                 - `"center"` the cell center
        Return:
            The lower bound of `array`.
        """
        return _bounds(array, ref)[0]

    @staticmethod
    def upper_bound(array: xr.DataArray | np.ndarray, ref: _CellRef = "lower"):
        """Get the upper bound of one-dimensional `array`.

        Args:
            array: numpy ndarray-like array
            ref: cell reference
                 - `"lower"` the cell lower bound
                 - `"upper"` the cell upper bound
                 - `"center"` the cell center
        Return:
            The lower bound of `array`.
        """
        return _bounds(array, ref)[1]


def _bounds(array: xr.DataArray | np.ndarray, ref: _CellRef):
    if len(array.shape) != 1:
        raise ValueError(f"array must be 1-dimensional, got shape {array.shape}")
    if array.shape[0] == 0:
        raise ValueError(f"array must not be empty")
    v1, v2 = array[0], array[-1]
    delta = array[1] - v1 if array.size > 1 else 0
    if v1 > v2:
        v1, v2 = v2, v1
        delta = -delta
    if ref == "lower":
        return v1, v2 + delta
    elif ref == "upper":
        return v1 - delta, v2
    else:
        return v1 - delta / 2, v2 + delta / 2
