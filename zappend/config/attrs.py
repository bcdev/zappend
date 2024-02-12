# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import datetime
from typing import Any

import numpy as np


def eval_attrs(attrs: dict[str, Any], env: dict[str, Any]):
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
    if isinstance(value, datetime.datetime):
        return value.replace(microsecond=0).isoformat()
    try:
        if np.ndim(value) == 0:
            if np.issubdtype(value, int):
                return int(value)
            if np.issubdtype(value, float):
                return float(value)
            if np.issubdtype(value, np.datetime64):
                return np.datetime_as_string(value, unit="s")
    except (TypeError, ValueError):
        pass
    return value
