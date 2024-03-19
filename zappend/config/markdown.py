# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
from typing import Any


def schema_to_markdown(schema: dict[str, Any]) -> str:
    lines = []
    _schema_to_md(schema, [], lines)
    return "\n".join(lines)


def _schema_to_md(
    schema: dict[str, Any],
    path: list[str],
    lines: list[str],
    sequence_name: str | None = None,
):
    undefined = object()
    is_root = len(path) == 0

    _type = schema.get("type")
    if _type and not is_root:
        if isinstance(_type, str):
            _type = [_type]
        value = " | ".join([f"_{name}_" for name in _type])
        if sequence_name:
            lines.append(f"The {sequence_name} are of type {value}.")
        else:
            lines.append(f"Type {value}.")

    title = schema.get("title")
    if title:
        prefix = "# " if is_root else ""
        lines.append(prefix + title)
        lines.append("")

    description = schema.get("description")
    if description:
        lines.append(description)

    one_of = schema.get("anyOf") or schema.get("oneOf")
    if one_of:
        if sequence_name:
            lines.append(f"The {sequence_name} must be one of the following:")
        else:
            lines.append("Must be one of the following:")
        for sub_schema in one_of:
            sub_lines = []
            _schema_to_md(sub_schema, path, sub_lines)
            if sub_lines:
                lines.append("")
                lines.append("  * " + sub_lines[0])
                for sub_line in sub_lines[1:]:
                    lines.append("    " + sub_line)
        lines.append("")

    const = schema.get("const", undefined)
    if const is not undefined:
        value = json.dumps(const)
        lines.append(f"Its value is `{value}`.")

    default = schema.get("default", undefined)
    if default is not undefined:
        value = json.dumps(default)
        lines.append(f"Defaults to `{value}`.")

    enum = schema.get("enum")
    if enum:
        values = ", ".join([json.dumps(v) for v in enum])
        lines.append(f"Must be one of `{values}`.")

    required = schema.get("required")
    if required:
        names = [f"`{name}`" for name in required]
        if len(names) > 1:
            lines.append(f"The keys {', '.join(names)} are required.")
        else:
            lines.append(f"The key {names[0]} is required.")

    properties = schema.get("properties")
    if properties:
        for name, property_schema in properties.items():
            if is_root:
                lines.append("")
                lines.append(f"## `{name}`")
                lines.append("")
                _schema_to_md(property_schema, path + [name], lines)
            else:
                lines.append("")
                lines.append(f"  * `{name}`:")
                sub_lines = []
                _schema_to_md(property_schema, path + [name], sub_lines)
                for sub_line in sub_lines:
                    lines.append("    " + sub_line)

    additional_properties = schema.get("additionalProperties")
    if isinstance(additional_properties, dict):
        _schema_to_md(
            additional_properties,
            path,
            lines,
            sequence_name="object's values",
        )

    items = schema.get("items")
    if isinstance(items, dict):
        _schema_to_md(
            items,
            path,
            lines,
            sequence_name="items of the array",
        )
