# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.


def split_parent(path: str, sep: str = "/") -> tuple[str, str]:
    if not path:
        raise ValueError("cannot get parent of empty path")
    comps = path.rsplit(sep, maxsplit=1)
    if len(comps) == 1:
        return "", comps[0]
    parent, rest = comps
    return parent or "/", rest


def split_components(path: str, sep: str = "/") -> list[str]:
    leading_sep = path.startswith(sep)
    if leading_sep:
        path = path[1:]

    trailing_sep = path.endswith(sep)
    if trailing_sep:
        path = path[:-1]

    path_components = path.split(sep)

    if leading_sep:
        path_components[0] = sep + path_components[0]
    if trailing_sep:
        path_components[-1] = path_components[-1] + sep

    return path_components
