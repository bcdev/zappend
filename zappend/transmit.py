# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Callable, Literal

import fsspec

FileFilter = Callable[
    [
        str,  # source path
        str,  # source filename
        bytes,  # source data
    ],
    tuple[str, bytes] | None  # (target filename, target data) | skip file
]

RollbackOp = (Literal["delete_dir"]
              | Literal["delete_file"]
              | Literal["replace_file"])

RollbackCallback = Callable[
    [
        RollbackOp,
        str,  # target path
        bytes | None  # original data, if operation is "replace_file"
    ],
    None  # void
]


def transmit(source_fs: fsspec.AbstractFileSystem,
             source_path: str,
             target_fs: fsspec.AbstractFileSystem,
             target_path: str,
             file_filter: FileFilter | None = None,
             rollback_cb: RollbackCallback | None = None):
    """Deeply copy *source_path* from filesystem *source_fs* to
    *target_path* in filesystem *target_fs*. Filter function *filter_file*,
    if given, is used to exclude source files and directories for which
    it returns a falsy value.
    """

    num_created = make_dirs(target_fs, target_path, rollback_cb=rollback_cb)
    if num_created:
        rollback_cb = None  # No need to notify for nested items

    # Check, if it more performant to use source_fs.walk().
    for source_file_info in source_fs.ls(source_path, detail=True):
        source_file_path: str = source_file_info["name"]
        source_file_type: str = source_file_info["type"]

        _, source_file_name = source_file_path.rsplit("/", maxsplit=1)
        source_file_path = f"{source_path}/{source_file_name}"

        if source_file_type == "directory":
            target_file_path = f"{target_path}/{source_file_name}"
            transmit(source_fs, source_file_path,
                     target_fs, target_file_path,
                     file_filter=file_filter,
                     rollback_cb=rollback_cb)
        elif source_file_type == "file":
            # Note, we could also consider reading/writing block-wise,
            # given that rollback_cb is None and source data
            # equals target data.
            # But our main use case is copying Zarr chunks
            # which usually fit into users' RAM to allow for
            # out-of-core computation.
            with source_fs.open(source_file_path, "rb") as sf:
                source_data = sf.read()
                target_file_name, target_data = _maybe_apply_file_filter(
                    file_filter,
                    source_path,
                    source_file_name,
                    source_data
                )
                if target_file_name and target_data is not None:
                    target_file_path = f"{target_path}/{target_file_name}"
                    target_exists = target_fs.exists(target_file_path)
                    original_data = None
                    if target_exists and rollback_cb is not None:
                        with target_fs.open(target_file_path, "rb") as tf:
                            original_data = tf.read()
                    with target_fs.open(target_file_path, "wb") as tf:
                        if target_exists:
                            _maybe_emit_rollback_op(
                                rollback_cb,
                                "replace_file",
                                target_file_path,
                                original_data
                            )
                        else:
                            _maybe_emit_rollback_op(
                                rollback_cb,
                                "delete_file",
                                target_file_path
                            )
                        tf.write(target_data)


def make_dirs(fs: fsspec.AbstractFileSystem,
              path: str,
              rollback_cb: RollbackCallback | None = None) -> int:
    num_created = 0
    _path = None
    for path_component in split_path(path):
        if _path is None:
            _path = path_component
        else:
            _path = f"{_path}/{path_component}"
        if not fs.exists(_path):
            fs.mkdir(_path)
            num_created += 1
            _maybe_emit_rollback_op(rollback_cb, "delete_dir", _path)
            # No need to notify for nested dirs
            rollback_cb = None
    return num_created


def split_path(path: str, sep: str = "/") -> list[str]:
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


def _maybe_apply_file_filter(
        file_filter: FileFilter | None,
        source_path: str,
        source_file_name: str,
        source_data: bytes
) -> tuple[str, bytes] | tuple[None, None]:
    if file_filter is not None:
        result = file_filter(source_path,
                             source_file_name,
                             source_data)
        if result:
            return result[0], result[1]
        else:
            return None, None
    else:
        return source_file_name, source_data


def _maybe_emit_rollback_op(callback: RollbackCallback | None,
                            op: RollbackOp,
                            path: str,
                            data: bytes | None = None):
    if callback is not None:
        callback(op, path, data)
