# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Callable, Literal

import fsspec

from .fileobj import FileObj
from .path import split_components, split_filename

FileFilter = Callable[
    [
        str,  # source path
        str,  # source filename
        bytes,  # source data
    ],
    tuple[str, bytes] | None  # (target filename, target data) | skip file
]

RollbackAction = (Literal["delete_dir"]
                  | Literal["delete_file"]
                  | Literal["replace_file"])

RollbackCallback = Callable[
    [
        RollbackAction,
        # TODO: Check if we should use FileObj instead of path
        str,  # target path
        bytes | None  # original data, if operation is "replace_file"
    ],
    None  # void
]


def transmit(source_dir: FileObj,
             target_dir: FileObj,
             file_filter: FileFilter | None = None,
             rollback_cb: RollbackCallback | None = None):
    """Deeply transmit *source_dir* to *target_dir*.
    A filter function *filter_file* can be provided to
    transform a source file. If it returns a falsy value,
    the file will not be transmitted at all.
    TODO: Wait. What about a target file exists and the filter function
        returns a falsy value. Should it be deleted then?
    """
    _transmit(source_dir.fs,
              source_dir.path,
              target_dir.fs,
              target_dir.path,
              file_filter=file_filter,
              rollback_cb=rollback_cb)


def _transmit(source_fs: fsspec.AbstractFileSystem,
              source_path: str,
              target_fs: fsspec.AbstractFileSystem,
              target_path: str,
              file_filter: FileFilter | None = None,
              rollback_cb: RollbackCallback | None = None):
    num_created = _make_dirs(target_fs,
                             target_path,
                             rollback_cb=rollback_cb)
    if num_created:
        rollback_cb = None  # No need to notify for nested items

    # Check, if it more performant to use source_fs.walk().
    for source_file_info in source_fs.ls(source_path, detail=True):
        source_file_path: str = source_file_info["name"]
        source_file_type: str = source_file_info["type"]

        _, source_file_name = split_filename(source_file_path)
        source_file_path = f"{source_path}/{source_file_name}"

        if source_file_type == "directory":
            target_file_path = f"{target_path}/{source_file_name}"
            _transmit(source_fs, source_file_path,
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


def make_dirs(target_dir: FileObj,
              rollback_cb: RollbackCallback | None = None) -> int:
    return _make_dirs(target_dir.fs,
                      target_dir.path,
                      rollback_cb=rollback_cb)


def _make_dirs(fs: fsspec.AbstractFileSystem,
               path: str,
               rollback_cb: RollbackCallback | None = None) -> int:
    num_created = 0
    _path = None
    for path_component in split_components(path):
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
                            action: RollbackAction,
                            path: str,
                            data: bytes | None = None):
    if callback is not None:
        callback(action, path, data)
