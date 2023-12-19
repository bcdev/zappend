# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Callable, Literal

import fsspec

FileFilter = Callable[
    [
        str,  # source dir path
        str,  # target dir path
        str,  # source file/directory name
        bool,  # source is directory?
    ],
    str | None  # old/new target file name | skip file?
]

FileOp = (Literal["create_dir"]
          | Literal["create_file"]
          | Literal["replace_file"])

FileOpCallback = Callable[
    [
        FileOp,  # file operation
        str,  # target file or dir path
    ],
    None
]


def copy_dir(source_fs: fsspec.AbstractFileSystem,
             source_path: str,
             target_fs: fsspec.AbstractFileSystem,
             target_path: str,
             file_filter: FileFilter | None = None,
             file_op_cb: FileOpCallback | None = None):
    """Deeply copy *source_path* from filesystem *source_fs* to
    *target_path* in filesystem *target_fs*. Filter function *filter_file*,
    if given, is used to exclude source files and directories for which
    it returns a falsy value.
    """

    num_created = make_dirs(target_fs, target_path, file_op_cb=file_op_cb)
    if num_created:
        file_op_cb = None  # No need to notify for nested items

    for source_file_info in source_fs.ls(source_path, detail=True):
        source_file_path: str = source_file_info["name"]
        source_file_type: str = source_file_info["type"]

        _, source_file_name = source_file_path.rsplit("/", maxsplit=1)

        if file_filter is not None:
            target_file_name = file_filter(source_path,
                                           target_path,
                                           source_file_name,
                                           source_file_type == "directory")
            if not target_file_name:
                continue
        else:
            target_file_name = source_file_name

        source_file_path = f"{source_path}/{source_file_name}"
        target_file_path = f"{target_path}/{target_file_name}"

        if source_file_type == "directory":
            copy_dir(source_fs, source_file_path,
                     target_fs, target_file_path,
                     file_filter=file_filter,
                     file_op_cb=file_op_cb)
        elif source_file_type == "file":
            target_exists = target_fs.exists(target_file_path)
            with source_fs.open(source_file_path, "rb") as sf:
                with target_fs.open(target_file_path, "wb") as tf:
                    if target_exists:
                        _maybe_notify(file_op_cb, "replace_file",
                                      target_file_path)
                    else:
                        _maybe_notify(file_op_cb, "create_file",
                                      target_file_path)
                    # TODO: read/write block-wise
                    tf.write(sf.read())


def make_dirs(fs: fsspec.AbstractFileSystem,
              path: str,
              file_op_cb: FileOpCallback | None = None) -> int:
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
            _maybe_notify(file_op_cb, "create_dir", _path)
            # No need to notify for nested dirs
            file_op_cb = None
    return num_created


def split_path(path: str) -> list[str]:
    leading_sep = path.startswith("/")
    if leading_sep:
        path = path[1:]

    trailing_sep = path.endswith("/")
    if trailing_sep:
        path = path[:-1]

    path_components = path.split("/")

    if leading_sep:
        path_components[0] = "/" + path_components[0]
    if trailing_sep:
        path_components[-1] = path_components[-1] + "/"

    return path_components


def _maybe_notify(callback: FileOpCallback | None,
                  op: FileOp,
                  path: str):
    if callback is not None:
        callback(op, path)
