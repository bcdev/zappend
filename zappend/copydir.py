# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Callable

import fsspec

FilterFile = Callable[
    [
        str,  # source dir path
        str,  # target dir path
        str,  # source file/directory name
        bool,  # source is directory?
    ],
    str | None  # target file name | skip file?
]


def copy_dir(source_fs: fsspec.AbstractFileSystem,
             source_path: str,
             target_fs: fsspec.AbstractFileSystem,
             target_path: str,
             filter_file: FilterFile | None = None):
    """Deeply copy *source_path* from filesystem *source_fs* to
    *target_path* in filesystem *target_fs*.
    """
    if filter_file is None \
            and source_fs.fsid == target_fs.fsid:
        source_fs.copy(source_path, target_path, recursive=True)
        return

    if not target_fs.exists(target_path):
        target_fs.mkdirs(target_path, exist_ok=True)

    for source_file_info in source_fs.ls(source_path, detail=True):
        source_file_name = source_file_info["name"]
        source_file_type = source_file_info["type"]

        if filter_file is not None:
            target_file_name = filter_file(source_path,
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
            target_fs.mkdir(target_file_path)
            copy_dir(source_fs, source_file_path,
                     target_fs, target_file_path,
                     filter_file=filter_file)
        elif source_file_type == "file":
            if target_file_name is not None:
                with source_fs.open(source_file_path, "rb") as sf:
                    with target_fs.open(target_file_path, "wb") as tf:
                        # TODO: read/write block-wise
                        tf.write(sf.read())
