# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
from functools import cached_property
from typing import Iterator, Any, Dict

import fsspec


class Context:

    def __init__(self, target_path: str, config: Dict[str, Any]):
        self._config = config
        target_fs_options = config.get("target_fs_options", {})
        self._target_fs, self._target_path = fsspec.core.url_to_fs(
            target_path,
            **target_fs_options
        )

    @property
    def target_fs(self) -> fsspec.AbstractFileSystem:
        return self._target_path

    @property
    def target_path(self) -> str:
        return self._target_path

    def get_slice_fs(self, slice_path: str):
        slice_fs_options = self._config.get("slice_fs_options", {})
        return fsspec.core.url_to_fs(
            slice_path,
            **slice_fs_options
        )
