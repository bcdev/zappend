# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import tempfile
from typing import Iterator, Any, Dict

from .config import DEFAULT_SLICE_POLLING_INTERVAL
from .config import DEFAULT_SLICE_POLLING_TIMEOUT

import fsspec


class Context:

    def __init__(self, target_path: str, config: Dict[str, Any]):
        self._config = config

        target_fs_options = config.get("target_fs_options", {})
        self._target_fs, self._target_path = fsspec.core.url_to_fs(
            target_path,
            **target_fs_options
        )

        temp_path = config.get("temp_path", tempfile.gettempdir())
        temp_fs_options = config.get("temp_fs_options", {})
        self._temp_fs, self._temp_path = fsspec.core.url_to_fs(
            temp_path,
            **temp_fs_options
        )

    @property
    def config(self) -> dict[str, Any]:
        return self._config

    @property
    def target_fs(self) -> fsspec.AbstractFileSystem:
        return self._target_path

    @property
    def target_path(self) -> str:
        return self._target_path

    @property
    def slice_fs_options(self) -> dict[str, Any]:
        return self._config.get("slice_fs_options", {})

    def get_slice_fs(self, slice_path: str) \
            -> tuple[fsspec.AbstractFileSystem, str]:
        return fsspec.core.url_to_fs(
            slice_path,
            **self.slice_fs_options
        )

    @property
    def slice_polling(self) -> tuple[float, float]:
        slice_polling = self.config.get("slice_polling", False)
        if slice_polling is False:
            return 0, 0
        if slice_polling is True:
            slice_polling = {}
        return (
            slice_polling.get("interval", DEFAULT_SLICE_POLLING_INTERVAL),
            slice_polling.get("timeout", DEFAULT_SLICE_POLLING_TIMEOUT)
        )

    @property
    def temp_fs(self) -> fsspec.AbstractFileSystem:
        return self._temp_fs

    @property
    def temp_path(self) -> str:
        return self._temp_path
