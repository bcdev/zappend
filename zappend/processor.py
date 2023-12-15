# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Iterator

from .context import Context


class Processor:
    def __init__(self, ctx: Context):
        self._ctx = ctx

    def process_slice(self, slice_path: str):
        target_fs = self._ctx.target_fs
        if not target_fs.exists(self._ctx.target_path):
            self.create_target(slice_path)
        else:
            self.open_target()
            self.append_slice(slice_path)

    def process_slices(self,
                       slice_iter: Iterator[str]):
        for slice_path in slice_iter:
            self.process_slice(slice_path)

    def create_target(self, slice_path):
        pass

    def open_target(self):
        pass

    def append_slice(self, slice_path):
        pass
