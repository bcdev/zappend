# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from abc import abstractmethod, ABC

import fsspec

from ..context import Context


class SliceZarr(ABC):
    """Provides a slice dataset in Zarr format compatible
    with a given target layout in `ctx.target_layout`.

    :param ctx: The processing context
    """

    def __init__(self, ctx: Context):
        self.ctx = ctx

    def __del__(self):
        """Overridden to call ``dispose()``."""
        self.dispose()

    def __enter__(self):
        return self.prepare()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()

    @abstractmethod
    def prepare(self) -> tuple[fsspec.AbstractFileSystem, str]:
        """Prepare this slice Zarr so it can be used.

        :return: a tuple (filesystem, path) that are used to
            access the Zarr.
        """

    def dispose(self):
        """Dispose this slice Zarr.
        This should include cleaning up of used resources.
        """
        if hasattr(self, "ctx"):
            self.ctx = None
            del self.ctx
