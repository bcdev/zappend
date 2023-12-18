# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from abc import abstractmethod, ABC

from ..context import Context
from ..fileobj import FileObj


class SliceZarr(ABC):
    """Provides a slice dataset in Zarr format compatible
    with a given target layout in `ctx.target_layout`.

    :param ctx: The processing context
    """

    def __init__(self, ctx: Context):
        self._ctx = ctx

    def __del__(self):
        """Overridden to call ``dispose()``."""
        self.dispose()

    def __enter__(self) -> FileObj:
        return self.prepare()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()

    @abstractmethod
    def prepare(self) -> FileObj:
        """Prepare this slice Zarr so it can be used.

        Returns a file object that points to slice Zarr dataset
        whose chunk files can be used to update the target dataset.

        An implementation must ensure that the returned file object
        points to a slice Zarr dataset that is fully compatible with
        target dataset outline:

        * slice must have same fixed dimensions
        * append dimension must exist in slice
        * slice variable outlines are equal to target variable outlines
          which includes strict equality of the following properties:
          dtype, dims, shape, chunks, fill_value,
          scale_factor, add_offset, compressor, filters.

        :return: a file object that can be safely used to
            update the target dataset.
        """

    def dispose(self):
        """Dispose this slice Zarr.
        This should include cleaning up of used resources.
        """
        if hasattr(self, "ctx"):
            self._ctx = None
            del self._ctx
