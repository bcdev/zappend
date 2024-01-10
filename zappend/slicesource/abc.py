# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from abc import abstractmethod, ABC

import xarray as xr

from ..context import Context


class SliceSource(ABC):
    """Provides a slice dataset from different sources.
    Instances of this class are supposed to be used as context managers.
    The context manager provides the dataset instance.

    :param ctx: The processing context
    """

    def __init__(self, ctx: Context):
        self._ctx = ctx

    def __del__(self):
        """Overridden to call ``close()``."""
        self.close()

    def __enter__(self) -> xr.Dataset:
        return self.open()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @abstractmethod
    def open(self) -> xr.Dataset:
        """Open this slice source and return the dataset instance.

        It should return a dataset that is compatible with
        target dataset:

        * append dimension must exist in slice
        * slice must have same fixed dimensions

        :return: a slice dataset.
        """

    def close(self):
        """Dispose this slice Zarr.
        This should include cleaning up of used resources.
        """
        self._ctx = None
