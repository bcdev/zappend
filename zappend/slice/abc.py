# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from abc import abstractmethod, ABC

import xarray as xr

from ..context import Context


class SliceSource(ABC):
    """Represents a source for a slice dataset.
    Instances of this class are supposed to be used as context managers.
    The context manager provides the dataset instance by calling the
    [get_dataset()][zappend.slice.abc.SliceSource.get_dataset] method.
    When the context manager exits, the
    [dispose()][zappend.slice.abc.SliceSource.dispose] method
    is called.

    You may implement your own slice source class and define a
    [slice source factory][zappend.slice.common.SliceFactory] function
    that creates instances of your slice source. Such functions can
    be passed input to the [zappend()](zappend.api.zappend) function, usually
    in the form of a
    [closure](https://en.wikipedia.org/wiki/Closure_(computer_programming)) to
    capture slice-specific information.

    Args:
        ctx: The processing context.
    """

    def __init__(self, ctx: Context):
        self._ctx = ctx

    @property
    def ctx(self) -> Context:
        """The processing context passed to the constructor."""
        return self._ctx

    def __enter__(self) -> xr.Dataset:
        return self.get_dataset()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()

    @abstractmethod
    def get_dataset(self) -> xr.Dataset:
        """Open this slice source and return the dataset instance.

        This method is not intended to be called directly.
        Instead, instances of this class are context managers and
        should be used as such.

        It should return a dataset that is compatible with
        target dataset:

        * slice must have same fixed dimensions;
        * append dimension must exist in slice.

        Returns:
            A slice dataset.
        """

    def dispose(self):
        """Dispose this slice source.
        This should include cleaning up of used resources.

        This method is not intended to be called directly.
        Instead, instances of this class are context managers and
        should be used as such.
        """
        self._ctx = None