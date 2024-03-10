# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from abc import abstractmethod, ABC

import xarray as xr


class SliceSource(ABC):
    """Slice source interface definition.

    A slice source is a disposable source for a slice dataset.

    A slice source object must implement both

    * [get_dataset()][zappend.slice.abc.SliceSource.get_dataset] and.
    * [dispose()][zappend.slice.abc.SliceSource.dispose]

    If your slice source class requires the processing context,
    your class constructor may define a `ctx: Context` as 1st positional
    argument or as keyword argument.
    """

    @abstractmethod
    def get_dataset(self) -> xr.Dataset:
        """Open this slice source, do some processing and return a dataset of type
        [xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html)
        as result.

        This method is not intended to be called directly.
        This method is called exactly once for each instance of this class.

        It should return a dataset that is compatible with
        target dataset:

        * slice must have same fixed dimensions;
        * append dimension must exist in slice.

        Returns:
            A slice dataset.
        """

    @abstractmethod
    def dispose(self):
        """Dispose this slice source.
        This should include cleaning up of any temporary resources.

        This method is not intended to be called directly
        and is called exactly once for each instance of this class.
        """
