# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import contextlib
import warnings
from abc import abstractmethod, ABC
from typing import Callable, ContextManager, Type

import xarray as xr

from zappend.context import Context
from zappend.fsutil import FileObj


class SliceSource(ABC):
    """Slice source interface definition.

    A slice source is a closable source for a slice dataset.

    A slice source is intended to be implemented by users. An implementation
    must provide the methods [get_dataset()][zappend.api.SliceSource.get_dataset]
    and [close()][zappend.api.SliceSource.close].

    If your slice source class requires the processing context,
    your class constructor may define a `ctx: Context` as 1st positional
    argument or as keyword argument.
    """

    @abstractmethod
    def get_dataset(self) -> xr.Dataset:
        """Open this slice source, do some processing and return a dataset of type
        [xarray.Dataset](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html)
        as result.

        This method is not intended to be called directly and is called exactly
        once for each instance of this class.

        It should return a dataset that is compatible with
        target dataset:

        * slice must have same fixed dimensions;
        * append dimension must exist in slice.

        Returns:
            A slice dataset.
        """

    def close(self):
        """Close this slice source.
        This should include cleaning up of any temporary resources.

        This method is not intended to be called directly
        and is called exactly once for each instance of this class.
        """
        if hasattr(self, "dispose"):
            warnings.warn(
                "The dispose() method of SliceSource has been"
                " deprecated since zappend 0.6.0,"
                " please override close() instead.",
                category=DeprecationWarning,
            )
            self.dispose()

    def dispose(self):
        """Deprecated since version 0.6.0, override
        [close()][zappend.api.SliceSource.close] instead.
        """


SliceItem = str | FileObj | xr.Dataset | ContextManager[xr.Dataset] | SliceSource
"""The possible types that can represent a slice dataset."""

SliceCallable = Type[SliceSource] | Callable[[...], SliceItem]
"""This type is either a class derived from `SliceSource` or a function that 
returns a `SliceItem`. Both can be invoked with any number of positional or 
keyword arguments. The processing context, if used, must be named `ctx` and 
must be either the 1st positional argument or a keyword argument. Its type 
is `Context`.
"""


def to_slice_source(
    ctx: Context,
    slice_item: SliceItem,
    slice_index: int,
) -> SliceSource | ContextManager[xr.Dataset]:
    # prevent cyclic import
    from .sources import MemorySliceSource
    from .sources import PersistentSliceSource
    from .sources import TemporarySliceSource
    from .callable import invoke_slice_callable

    slice_callable = ctx.config.slice_source
    if slice_callable is not None:
        slice_item = invoke_slice_callable(slice_callable, slice_item, ctx)

    if isinstance(slice_item, SliceSource):
        return slice_item
    if isinstance(slice_item, str):
        slice_file = FileObj(
            slice_item, storage_options=ctx.config.slice_storage_options
        )
        return PersistentSliceSource(ctx, slice_file)
    if isinstance(slice_item, FileObj):
        return PersistentSliceSource(ctx, slice_item)
    if isinstance(slice_item, xr.Dataset):
        if ctx.config.persist_mem_slices:
            return TemporarySliceSource(ctx, slice_item, slice_index)
        else:
            return MemorySliceSource(slice_item, slice_index)
    if isinstance(slice_item, contextlib.AbstractContextManager):
        return slice_item
    raise TypeError(
        f"slice_item must have type"
        f" str,"
        f" xarray.Dataset,"
        f" contextlib.AbstractContextManager,"
        f" zappend.api.FileObj,"
        f" zappend.api.SliceSource,"
        f" but was type {type(slice_item).__name__}"
    )
