# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import contextlib

import xarray as xr

from .abc import SliceSource


class SliceSourceContextManager(contextlib.AbstractContextManager):
    """A context manager that wraps a slice source.

    Args:
        slice_source: The slice source.
    """

    def __init__(self, slice_source: SliceSource):
        self._slice_source = slice_source

    @property
    def slice_source(self) -> SliceSource:
        return self._slice_source

    def __enter__(self) -> xr.Dataset:
        return self._slice_source.get_dataset()

    def __exit__(self, *exception_args):
        self._slice_source.dispose()
        self._slice_source = None
