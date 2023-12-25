# Copyright © 2023 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import xarray as xr

from ..context import Context
from ..outline import DatasetOutline
from ..outline import assert_compliance
from .abc import SliceSource


class IdentitySliceSource(SliceSource):
    """A slice source that returns the dataset passed in when opened.

    :param ctx: Processing context
    :param slice_ds: The dataset
    """

    def __init__(self, ctx: Context, slice_ds: xr.Dataset):
        super().__init__(ctx)
        self._slice_ds = slice_ds

    def open(self) -> xr.Dataset:
        slice_outline = DatasetOutline.from_dataset(self._slice_ds)
        assert_compliance(self._ctx.target_outline, slice_outline)
        return self._slice_ds

    def close(self):
        self._slice_ds = None
        super().close()
