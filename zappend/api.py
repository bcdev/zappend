# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import xarray as xr
from typing import Any, Iterable

from .config import ConfigLike
from .processor import Processor


def zappend(slices: Iterable[str | xr.Dataset], config: ConfigLike = None, **kwargs):
    """
    Create or update a Zarr dataset from dataset slices.

    :param slices: The slice datasets. An iterable that yields either
        ``str`` or ``xarray.Dataset`` objects. If ``str`` is used,
        it is interpreted as local dataset path or dataset URI.
        If a URI is used, protocol-specific parameters apply, given by
        configuration parameter ``slice_storage_options``.
    :param config: Processor configuration.
        May be a file path or URI, a ``dict``, ``None``, or a sequence of
        the aforementioned. If a sequence is used, subsequent configurations
        are incremental to the previous ones.
    :param kwargs: Additional configuration parameters.
        Can be used to pass or override configuration values in *config*.
    """
    processor = Processor(config, **kwargs)
    processor.process_slices(slices)
