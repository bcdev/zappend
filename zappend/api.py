# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Any, Iterable

from .config import Config
from .config import ConfigItem
from .config import ConfigLike
from .config import ConfigList
from .context import Context
from .fsutil import FileObj
from .processor import Processor
from .slice import SliceCallable
from .slice import SliceItem
from .slice import SliceSource


__all__ = [
    "Config",
    "ConfigItem",
    "ConfigLike",
    "ConfigList",
    "Context",
    "FileObj",
    "SliceCallable",
    "SliceItem",
    "SliceSource",
    "zappend",
]


def zappend(
    slices: Iterable[Any],
    config: ConfigLike = None,
    **kwargs: Any,
) -> int:
    """Robustly create or update a Zarr dataset from dataset slices.

    The `zappend` function concatenates the dataset slices from given
    `slices` along a given append dimension, e.g., `"time"` (the default)
    for geospatial satellite observations.
    Each append step is atomic, that is, the append operation is a transaction
    that can be rolled back, in case the append operation fails.
    This ensures integrity of the  target data cube `target_dir` given
    in `config` or `kwargs`.

    Each slice item in `slices` provides a slice dataset to be appended.
    The interpretation of a given slice item depends on whether a slice source
    is configured or not (setting `slice_source`).

    If no slice source is configured, a slice item must be an object of type
    `str`, `FileObj`, `xarray.Dataset`, or `SliceSource`.
    If `str` or `FileObj` are used, they are interpreted as local dataset path or
    dataset URI. If a URI is used, protocol-specific parameters apply, given by the
    configuration parameter `slice_storage_options`.

    If a slice source is configured, a slice item represents the argument(s) passed
    to that slice source. Multiple positional arguments can be passed as `list`,
    multiple keyword arguments as `dict`, and both as a `tuple` of `list` and `dict`.

    Args:
        slices: An iterable that yields slice items.
        config: Processor configuration.
            Can be a file path or URI, a `dict`, `None`, or a sequence of
            the aforementioned. If a sequence is used, subsequent
            configurations are incremental to the previous ones.
        kwargs: Additional configuration parameters.
            Can be used to pass or override configuration values in *config*.

    Returns:
        The number of slices processed. The value can be useful if \
        the number of items in `slices` is unknown.

    """
    processor = Processor(config, **kwargs)
    return processor.process_slices(slices)
