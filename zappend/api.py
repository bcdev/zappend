# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Iterable, Any

from .config import ConfigItem
from .config import ConfigLike
from .config import ConfigList
from .context import Context
from .fsutil import FileObj
from .processor import Processor
from .slice import SliceFactory
from .slice import SliceObj
from .slice import SliceSource
from .slice import to_slice_factories
from .slice import to_slice_factory


__all__ = [
    "ConfigItem",
    "ConfigLike",
    "ConfigList",
    "Context",
    "FileObj",
    "SliceSource",
    "SliceFactory",
    "SliceObj",
    "zappend",
    "to_slice_factories",
    "to_slice_factory",
]


def zappend(
    slices: Iterable[SliceObj | SliceFactory], config: ConfigLike = None, **kwargs: Any
):
    """
    Robustly create or update a Zarr dataset from dataset slices.

    Args:
        slices: An iterable that yields slice objects. A slice object is
            either a ``str``, ``xarray.Dataset``, ``SliceSource`` or a factory
            function that returns a slice object. If ``str`` is used,
            it is interpreted as local dataset path or dataset URI.
            If a URI is used, protocol-specific parameters apply, given by
            configuration parameter ``slice_storage_options``.
        config: Processor configuration.
            May be a file path or URI, a ``dict``, ``None``, or a sequence of
            the aforementioned. If a sequence is used, subsequent configurations
            are incremental to the previous ones.
        kwargs: Additional configuration parameters.
            Can be used to pass or override configuration values in *config*.
    """
    processor = Processor(config, **kwargs)
    processor.process_slices(slices)
