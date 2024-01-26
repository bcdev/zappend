# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.
from cProfile import Profile
from io import StringIO
from pstats import Stats
from typing import Iterable, Any, Literal

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
from .log import logger


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

_TOTAL_TIME: Literal["tottime"] = "tottime"


def zappend(
    slices: Iterable[SliceObj | SliceFactory],
    config: ConfigLike = None,
    profiling=None,
    **kwargs: Any,
):
    """
    Robustly create or update a Zarr dataset from dataset slices.

    The `zappend` function concatenates the dataset slices from given `slices` along a
    given append dimension, e.g., `"time"` (the default) for geospatial satellite
    observations.
    Each append step is atomic, that is, the append operation is a transaction
    that can be rolled back, in case the append operation fails.
    This ensures integrity of the  target data cube `target_dir` given in `config`
    or `kwargs`.

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
        profiling: Path for profiling output.
            Switches on profiling
        kwargs: Additional configuration parameters.
            Can be used to pass or override configuration values in *config*.
    """
    processor = Processor(config, **kwargs)

    if profiling:
        profile = Profile()
        logger.info(f"profiling ...")
        profile.enable()

    processor.process_slices(slices)

    if profiling:
        profile.disable()
        results = StringIO()
        stats = Stats(profile, stream=results)
        stats.sort_stats(_TOTAL_TIME).print_stats()
        with open(profiling, "w") as f:
            f.write(results.getvalue())
        logger.info(f"profiling output written to {profiling}")
