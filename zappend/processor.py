# Copyright © 2024 Norman Fomferra
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from typing import Iterable

import xarray as xr

from .config import ConfigLike
from .config import normalize_config
from .config import validate_config
from .context import Context
from .fsutil.transaction import Transaction
from .fsutil.transaction import RollbackCallback
from .log import logger
from .log import configure_logging
from .rollbackstore import RollbackStore
from .slice import SliceObj
from .slice import get_slice_dataset
from .tailoring import tailor_target_dataset
from .tailoring import tailor_slice_dataset


class Processor:
    """This class implements the actual `zappend` process.

    Args:
        config: Processor configuration.
            May be a file path or URI, a ``dict``, ``None``, or a sequence of
            the aforementioned. If a sequence is used, subsequent configurations
            are incremental to the previous ones.
        kwargs: Additional configuration parameters.
            Can be used to pass or override configuration values in *config*.
    """

    def __init__(self, config: ConfigLike = None, **kwargs):
        config = normalize_config(config)
        config.update({k: v for k, v in kwargs.items() if v is not None})
        validate_config(config)
        configure_logging(config.get("logging"))
        self._config = config

    def process_slices(self, slices: Iterable[SliceObj]):
        """Process the given *slices*.
        Passes each slice in *slices* to the ``process_slice()`` method.

        Args:
            slices: Slice objects.
        """
        for slice_index, slice_obj in enumerate(slices):
            self.process_slice(slice_obj, slice_index=slice_index)

    def process_slice(self, slice_obj: SliceObj, slice_index: int = 0):
        """Process a single slice object *slice_obj*.

        A slice object is
        either a ``str``, ``xarray.Dataset``, ``SliceSource`` or a factory
        function that returns a slice object. If ``str`` is used,
        it is interpreted as local dataset path or dataset URI.
        If a URI is used, protocol-specific parameters apply, given by
        configuration parameter ``slice_storage_options``.

        If there is no target yet, just config and slice:

        * create target metadata from configuration and slice dataset
        * tailor slice according to target metadata and configuration
        * set encoding and attributes in slice according to target metadata
        * write target from slice

        If target exists, with config, slice, and target:

        * create target metadata from configuration and target dataset
        * create slice metadata from configuration and slice dataset
        * verify target and slice metadata are compatible
        * tailor slice according to target metadata and configuration
        * remove encoding and attributes from slice
        * update target from slice

        Args:
            slice_obj: The slice object.
            slice_index: An index identifying the slice.
        """

        ctx = Context(self._config)

        with get_slice_dataset(ctx, slice_obj, slice_index) as slice_dataset:
            slice_metadata = ctx.get_dataset_metadata(slice_dataset)
            if ctx.target_metadata is None:
                ctx.target_metadata = slice_metadata
            else:
                ctx.target_metadata.assert_compatible_slice(
                    slice_metadata, ctx.append_dim_name
                )

            transaction = Transaction(
                ctx.target_dir, ctx.temp_dir, disable_rollback=ctx.disable_rollback
            )
            with transaction as rollback_callback:
                if ctx.target_metadata is slice_metadata:
                    create_target_from_slice(ctx, slice_dataset, rollback_callback)
                else:
                    update_target_from_slice(ctx, slice_dataset, rollback_callback)


def create_target_from_slice(
    ctx: Context, slice_ds: xr.Dataset, rollback_cb: RollbackCallback
):
    target_dir = ctx.target_dir
    logger.info(f"Creating target dataset {target_dir.uri}")
    target_ds = tailor_target_dataset(slice_ds, ctx.target_metadata)
    if ctx.dry_run:
        return
    # TODO: adjust global attributes dependent on append_dim,
    #  e.g., time coverage
    try:
        target_ds.to_zarr(
            store=target_dir.uri,
            storage_options=target_dir.storage_options,
            zarr_version=ctx.zarr_version,
            write_empty_chunks=False,
            consolidated=True,
        )
    finally:
        if target_dir.exists():
            rollback_cb("delete_dir", "", None)


def update_target_from_slice(
    ctx: Context, slice_ds: xr.Dataset, rollback_cb: RollbackCallback
):
    target_dir = ctx.target_dir
    logger.info(f"Updating target dataset {target_dir.uri}")
    append_dim_name = ctx.append_dim_name

    slice_ds = tailor_slice_dataset(slice_ds, ctx.target_metadata, append_dim_name)

    if ctx.dry_run:
        return

    # TODO: adjust global attributes dependent on append_dim,
    #  e.g., time coverage

    store = RollbackStore(target_dir.fs.get_mapper(root=target_dir.path), rollback_cb)
    slice_ds.to_zarr(
        store=store,
        write_empty_chunks=False,
        consolidated=True,
        mode="a",
        append_dim=append_dim_name,
    )
