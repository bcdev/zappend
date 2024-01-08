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
from .slicesource import open_slice_source
from .tailoring import tailor_target_dataset
from .tailoring import tailor_slice_dataset


class Processor:
    def __init__(self, config: ConfigLike = None, **kwargs):
        config = normalize_config(config)
        config.update({k: v for k, v in kwargs.items() if v is not None})
        validate_config(config)
        configure_logging(config.get("logging"))
        self._config = config

    def process_slices(self,
                       slice_iter: Iterable[str | xr.Dataset]):
        for slice_obj in slice_iter:
            self.process_slice(slice_obj)

    def process_slice(self, slice_obj: str | xr.Dataset):
        """Process a single slice.

        If there is no target yet, just config and slice:

        * complete config dataset metadata from slice dataset metadata
        * check config/slice outline compliance
        * copy slice and add/remove vars to/from slice
        * set encoding in slice
        * write target from slice

        If target exists, with config, slice, and target:

        * complete config outline from target outline
        * check config/target outline compliance
        * check config/slice outline compliance
        * copy slice and add/remove vars to/from slice
        * remove encoding from slice
        * update target from slice
        """

        ctx = Context(self._config)

        with open_slice_source(ctx, slice_obj) as slice_dataset:

            slice_metadata = ctx.get_dataset_metadata(slice_dataset)
            if ctx.target_metadata is None:
                ctx.target_metadata = slice_metadata

            with Transaction(ctx.target_dir, ctx.temp_dir) as rollback_cb:
                if ctx.target_metadata is slice_metadata:
                    create_target_from_slice(ctx,
                                             slice_dataset,
                                             rollback_cb)
                else:
                    update_target_from_slice(ctx,
                                             slice_dataset,
                                             rollback_cb)


def create_target_from_slice(ctx: Context,
                             slice_ds: xr.Dataset,
                             rollback_cb: RollbackCallback):
    logger.info(f"Creating target dataset")
    target_ds = tailor_target_dataset(slice_ds, ctx.target_metadata)
    if ctx.dry_run:
        return
    target_dir = ctx.target_dir
    # TODO: adjust global attributes dependent on append_dim,
    #  e.g., time coverage
    try:
        target_ds.to_zarr(store=target_dir.uri,
                          storage_options=target_dir.storage_options,
                          zarr_version=ctx.zarr_version,
                          write_empty_chunks=False,
                          consolidated=True)
    finally:
        if target_dir.exists():
            rollback_cb("delete_dir", target_dir.path, None)


def update_target_from_slice(ctx: Context,
                             slice_ds: xr.Dataset,
                             rollback_cb: RollbackCallback):
    logger.info(f"Updating target dataset")
    target_dir = ctx.target_dir
    append_dim_name = ctx.append_dim_name

    slice_ds = tailor_slice_dataset(slice_ds,
                                    ctx.target_metadata,
                                    append_dim_name)

    if ctx.dry_run:
        return

    # TODO: adjust global attributes dependent on append_dim,
    #  e.g., time coverage

    store = RollbackStore(target_dir.fs.get_mapper(root=target_dir.path),
                          rollback_cb)
    slice_ds.to_zarr(store=store,
                     write_empty_chunks=False,
                     consolidated=True,
                     mode="a",
                     append_dim=append_dim_name)
