#  Copyright © 2024-2026 Brockmann Consult and contributors
#  Permissions are hereby granted under the terms of the MIT License:
#  https://opensource.org/licenses/MIT.

import collections.abc
from collections.abc import Iterable
from typing import Any

import numpy as np
import xarray as xr
import zarr

from .config import (
    Config,
    ConfigLike,
    eval_dyn_config_attrs,
    exclude_from_config,
    get_dyn_config_attrs_env,
    has_dyn_config_attrs,
    merge_configs,
    normalize_config,
    validate_config,
)
from .context import Context
from .fsutil.transaction import RollbackCallback, Transaction
from .log import configure_logging, logger
from .profiler import Profiler
from .rollbackstore import make_rollback_store, make_zarr_store
from .slice import SliceItem, open_slice_dataset
from .tailoring import tailor_slice_dataset, tailor_target_dataset


class Processor:
    """This class implements the actual `zappend` process.

    Args:
        config: Processor configuration.
        kwargs: Additional configuration parameters.
            Can be used to pass or override configuration values in `config`.
    """

    def __init__(self, config: ConfigLike = None, **kwargs):
        config = normalize_config(config)
        config.update({k: v for k, v in kwargs.items() if v is not None})
        # Validate value of slice_source later,
        # it could be a callable instead of a str
        # See https://github.com/bcdev/zappend/issues/49
        with exclude_from_config(config, "slice_source") as c:
            validate_config(c)
        _config = Config(config)
        configure_logging(_config.logging)
        if _config.force_new:
            logger.warning(
                "Setting 'force_new' is enabled. This will"
                " permanently delete existing targets (no rollback)."
            )
            delete_target_permanently(_config)
        self._config = _config
        # Keep the normalized configuration so DataTree child groups can use
        # the same settings with only their target path changed.
        self._config_dict = dict(config)
        self._slice_container_type: type[xr.Dataset | xr.DataTree] | None = None
        self._datatree_group_paths: frozenset[str] | None = None
        self._profiler = Profiler(_config.profiling)

    def process_slices(self, slices: Iterable[SliceItem]) -> int:
        """Process the given `slices`.
        Passes each slice in `slices` to the `process_slice()` method.

        Args:
            slices: Iterable of slice items.
        Returns:
            The number of slices processed.
        """
        slice_index = 0
        with self._profiler:
            for slice_item in slices:
                self.process_slice(slice_item, slice_index=slice_index)
                slice_index += 1
        return slice_index

    def process_slice(self, slice_item: SliceItem, slice_index: int = 0):
        """Process a single slice item *slice_item*.

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
            slice_item: The slice item.
            slice_index: An index identifying the slice.
        """

        ctx = Context(self._config)

        with open_slice_dataset(ctx, slice_item, slice_index) as slice_data:
            if isinstance(slice_data, xr.Dataset):
                self._assert_slice_container_type(xr.Dataset)
                self.process_dataset_slice(ctx, slice_data)
            elif isinstance(slice_data, xr.DataTree):
                self._assert_slice_container_type(xr.DataTree)
                self.process_datatree_slice(ctx, slice_data)
            else:
                raise TypeError(
                    "slice source must yield an xarray.Dataset or xarray.DataTree, "
                    f"but yielded {type(slice_data).__name__!r}"
                )

    def _assert_slice_container_type(
        self, container_type: type[xr.Dataset | xr.DataTree]
    ):
        """Ensure a streaming run does not mix Dataset and DataTree slices."""
        if self._slice_container_type is None:
            self._slice_container_type = container_type
        elif container_type is not self._slice_container_type:
            raise TypeError(
                "Cannot mix xarray.Dataset and xarray.DataTree slices in one "
                f"zappend run; expected {self._slice_container_type.__name__}, "
                f"got {container_type.__name__}"
            )

    def process_datatree_slice(self, ctx: Context, tree: xr.DataTree):
        """Append one DataTree slice in a transaction spanning all groups.

        This is deliberately part of :class:`Processor`: DataTrees can now be
        returned by any configured slice source and use the same polling,
        profiling, force-new, and configuration flow as Dataset slices.
        Each child group's rollback actions are registered relative to the
        DataTree root, so an error restores the complete tree to its state
        before this slice began.
        """
        group_paths = frozenset(tree.groups)
        unknown_group_paths = set(ctx.config.group_variables) - group_paths
        if unknown_group_paths:
            raise ValueError(
                "group_variables contains paths that are not present in the "
                f"DataTree: {sorted(unknown_group_paths)!r}"
            )
        if self._datatree_group_paths is None:
            self._datatree_group_paths = group_paths
        elif group_paths != self._datatree_group_paths:
            raise ValueError(
                "DataTree has different group paths; expected "
                f"{sorted(self._datatree_group_paths)!r}, got {sorted(group_paths)!r}"
            )

        transaction = Transaction(
            ctx.config.target_dir,
            ctx.config.temp_dir,
            disable_rollback=ctx.config.disable_rollback,
        )
        with transaction as rollback_callback:
            self._process_datatree_slice(ctx, tree, rollback_callback)

    def _process_datatree_slice(
        self, ctx: Context, tree: xr.DataTree, rollback_callback: RollbackCallback
    ):
        """Process a DataTree while recording all paths relative to its root."""

        data_paths = tuple(
            sorted(
                (node.path for node in tree.subtree if node.ds.data_vars),
                key=lambda path: (path.count("/"), path),
            )
        )
        root_has_data = "/" in data_paths
        if (
            not root_has_data
            and not ctx.config.dry_run
            and not ctx.config.target_dir.exists()
        ):
            # Child group transactions require their parent directory. Avoid
            # consolidating here, as it would be stale after child writes.
            rollback_callback("delete_dir", "", None)
            xr.Dataset().to_zarr(
                ctx.config.target_dir.uri,
                mode="w",
                zarr_format=ctx.config.zarr_version,
                consolidated=False,
                storage_options=ctx.config.target_dir.storage_options,
            )

        for group_path in data_paths:
            group_variables = ctx.config.group_variables.get(group_path)
            if group_path == "/" and group_variables is None:
                group_ctx = ctx
            else:
                group_target = (
                    ctx.config.target_dir
                    if group_path == "/"
                    else ctx.config.target_dir / group_path.lstrip("/")
                )
                group_config = dict(self._config_dict)
                group_config["target_dir"] = group_target.uri
                group_config["variables"] = merge_configs(
                    self._config_dict.get("variables") or {}, group_variables or {}
                )
                # force_new was already applied once to the complete root in
                # Processor.__init__, never once per child group.
                group_config["force_new"] = False
                group_ctx = Context(group_config)
            self.process_dataset_slice(
                group_ctx,
                tree[group_path].ds,
                rollback_callback=prefix_rollback_callback(
                    rollback_callback, group_path
                ),
            )

        if not root_has_data and not ctx.config.dry_run:
            update_datatree_root_attrs(ctx, tree, rollback_callback)

        if not ctx.config.dry_run and ctx.config.zarr_version == 2:
            zarr.consolidate_metadata(
                make_rollback_store(
                    ctx.config.target_dir.fs.get_mapper(root=ctx.config.target_dir.path),
                    rollback_callback,
                )
            )

    @staticmethod
    def process_dataset_slice(
        ctx: Context,
        slice_dataset: xr.Dataset,
        rollback_callback: RollbackCallback | None = None,
    ):
        """Process one Dataset slice using the normal transactional append path."""
        slice_metadata = ctx.get_dataset_metadata(slice_dataset)
        if ctx.target_metadata is None:
            ctx.target_metadata = slice_metadata
        else:
            ctx.target_metadata.assert_compatible_slice(
                slice_metadata, ctx.config.append_dim
            )

        verify_append_labels(ctx, slice_dataset)

        if rollback_callback is None:
            transaction = Transaction(
                ctx.config.target_dir,
                ctx.config.temp_dir,
                disable_rollback=ctx.config.disable_rollback,
            )
            with transaction as transaction_rollback_callback:
                process_dataset_slice_with_rollback(
                    ctx, slice_dataset, slice_metadata, transaction_rollback_callback
                )
        else:
            process_dataset_slice_with_rollback(
                ctx, slice_dataset, slice_metadata, rollback_callback
            )


def process_dataset_slice_with_rollback(
    ctx: Context,
    slice_dataset: xr.Dataset,
    slice_metadata: Any,
    rollback_callback: RollbackCallback,
):
    if ctx.target_metadata is slice_metadata:
        create_target_from_slice(ctx, slice_dataset, rollback_callback)
    else:
        update_target_from_slice(ctx, slice_dataset, rollback_callback)


def prefix_rollback_callback(
    rollback_callback: RollbackCallback, group_path: str
) -> RollbackCallback:
    """Adapt child-group paths to the root transaction's relative paths."""
    prefix = group_path.strip("/")
    if not prefix:
        return rollback_callback

    def prefixed_callback(action, target_path, data):
        path = prefix if not target_path else f"{prefix}/{target_path}"
        rollback_callback(action, path, data)

    return prefixed_callback


def update_datatree_root_attrs(
    ctx: Context, tree: xr.DataTree, rollback_callback: RollbackCallback
):
    """Apply Dataset attribute semantics to a metadata-only DataTree root."""
    root_group = zarr.open_group(
        make_rollback_store(
            ctx.config.target_dir.fs.get_mapper(root=ctx.config.target_dir.path),
            rollback_callback,
        ),
        mode="a",
        zarr_format=ctx.config.zarr_version,
    )
    attrs = dict(tree.ds.attrs)
    attrs.update(ctx.config.attrs)
    if ctx.config.attrs_update_mode == "replace":
        root_group.attrs.clear()
        root_group.attrs.update(attrs)
    elif ctx.config.attrs_update_mode == "update":
        root_group.attrs.update(attrs)
    elif ctx.config.attrs_update_mode == "keep":
        for key, value in attrs.items():
            root_group.attrs.setdefault(key, value)
    elif ctx.config.attrs_update_mode == "ignore":
        root_group.attrs.update(ctx.config.attrs)


def delete_target_permanently(config: Config):
    target_dir = config.target_dir
    if target_dir.exists():
        logger.warning(f"Permanently deleting {target_dir}")
        if not config.dry_run:
            target_dir.delete(recursive=True)
    target_lock = Transaction.get_lock_file(target_dir)
    if target_lock.exists():
        logger.warning(f"Permanently deleting {target_lock}")
        if not config.dry_run:
            target_lock.delete()


def create_target_from_slice(
    ctx: Context, slice_ds: xr.Dataset, rollback_cb: RollbackCallback
):
    target_dir = ctx.config.target_dir
    logger.info(f"Creating target dataset {target_dir.uri}")

    target_ds = tailor_target_dataset(ctx, slice_ds)

    if ctx.config.dry_run:
        return

    try:
        target_ds.to_zarr(
            store=target_dir.uri,
            storage_options=target_dir.storage_options,
            zarr_format=ctx.config.zarr_version,
            write_empty_chunks=False,
            consolidated=ctx.config.zarr_version == 2,
        )

        post_create_target(ctx, target_ds)

    finally:
        if target_dir.exists():
            rollback_cb("delete_dir", "", None)


def update_target_from_slice(
    ctx: Context, slice_ds: xr.Dataset, rollback_cb: RollbackCallback
):
    target_dir = ctx.config.target_dir
    logger.info(f"Updating target dataset {target_dir.uri}")

    slice_ds = tailor_slice_dataset(ctx, slice_ds)

    if ctx.config.dry_run:
        return

    target_store = make_rollback_store(
        target_dir.fs.get_mapper(root=target_dir.path), rollback_cb
    )
    slice_ds.to_zarr(
        store=target_store,
        write_empty_chunks=False,
        consolidated=ctx.config.zarr_version == 2,
        mode="a",
        append_dim=ctx.config.append_dim,
        zarr_format=ctx.config.zarr_version,
    )

    post_update_target(ctx, target_store, slice_ds)


def post_create_target(ctx: Context, target_ds: xr.Dataset):
    """Post-process the target dataset given by `target_ds`
    that has just been created.

    Args:
        ctx: Current processing context.
        target_ds: The target dataset.
    """
    target_attrs = target_ds.attrs
    if ctx.config.permit_eval and has_dyn_config_attrs(target_attrs):
        target_store = ctx.config.target_dir.fs.get_mapper(
            root=ctx.config.target_dir.path
        )
        resolve_target_attrs(
            make_zarr_store(target_store),
            target_ds,
            target_attrs,
            ctx.config.zarr_version,
        )


def post_update_target(ctx: Context, target_store: Any, slice_ds: xr.Dataset):
    """Post-process the target dataset given by `target_store` that has just
    been updated by `slice_ds`.

    Args:
        ctx: Current processing context.
        target_store: The target dataset as a rollback store.
        slice_ds: The current slice dataset that has already been appended.
    """
    target_attrs = slice_ds.attrs
    if ctx.config.permit_eval and has_dyn_config_attrs(target_attrs):
        with xr.open_zarr(
            target_store, zarr_format=ctx.config.zarr_version
        ) as target_ds:
            resolve_target_attrs(
                target_store, target_ds, target_attrs, ctx.config.zarr_version
            )


def resolve_target_attrs(
    target_store: collections.abc.MutableMapping,
    target_ds: xr.Dataset,
    target_attrs: dict[str, Any],
    zarr_version: int,
):
    resolved_attrs = eval_dyn_config_attrs(
        target_attrs,
        get_dyn_config_attrs_env(target_ds),
    )
    target_group = zarr.open_group(
        store=target_store, mode="a", zarr_format=zarr_version
    )
    target_group.attrs.update(resolved_attrs)
    if zarr_version == 2:
        zarr.consolidate_metadata(target_store)


def verify_append_labels(ctx: Context, slice_ds: xr.Dataset):
    append_step = ctx.config.append_step
    if append_step is None:
        # If step size is not specified, there is nothing to do
        return

    append_labels: xr.DataArray = slice_ds.get(ctx.config.append_dim)
    if append_labels is None:
        # It is ok to not have append-labels in the dataset
        return

    last_append_label = ctx.last_append_label
    if last_append_label is not None:
        # Existing target dataset with append-labels
        step_sizes = np.diff(append_labels.values, prepend=last_append_label)
    elif append_labels.size >= 2:
        # Target dataset does not exist yet, but we have a number of labels
        step_sizes = np.diff(append_labels.values)
    else:
        # Target dataset does not exist yet, and just one label,
        # so nothing to verify yet.
        return

    zero = step_sizes.dtype.type(0)

    if append_step == "+":
        # Force monotonically increasing labels
        if not np.all(step_sizes > zero):
            raise ValueError(
                "Cannot append slice because labels must be monotonically increasing."
            )
    elif append_step == "-":
        # Force monotonically decreasing labels
        if not np.all(step_sizes < zero):
            raise ValueError(
                "Cannot append slice because labels must be monotonically decreasing."
            )
    else:
        # Force fixed step size
        if np.issubdtype(step_sizes.dtype, np.timedelta64):
            deltas = step_sizes - to_timedelta(append_step)
        else:
            deltas = step_sizes - append_step
        if not np.all(deltas == zero):
            raise ValueError(
                "Cannot append slice because this would result in an invalid step size."
            )


def to_timedelta(append_step: str | float) -> np.timedelta64:
    if isinstance(append_step, str):
        i = 0
        for i in range(len(append_step)):
            if append_step[i].isalpha():
                break
        if i == 0:
            count = 1
            unit = append_step
        else:
            count = int(append_step[0:i])
            unit = append_step[i:]
    else:
        count = int(append_step)
        unit = "s"
    return np.timedelta64(count, unit)
