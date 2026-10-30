#  Copyright © 2024-2026 Brockmann Consult and contributors
#  Permissions are hereby granted under the terms of the MIT License:
#  https://opensource.org/licenses/MIT.

"""Experimental support for appending :class:`xarray.DataTree` objects."""

from __future__ import annotations

from collections.abc import Iterable
from copy import deepcopy
from typing import Any

import xarray as xr
import zarr

from zappend.api import zappend
from zappend.config import Config, ConfigLike, normalize_config, validate_config
from zappend.config.defaults import DEFAULT_APPEND_DIM
from zappend.fsutil import FileObj
from zappend.fsutil.transaction import Transaction
from zappend.metadata import DatasetMetadata


def zappend_datatrees(
    trees: Iterable[xr.DataTree],
    config: ConfigLike = None,
    **kwargs: Any,
) -> int:
    """Append corresponding data-bearing nodes of DataTree slices to Zarr groups.

    Each tree represents one slice of a nested Zarr dataset. All trees must
    have identical group paths and every data-bearing group must have the
    configured append dimension. Each group is appended using
    :func:`zappend.api.zappend`, so the usual metadata validation, chunking,
    and rollback behaviour apply independently to every group.

    Root-only attributes are retained even when the root node contains no data
    variables. They follow ``attrs_update_mode`` and ``attrs`` in the same way
    as dataset attributes in the normal zappend API.

    Warning:
        Zarr has no transaction spanning several groups. A failed call rolls
        back the currently processed group, but groups completed earlier in
        the same call remain appended. The full tree is validated for matching
        group paths before any data is written, which prevents the most common
        source of partial trees.

    Args:
        trees: DataTree slice objects to append. Nodes without data variables
            are treated as metadata-only nodes.
        config: zappend configuration, as accepted by :func:`zappend.api.zappend`.
        kwargs: Configuration values that override *config*.

    Returns:
        The number of input trees processed.
    """
    trees = list(trees)
    if not trees:
        return 0
    if not all(isinstance(tree, xr.DataTree) for tree in trees):
        invalid_tree = next(tree for tree in trees if not isinstance(tree, xr.DataTree))
        raise TypeError(
            "'trees' items must have type 'xarray.DataTree', but found "
            f"{type(invalid_tree).__name__!r}"
        )

    config_dict = deepcopy(normalize_config(config))
    config_dict.update({key: value for key, value in kwargs.items() if value is not None})
    if config_dict.get("slice_source") is not None:
        raise ValueError("'slice_source' is not supported by zappend_datatrees()")
    validate_config(config_dict)

    target_uri = config_dict.get("target_dir")
    if not target_uri:
        raise ValueError("Missing 'target_dir' in configuration")
    target_dir = FileObj(target_uri, config_dict.get("target_storage_options"))
    append_dim = config_dict.get("append_dim") or DEFAULT_APPEND_DIM

    group_paths = _validate_tree_structure(trees, append_dim)
    data_paths = _data_paths(trees[0])
    _validate_source_compatibility(trees, data_paths, Config(config_dict), append_dim)
    root_has_data = "/" in data_paths

    # force_new applies to the complete DataTree target, rather than once per
    # child group. The individual calls below must therefore not delete their
    # own group targets.
    force_new = bool(config_dict.get("force_new"))
    dry_run = bool(config_dict.get("dry_run"))
    if force_new and not dry_run:
        if target_dir.exists():
            target_dir.delete(recursive=True)
        target_lock = Transaction.get_lock_file(target_dir)
        if target_lock.exists():
            target_lock.delete()

    # A child target needs its parent directory to exist when its transaction
    # starts. If the root has no dataset, create a metadata-only root group.
    # Do not create it with consolidated metadata: child groups are added
    # afterwards and the root is consolidated once all writes have succeeded.
    if not root_has_data and not dry_run and not target_dir.exists():
        xr.Dataset().to_zarr(
            target_dir.uri,
            mode="w",
            zarr_format=config_dict.get("zarr_version", 2),
            consolidated=False,
            storage_options=target_dir.storage_options,
        )

    child_config = deepcopy(config_dict)
    child_config["force_new"] = False
    for group_path in group_paths:
        if group_path not in data_paths:
            continue
        group_target = target_dir if group_path == "/" else target_dir / group_path.lstrip("/")
        group_config = deepcopy(child_config)
        group_config["target_dir"] = group_target.uri
        zappend((tree[group_path].ds for tree in trees), config=group_config)

    if not root_has_data and not dry_run:
        # Apply attributes in slice order: this preserves the established
        # keep/update/replace semantics when root attributes vary by slice.
        for tree in trees:
            _update_root_attrs(tree, target_dir, config_dict)

    # A v2 root-level .zmetadata written before adding child groups would not
    # describe those children. Consolidating only after a successful complete
    # call avoids publishing that stale metadata.
    if not dry_run and config_dict.get("zarr_version", 2) == 2:
        zarr.consolidate_metadata(target_dir.fs.get_mapper(root=target_dir.path))

    return len(trees)


def _validate_tree_structure(trees: list[xr.DataTree], append_dim: str) -> tuple[str, ...]:
    """Validate group paths and append dimensions before starting writes."""
    expected_group_paths = tuple(trees[0].groups)
    expected_data_paths = _data_paths(trees[0])
    for index, tree in enumerate(trees[1:], start=1):
        if tuple(tree.groups) != expected_group_paths:
            raise ValueError(
                f"DataTree at index {index} has different group paths; expected "
                f"{expected_group_paths!r}, got {tuple(tree.groups)!r}"
            )
        data_paths = _data_paths(tree)
        if data_paths != expected_data_paths:
            raise ValueError(
                f"DataTree at index {index} has data in different groups; expected "
                f"{expected_data_paths!r}, got {data_paths!r}"
            )

    for tree_index, tree in enumerate(trees):
        for group_path in expected_data_paths:
            if append_dim not in tree[group_path].ds.sizes:
                raise ValueError(
                    f"DataTree at index {tree_index}, group {group_path!r} does not "
                    f"have append dimension {append_dim!r}"
                )
    return expected_group_paths


def _data_paths(tree: xr.DataTree) -> tuple[str, ...]:
    return tuple(node.path for node in tree.subtree if node.ds.data_vars)


def _validate_source_compatibility(
    trees: list[xr.DataTree],
    data_paths: tuple[str, ...],
    config: Config,
    append_dim: str,
) -> None:
    """Verify every source group before any group is appended.

    The core processor performs this validation while writing one group. Doing
    it here first ensures an incompatible later group cannot leave an earlier
    group newly appended in this call.
    """
    for group_path in data_paths:
        target_metadata = DatasetMetadata.from_dataset(trees[0][group_path].ds, config)
        for tree in trees[1:]:
            target_metadata.assert_compatible_slice(
                DatasetMetadata.from_dataset(tree[group_path].ds, config), append_dim
            )


def _update_root_attrs(
    tree: xr.DataTree, target_dir: FileObj, config: dict[str, Any]
) -> None:
    """Apply normal zappend attribute semantics to a metadata-only root node."""
    root_group = zarr.open_group(
        target_dir.fs.get_mapper(root=target_dir.path),
        mode="a",
        zarr_format=config.get("zarr_version", 2),
    )
    attrs = dict(tree.ds.attrs)
    attrs.update(config.get("attrs") or {})
    mode = config.get("attrs_update_mode") or "keep"
    if mode == "replace":
        root_group.attrs.clear()
        root_group.attrs.update(attrs)
    elif mode == "update":
        root_group.attrs.update(attrs)
    elif mode == "keep":
        for key, value in attrs.items():
            root_group.attrs.setdefault(key, value)
    elif mode == "ignore":
        # Configured attributes always take precedence, just as in core zappend.
        root_group.attrs.update(config.get("attrs") or {})
