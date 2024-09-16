# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
import logging
from typing import Any, Hashable

import xarray as xr
import fsspec

from zappend.api import zappend

# Note, the function may be easily adapted to zappend
# to existing multi-level datasets.


def write_levels(
    source_path: str,
    source_storage_options: dict[str, Any] | None = None,
    target_path: str | None = None,
    num_levels: int | None = None,
    agg_methods: dict[str, Any] | None = None,
    use_saved_levels: bool = False,
    link_level_zero: bool = False,
    xy_dim_names: tuple[str, str] | None = None,
    tile_size: tuple[int, int] | None = None,
    **zappend_config,
):
    """TODO - document me"""
    from xcube.core.tilingscheme import get_num_levels
    from xcube.core.gridmapping import GridMapping
    from xcube.core.subsampling import get_dataset_agg_methods
    from xcube.core.subsampling import subsample_dataset
    from xcube.util.fspath import get_fs_path_class

    target_dir = zappend_config.pop("target_dir", None)
    if not target_dir and not target_path:
        raise ValueError("either 'target_dir' or 'target_path' can be given, not both")
    if target_dir and target_path and target_dir != target_path:
        raise ValueError("either 'target_dir' or 'target_path' can be given, not both")
    target_path = target_path or target_dir
    target_storage_options = zappend_config.pop(
        "target_storage_options", source_storage_options or {}
    )
    target_fs, target_root = fsspec.core.url_to_fs(
        target_path, **target_storage_options
    )

    source_fs, source_root = fsspec.core.url_to_fs(
        source_path,
        **(
            source_storage_options
            if source_storage_options is not None
            else target_storage_options
        ),
    )
    source_store = source_fs.get_mapper(root=source_root)
    source_ds = xr.open_zarr(source_store)

    logger = logging.getLogger("zappend")

    grid_mapping: GridMapping | None = None

    if xy_dim_names is None:
        grid_mapping = grid_mapping or GridMapping.from_dataset(source_ds)
        xy_dim_names = grid_mapping.xy_dim_names

    if tile_size is None:
        grid_mapping = grid_mapping or GridMapping.from_dataset(source_ds)
        tile_size = grid_mapping.tile_size

    if num_levels is None:
        grid_mapping = grid_mapping or GridMapping.from_dataset(source_ds)
        num_levels = get_num_levels(grid_mapping.size, tile_size)

    agg_methods = get_dataset_agg_methods(
        source_ds,
        xy_dim_names=xy_dim_names,
        agg_methods=agg_methods,
    )

    force_new = zappend_config.pop("force_new", None)

    append_dim = zappend_config.pop("append_dim", "time")
    append_coord = source_ds.coords[append_dim]

    variables = get_variables_config(
        source_ds,
        {
            xy_dim_names[0]: tile_size[0],
            xy_dim_names[1]: tile_size[1],
            append_dim: 1,
        },
        variables=zappend_config.pop("variables", None),
    )

    with target_fs.open(f"{target_root}/.zlevels", "wt") as fp:
        levels_data: dict[str, Any] = dict(
            version="1.0",
            num_levels=num_levels,
            agg_methods=agg_methods,
            use_saved_levels=use_saved_levels,
        )
        json.dump(levels_data, fp, indent=2)

    if link_level_zero:
        path_class = get_fs_path_class(target_fs)
        rel_source_path = (
            "../"
            + path_class(source_root)
            .relative_to(path_class(target_root).parent)
            .as_posix()
        )
        with target_fs.open(f"{target_root}/0.link", "wt") as fp:
            fp.write(rel_source_path)

    subsample_dataset_kwargs = dict(xy_dim_names=xy_dim_names, agg_methods=agg_methods)

    for slice_index in range(append_coord.size):
        slice_ds_indexer = {append_dim: slice(slice_index, slice_index + 1)}
        slice_ds = source_ds.isel(slice_ds_indexer)

        for level_index in range(num_levels):
            if level_index == 0:
                level_slice_ds = slice_ds
            elif use_saved_levels:
                if level_index == 1:
                    prev_level_ds = source_ds
                else:
                    prev_level_path = f"{target_root}/{level_index - 1}.zarr"
                    prev_level_store = target_fs.get_mapper(root=prev_level_path)
                    prev_level_ds = xr.open_zarr(prev_level_store)
                level_slice_ds = subsample_dataset(
                    prev_level_ds.isel(slice_ds_indexer),
                    step=2,
                    **subsample_dataset_kwargs,
                )
            else:
                level_slice_ds = subsample_dataset(
                    slice_ds,
                    step=2**level_index,
                    **subsample_dataset_kwargs,
                )

            if not link_level_zero or level_index > 0:
                level_slice_path = f"{target_path}/{level_index}.zarr"
                zappend(
                    [level_slice_ds],
                    target_dir=level_slice_path,
                    target_storage_options=target_storage_options,
                    append_dim=append_dim,
                    force_new=force_new if slice_index == 0 else False,
                    variables=variables,
                    **zappend_config,
                )

        logger.info(f"done writing {target_path}")


def get_variables_config(
    dataset: xr.Dataset,
    chunk_sizes: dict[Hashable, int],
    variables: dict[str, dict[str, Any]] | None = None,
):
    """Define the chunk sizes for the variables in *dataset*.

    Args:
        dataset: The dataset
        chunk_sizes: The chunk sizes
        variables: Value of the zappend ``variables``
            configuration parameter
    Return:
        A zappend compatible with the zappend ``variables``
        configuration parameter.
    """
    var_configs = dict(variables or {})
    for var_name, var in dataset.variables.items():
        var_name = str(var_name)
        var_config = dict(var_configs.get(var_name, {}))
        var_encoding = dict(var_config.get("encoding", {}))
        var_chunks = var_encoding.get("chunks")
        if "chunks" not in var_encoding and var.dims:
            if var_name in dataset.coords or set(var.dims).isdisjoint(chunk_sizes):
                var_chunks = None
            else:
                var_chunks = [chunk_sizes.get(dim) for dim in var.dims]
        var_encoding["chunks"] = var_chunks
        var_config["encoding"] = var_encoding
        var_configs[var_name] = var_config
    return var_configs
