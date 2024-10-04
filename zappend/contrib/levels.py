# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
import logging
import warnings
from typing import Any, Hashable

import xarray as xr
import fsspec

from zappend.api import zappend

# Note, the function may be easily adapted to zappend
# to existing multi-level datasets.


def write_levels(
    *,
    source_ds: xr.Dataset | None = None,
    source_path: str | None = None,
    source_storage_options: dict[str, Any] | None = None,
    source_append_offset: int | None = None,
    target_path: str | None = None,
    num_levels: int | None = None,
    tile_size: tuple[int, int] | None = None,
    agg_methods: str | dict[str, Any] | None = None,
    use_saved_levels: bool = False,
    link_level_zero: bool = False,
    xy_dim_names: tuple[str, str] | None = None,
    **zappend_config,
):
    """Write a dataset given by `source_ds` or `source_path` to `target_path`
    using the
    [multi-level dataset format](https://xcube.readthedocs.io/en/latest/mldatasets.html)
    as specified by
    [xcube](https://github.com/xcube-dev/xcube).

    It resembles the `store.write_data(dataset, "<name>.levels", ...)` method
    provided by the xcube filesystem data stores ("file", "s3", "memory", etc.).
    The zappend version may be used for potentially very large datasets in terms
    of dimension sizes or for datasets with very large number of chunks.
    It is considerably slower than the xcube version (which basically uses
    `xarray.to_zarr()` for each resolution level), but should run robustly with
    stable memory consumption.

    The function opens the source dataset and subdivides it into dataset slices
    along the append dimension given by `append_dim`, which defaults
    to `"time"`. The slice size in the append dimension is one.
    Each slice is downsampled to the number of levels and each slice level
    dataset is created/appended the target dataset's individual level
    datasets.

    The target dataset's chunk size in the spatial x- and y-dimensions will
    be the same as the specified (or derived) tile size.
    The append dimension will be one. The chunking will be reflected as the
    `variables` configuration parameter passed to each `zappend()` call.
    If configuration parameter `variables` is also given as part of
    `zappend_config`, it will be merged with the chunk definitions.

    **Important notes:**

    - This function depends on `xcube.core.gridmapping.GridMapping` and
      ` xcube.core.subsampling.subsample_dataset()` of the `xcube` package.
    - `write_levels()` is not as robust as zappend itself. For example,
      there may be inconsistent dataset levels if the processing
      is interrupted while a level is appended.
    - There is a remaining issue that with (coordinate) variables that
      have a dimension that is not a dimension of any variable that has
      one of the spatial dimensions, e.g., `time_bnds` with dimensions
      `time` and `bnds`. Please exclude such variables using the parameter
      `excluded_variables`.

    Args:
        source_ds: The source dataset.
            Must be given in case `source_path` is not given.
        source_path: The source dataset path.
            If `source_ds` is provided and `link_level_zero` is true,
            then `source_path` must also be provided in order
            to determine the path of the level zero source.
        source_storage_options: Storage options for the source
            dataset's filesystem.
        source_append_offset: Optional offset in the append dimension.
            Only slices with indexes greater or equal the offset are
            appended.
        target_path: The target multi-level dataset path.
            Filename extension should be `.levels`, by convention.
            If not given, `target_dir` should be passed as part of the
            `zappend_config`. (The name `target_path` is used here for
            consistency with `source_path`.)
        num_levels: Optional number of levels.
            If not given, a reasonable number of levels is computed
            from `tile_size`.
        tile_size: Optional tile size in the x- and y-dimension in pixels.
            If not given, the tile size is computed from the source
            dataset's chunk sizes in the x- and y-dimensions.
        xy_dim_names:
            Optional dimension names that identify the x- and y-dimensions.
            If not given, derived from the source dataset's grid mapping,
            if any.
        agg_methods: An aggregation method for all data variables or a
            mapping that provides the aggregation method for a variable
            name. Possible aggregation methods are
            `"first"`, `"min"`, `"max"`, `"mean"`, `"median"`.
        use_saved_levels: Whether a given, already written resolution level
            serves as input to aggregation for the next level. If `False`,
            the default, each resolution level other than zero is computed
            from the source dataset. If `True`, the function may perform
            significantly faster, but be aware that the aggregation
            methods `"first"` and `"median"` will produce inaccurate results.
        link_level_zero: Whether to _not_ write the level zero of the target
            multi-level dataset and link it instead. In this case, a link
            file `{target_path}/0.link` will be written.
            If `False`, the default, a level dataset `{target_path}/0.zarr`
            will be written instead.
        zappend_config:
            Configuration passed to zappend as `zappend(slice, **zappend_config)`
            for each slice in the append dimension. The zappend `config`
            parameter is not supported.
    """
    from xcube.core.gridmapping import GridMapping
    from xcube.core.subsampling import get_dataset_agg_methods
    from xcube.core.subsampling import subsample_dataset
    from xcube.core.tilingscheme import get_num_levels
    from xcube.util.fspath import get_fs_path_class

    config = zappend_config.pop("config", None)
    if config is not None:
        raise TypeError("write_levels() got an unexpected keyword argument 'config'")

    dry_run = zappend_config.pop("dry_run", False)

    if dry_run and use_saved_levels:
        warnings.warn(f"'use_saved_levels' argument is not applicable if dry_run=True")
        use_saved_levels = False

    target_dir = zappend_config.pop("target_dir", None)
    if not target_path and not target_dir:
        raise ValueError("missing 'target_path' argument")
    if target_dir and target_path:
        raise ValueError("either 'target_dir' or 'target_path' can be given, not both")
    target_path = target_path or target_dir
    target_storage_options = zappend_config.pop(
        "target_storage_options", source_storage_options or {}
    )
    target_fs, target_root = fsspec.core.url_to_fs(
        target_path, **target_storage_options
    )

    force_new = zappend_config.pop("force_new", None)

    if source_path is not None:
        source_fs, source_root = fsspec.core.url_to_fs(
            source_path,
            **(
                source_storage_options
                if source_storage_options is not None
                else target_storage_options
            ),
        )
        if source_ds is None:
            source_store = source_fs.get_mapper(root=source_root)
            source_ds = xr.open_zarr(source_store)
    else:
        source_root = None
        if not isinstance(source_ds, xr.Dataset):
            raise TypeError(
                f"'source_ds' argument must be of type 'xarray.Dataset',"
                f" but was {type(source_ds).__name__!r}"
            )
        if link_level_zero:
            raise ValueError(
                f"'source_path' argument must be provided"
                f" if 'link_level_zero' is used"
            )

    append_dim = zappend_config.pop("append_dim", "time")
    append_coord = source_ds.coords[append_dim]

    if source_append_offset is None:
        source_append_offset = 0
    elif not isinstance(source_append_offset, int):
        raise TypeError(
            f"'source_append_offset' argument must be of type 'int',"
            f" but was {type(source_append_offset).__name__!r}"
        )
    if not (0 <= source_append_offset < append_coord.size):
        raise ValueError(
            f"'source_append_offset' argument"
            f" must be >=0 and <{append_coord.size},"
            f" but was {source_append_offset}"
        )

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

    variables = get_variables_config(
        source_ds,
        {
            xy_dim_names[0]: tile_size[0],
            xy_dim_names[1]: tile_size[1],
            append_dim: 1,
        },
        variables=zappend_config.pop("variables", None),
    )

    target_exists = target_fs.exists(target_root)
    if target_exists:
        logger.info(f"Target directory {target_path} exists")
        if force_new:
            logger.warning(f"Permanently deleting {target_path} (no rollback)")
            if not dry_run:
                target_fs.rm(target_root, recursive=True)
    else:
        logger.info(f"Creating target directory {target_path}")
        if not dry_run:
            target_fs.mkdirs(target_root, exist_ok=True)

    if not dry_run:
        with target_fs.open(f"{target_root}/.zlevels", "wt") as fp:
            levels_data: dict[str, Any] = dict(
                version="1.0",
                num_levels=num_levels,
                agg_methods=agg_methods,
                use_saved_levels=use_saved_levels,
            )
            json.dump(levels_data, fp, indent=2)

    if (not dry_run) and link_level_zero:
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

    num_slices = append_coord.size - source_append_offset
    for slice_index in range(num_slices):
        append_index = source_append_offset + slice_index
        slice_ds_indexer = {append_dim: slice(append_index, append_index + 1)}
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
                    dry_run=dry_run,
                    force_new=force_new if slice_index == 0 else False,
                    variables=variables,
                    **zappend_config,
                )
                steps_total = num_slices * num_levels
                percent_total = (
                    100 * ((slice_index * num_levels) + level_index + 1) / steps_total
                )
                logger.info(
                    f"Level slice written to {level_slice_path},"
                    f" {slice_index + 1}/{num_slices} slices,"
                    f" {level_index + 1}/{num_levels} levels,"
                    f" {percent_total:.2f}% total"
                )
        logger.info(f"Done appending {num_levels} level slices to {target_path}")
    logger.info(f"Done appending {num_slices} slices to {target_path}")


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

        if "dims" not in var_config and var.dims:
            var_config["dims"] = [str(dim) for dim in var.dims]

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
