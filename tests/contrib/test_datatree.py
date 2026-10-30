#  Copyright © 2024-2026 Brockmann Consult and contributors
#  Permissions are hereby granted under the terms of the MIT License:
#  https://opensource.org/licenses/MIT.

import pytest
import xarray as xr

from tests.helpers import clear_memory_fs, make_test_dataset
from zappend.contrib import zappend_datatrees

TARGET = "memory://datatree-target.zarr"


def make_test_datatree(
    time_index: int,
    *,
    include_group_1: bool = True,
    group_1_shape: tuple[int, int] = (3, 2),
) -> xr.DataTree:
    groups = {
        "/": xr.Dataset(attrs={"title": "nested test data", "source": "test"}),
        "/0": make_test_dataset(
            shape=(1, 2, 3), chunks=(1, 2, 3), index=time_index
        ),
    }
    if include_group_1:
        groups["/1"] = make_test_dataset(
            shape=(1, *group_1_shape), chunks=(1, *group_1_shape), index=time_index
        )
    return xr.DataTree.from_dict(groups)


@pytest.fixture(autouse=True)
def clear_memory():
    clear_memory_fs()


@pytest.mark.parametrize("zarr_version", [2, 3])
def test_append_datatrees_to_nested_zarr_groups(zarr_version):
    trees = [make_test_datatree(0), make_test_datatree(1)]

    assert zappend_datatrees(
        trees, target_dir=TARGET, zarr_version=zarr_version
    ) == 2

    for group in ("0", "1"):
        with xr.open_zarr(
            TARGET,
            group=group,
            zarr_format=zarr_version,
            consolidated=zarr_version == 2,
        ) as dataset:
            assert dataset.sizes["time"] == 2

    with xr.open_zarr(
        TARGET, zarr_format=zarr_version, consolidated=zarr_version == 2
    ) as root:
        assert root.attrs == {"title": "nested test data", "source": "test"}


def test_append_datatrees_rejects_different_group_paths_before_writing():
    with pytest.raises(ValueError, match="different group paths"):
        zappend_datatrees(
            [make_test_datatree(0), make_test_datatree(1, include_group_1=False)],
            target_dir=TARGET,
        )

    with pytest.raises(FileNotFoundError):
        xr.open_zarr(TARGET)


def test_append_datatrees_validates_all_groups_before_writing():
    with pytest.raises(ValueError, match="Wrong size for dimension"):
        zappend_datatrees(
            [make_test_datatree(0), make_test_datatree(1, group_1_shape=(4, 2))],
            target_dir=TARGET,
        )

    with pytest.raises(FileNotFoundError):
        xr.open_zarr(TARGET)


def test_append_datatrees_also_supports_a_data_bearing_root():
    trees = [
        xr.DataTree(make_test_dataset(shape=(1, 2, 3), index=index))
        for index in (0, 1)
    ]

    zappend_datatrees(trees, target_dir=TARGET)

    with xr.open_zarr(TARGET, zarr_format=2) as dataset:
        assert dataset.sizes["time"] == 2


def test_append_datatrees_force_new_replaces_complete_tree():
    zappend_datatrees(
        [make_test_datatree(0), make_test_datatree(1)], target_dir=TARGET
    )

    zappend_datatrees([make_test_datatree(3)], target_dir=TARGET, force_new=True)

    for group in ("0", "1"):
        with xr.open_zarr(TARGET, group=group, zarr_format=2) as dataset:
            assert dataset.sizes["time"] == 1
            assert dataset.time.values[0] == make_test_datatree(3)[group].ds.time.values[0]
