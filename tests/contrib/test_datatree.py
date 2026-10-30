#  Copyright © 2024-2026 Brockmann Consult and contributors
#  Permissions are hereby granted under the terms of the MIT License:
#  https://opensource.org/licenses/MIT.

import pytest
import xarray as xr

from tests.helpers import clear_memory_fs, make_test_dataset
from zappend.api import zappend
from zappend.context import Context
from zappend.fsutil import FileObj

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
def test_zappend_appends_datatrees_to_nested_zarr_groups(zarr_version):
    trees = [make_test_datatree(0), make_test_datatree(1)]

    assert zappend(trees, target_dir=TARGET, zarr_version=zarr_version) == 2

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


def test_zappend_rejects_datatrees_with_different_group_paths():
    with pytest.raises(ValueError, match="different group paths"):
        zappend(
            [make_test_datatree(0), make_test_datatree(1, include_group_1=False)],
            target_dir=TARGET,
        )



def test_zappend_rejects_unknown_group_variable_configuration():
    with pytest.raises(ValueError, match="group_variables contains paths"):
        zappend(
            [make_test_datatree(0)],
            target_dir=TARGET,
            group_variables={"/unknown": {}},
        )


def test_zappend_uses_optional_group_variable_chunk_configuration():
    group_0_variables = {
        variable: {"encoding": {"chunks": [2, 2, 3]}}
        for variable in ("chl", "tsm")
    }
    zappend(
        [make_test_datatree(0), make_test_datatree(1)],
        target_dir=TARGET,
        group_variables={"/0": group_0_variables},
    )

    with xr.open_zarr(TARGET, group="0", zarr_format=2) as dataset:
        assert dataset.chl.encoding["chunks"] == (2, 2, 3)
    with xr.open_zarr(TARGET, group="1", zarr_format=2) as dataset:
        # No group configuration: retain the first slice's existing chunking.
        assert dataset.chl.encoding["chunks"] == (1, 3, 2)


@pytest.mark.parametrize("attrs_update_mode", ["replace", "update", "ignore"])
def test_zappend_applies_root_attribute_update_modes(attrs_update_mode):
    zappend(
        [make_test_datatree(0)],
        target_dir=TARGET,
        attrs_update_mode=attrs_update_mode,
        attrs={"configured": attrs_update_mode},
    )

    with xr.open_zarr(TARGET, zarr_format=2) as root:
        assert root.attrs["configured"] == attrs_update_mode
        if attrs_update_mode == "ignore":
            assert "title" not in root.attrs
        else:
            assert root.attrs["title"] == "nested test data"


def test_context_rejects_a_non_appendable_dataset_target():
    xr.Dataset({"not_time": ("x", [1])}).to_zarr(TARGET, zarr_format=2)

    with pytest.raises(ValueError, match="Append dimension 'time' not found"):
        Context({"target_dir": TARGET})


def test_zappend_rejects_an_invalid_slice_source_result():
    class InvalidContextManager:
        def __enter__(self):
            return object()

        def __exit__(self, *args):
            pass

    with pytest.raises(TypeError, match="slice source must yield"):
        zappend(
            [None], target_dir=TARGET, slice_source=lambda _: InvalidContextManager()
        )


@pytest.mark.parametrize("zarr_version", [2, 3])
def test_zappend_rolls_back_the_complete_failing_datatree_slice(zarr_version):
    zappend([make_test_datatree(0)], target_dir=TARGET, zarr_version=zarr_version)

    with pytest.raises(ValueError, match="Wrong size for dimension"):
        zappend(
            [make_test_datatree(1, group_1_shape=(4, 2))],
            target_dir=TARGET,
            zarr_version=zarr_version,
        )

    # Group /0 was updated before incompatible group /1 was encountered. The
    # root transaction must nevertheless restore both groups to time index 0.
    for group in ("0", "1"):
        with xr.open_zarr(
            TARGET,
            group=group,
            zarr_format=zarr_version,
            consolidated=zarr_version == 2,
        ) as dataset:
            assert dataset.sizes["time"] == 1
            assert dataset.time.values[0] == make_test_datatree(0)[group].ds.time.values[0]


def test_zappend_removes_a_new_tree_when_a_later_group_fails():
    tree = make_test_datatree(0)
    tree["/1"].ds = tree["/1"].ds.isel(time=0, drop=True)

    with pytest.raises(ValueError, match="Append dimension 'time' not found"):
        zappend([tree], target_dir=TARGET)

    assert not FileObj(TARGET).exists()


def test_zappend_accepts_datatrees_from_a_slice_source():
    def make_slice(index: int) -> xr.DataTree:
        return make_test_datatree(index)

    assert zappend(
        [0, 1],
        target_dir=TARGET,
        slice_source=make_slice,
        persist_mem_slices=True,
        zarr_version=3,
    ) == 2

    for group in ("0", "1"):
        with xr.open_zarr(TARGET, group=group, zarr_format=3) as dataset:
            assert dataset.sizes["time"] == 2


def test_zappend_opens_a_nested_zarr_slice_as_a_datatree():
    source = "memory://nested-source.zarr"
    make_test_datatree(0).to_zarr(source, zarr_format=2)

    assert zappend([source], target_dir=TARGET) == 1

    for group in ("0", "1"):
        with xr.open_zarr(TARGET, group=group, zarr_format=2) as dataset:
            assert dataset.sizes["time"] == 1


@pytest.mark.parametrize(
    "slices",
    [
        lambda: [make_test_datatree(0), make_test_dataset(shape=(1, 2, 3))],
        lambda: [make_test_dataset(shape=(1, 2, 3)), make_test_datatree(0)],
    ],
)
def test_zappend_rejects_mixed_dataset_and_datatree_slices(slices):
    with pytest.raises(TypeError, match="Cannot mix xarray.Dataset and xarray.DataTree"):
        zappend(slices(), target_dir=TARGET)


def test_zappend_also_supports_a_data_bearing_root():
    trees = [
        xr.DataTree(make_test_dataset(shape=(1, 2, 3), index=index))
        for index in (0, 1)
    ]

    zappend(trees, target_dir=TARGET)

    with xr.open_zarr(TARGET, zarr_format=2) as dataset:
        assert dataset.sizes["time"] == 2


def test_zappend_force_new_replaces_complete_tree():
    zappend(
        [make_test_datatree(0), make_test_datatree(1)], target_dir=TARGET
    )

    zappend([make_test_datatree(3)], target_dir=TARGET, force_new=True)

    for group in ("0", "1"):
        with xr.open_zarr(TARGET, group=group, zarr_format=2) as dataset:
            assert dataset.sizes["time"] == 1
            assert dataset.time.values[0] == make_test_datatree(3)[group].ds.time.values[0]
