# Copyright © 2024, 2025 Brockmann Consult and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import json
import unittest

import pytest
import xarray as xr

from tests.helpers import clear_memory_fs, make_test_dataset
from zappend.contrib import write_levels
from zappend.contrib.levels import get_variables_config
from zappend.fsutil import FileObj

try:
    # noinspection PyUnresolvedReferences
    import xcube
except ImportError:
    xcube = None


class GetVariablesConfigTest(unittest.TestCase):
    def test_no_variables_given(self):
        dataset = make_test_dataset()
        variables = get_variables_config(dataset, dict(x=512, y=256, time=1))
        self.assertEqual(
            {
                "x": {"dims": ["x"], "encoding": {"chunks": None}},
                "y": {"dims": ["y"], "encoding": {"chunks": None}},
                "time": {"dims": ["time"], "encoding": {"chunks": None}},
                "chl": {
                    "dims": ["time", "y", "x"],
                    "encoding": {"chunks": [1, 256, 512]},
                },
                "tsm": {
                    "dims": ["time", "y", "x"],
                    "encoding": {"chunks": [1, 256, 512]},
                },
            },
            variables,
        )

    def test_variables_given(self):
        dataset = make_test_dataset()
        variables = get_variables_config(
            dataset,
            dict(x=512, y=256, time=1),
            variables={
                "time": {"encoding": {"chunks": [3]}},
                "chl": {"encoding": {"chunks": [3, 100, 100]}},
                "tsm": {"encoding": {"dtype": "uint16"}},
            },
        )
        self.assertEqual(
            {
                "x": {"dims": ["x"], "encoding": {"chunks": None}},
                "y": {"dims": ["y"], "encoding": {"chunks": None}},
                "time": {"dims": ["time"], "encoding": {"chunks": [3]}},
                "chl": {
                    "dims": ["time", "y", "x"],
                    "encoding": {"chunks": [3, 100, 100]},
                },
                "tsm": {
                    "dims": ["time", "y", "x"],
                    "encoding": {"chunks": [1, 256, 512], "dtype": "uint16"},
                },
            },
            variables,
        )


source_path = "memory://source.zarr"
target_path = "memory://target.levels"


# noinspection PyMethodMayBeStatic
@unittest.skipIf(xcube is None, reason="xcube is not installed")
class WriteLevelsArgsTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()
        make_test_dataset(
            uri=source_path,
            dims=("time", "lat", "lon"),
            shape=(3, 1024, 2048),
            chunks=(1, 128, 256),
        )

    def test_target_path_not_given(self):
        with pytest.raises(
            ValueError,
            match="missing 'target_path' argument",
        ):
            write_levels(source_path=source_path)

    def test_target_dir_and_target_path_given(self):
        with pytest.raises(
            ValueError,
            match="either 'target_dir' or 'target_path' can be given, not both",
        ):
            write_levels(
                source_path=source_path,
                target_path=target_path,
                target_dir=target_path,
            )

    def test_config_given(self):
        with pytest.raises(
            TypeError,
            match="write_levels\\(\\) got an unexpected keyword argument 'config'",
        ):
            write_levels(
                source_path=source_path,
                target_path=target_path,
                config={"dry_run": True},
            )

    def test_dry_run_and_use_saved_levels_given(self):
        with pytest.raises(
            FileNotFoundError,
            match="Target parent directory does not exist: /target.levels",
        ):
            with pytest.warns(
                UserWarning,
                match="'use_saved_levels' argument is not applicable if dry_run=True",
            ):
                write_levels(
                    source_path=source_path,
                    target_path=target_path,
                    dry_run=True,
                    use_saved_levels=True,
                )

    def test_source_path_and_source_ds_not_given(self):
        with pytest.raises(
            TypeError,
            match="'source_ds' argument must be of type 'xarray.Dataset',"
            " but was 'NoneType'",
        ):
            write_levels(
                target_path=target_path,
            )

    def test_source_path_not_given_and_link_level_zero_is_true(self):
        with pytest.raises(
            ValueError,
            match="'source_path' argument must be provided"
            " if 'link_level_zero' is used",
        ):
            write_levels(
                source_ds=xr.Dataset(), target_path=target_path, link_level_zero=True
            )

    def test_source_append_offset_not_int(self):
        with pytest.raises(
            TypeError,
            match="'source_append_offset' argument must be of type 'int',"
            " but was 'str'",
        ):
            # noinspection PyTypeChecker
            write_levels(
                source_ds=xr.open_zarr("memory://source.zarr"),
                source_append_offset="2",
                target_path=target_path,
            )

    def test_source_append_offset_out_of_range(self):
        with pytest.raises(
            ValueError,
            match="'source_append_offset' argument must be >=0 and <3, but was 13",
        ):
            # noinspection PyTypeChecker
            write_levels(
                source_ds=xr.open_zarr("memory://source.zarr"),
                source_append_offset=13,
                target_path=target_path,
            )


@unittest.skipIf(xcube is None, reason="xcube is not installed")
class WriteLevelsTest(unittest.TestCase):
    def setUp(self):
        clear_memory_fs()

    # noinspection PyMethodMayBeStatic

    def test_force_new(self):
        make_test_dataset(
            uri=source_path,
            dims=("time", "lat", "lon"),
            shape=(3, 1024, 2048),
            chunks=(1, 128, 256),
        )

        target_dir = FileObj("memory://target.levels")
        self.assertFalse(target_dir.exists())
        target_dir.mkdir()
        (target_dir / "0.zarr").mkdir()
        (target_dir / "0.zarr" / ".zgroup").write("{}")
        self.assertTrue(target_dir.exists())

        write_levels(
            source_path=source_path, target_path=target_dir.uri, force_new=True
        )

        self.assertTrue(target_dir.exists())

    def test_default_x_y_with_crs(self):
        make_test_dataset(
            uri=source_path,
            dims=("time", "y", "x"),
            shape=(3, 1024, 2048),
            chunks=(1, 128, 256),
            crs="EPSG:4326",
        )

        target_dir = FileObj("memory://target.levels")
        self.assertFalse(target_dir.exists())

        write_levels(source_path=source_path, target_path=target_dir.uri)

        self.assertTrue(target_dir.exists())

        levels_file = target_dir.for_path(".zlevels")
        self.assertTrue(levels_file.exists())
        levels_info = json.loads(levels_file.read())
        self.assertEqual(
            {
                "version": "1.0",
                "num_levels": 4,
                "agg_methods": {"chl": "mean", "tsm": "mean"},
                "use_saved_levels": False,
            },
            levels_info,
        )

        self.assert_level(target_dir.uri + "/0.zarr", 0, has_crs=True)
        self.assert_level(target_dir.uri + "/1.zarr", 1, has_crs=True)
        self.assert_level(target_dir.uri + "/2.zarr", 2, has_crs=True)
        self.assert_level(target_dir.uri + "/3.zarr", 3, has_crs=True)

    def test_default_lon_lat_no_crs(self):
        make_test_dataset(
            uri=source_path,
            dims=("time", "lat", "lon"),
            shape=(3, 1024, 2048),
            chunks=(1, 128, 256),
        )

        target_dir = FileObj("memory://target.levels")
        self.assertFalse(target_dir.exists())

        write_levels(source_path=source_path, target_path=target_dir.uri)

        self.assertTrue(target_dir.exists())

        levels_file = target_dir.for_path(".zlevels")
        self.assertTrue(levels_file.exists())
        levels_info = json.loads(levels_file.read())
        self.assertEqual(
            {
                "version": "1.0",
                "num_levels": 4,
                "agg_methods": {"chl": "mean", "tsm": "mean"},
                "use_saved_levels": False,
            },
            levels_info,
        )

        xy_dims = "lon", "lat"
        self.assert_level(target_dir.uri + "/0.zarr", 0, xy_dims=xy_dims)
        self.assert_level(target_dir.uri + "/1.zarr", 1, xy_dims=xy_dims)
        self.assert_level(target_dir.uri + "/2.zarr", 2, xy_dims=xy_dims)
        self.assert_level(target_dir.uri + "/3.zarr", 3, xy_dims=xy_dims)

    def test_default_lon_lat_no_crs_from_source_ds(self):
        source_ds = make_test_dataset(
            uri=source_path,
            dims=("time", "lat", "lon"),
            shape=(3, 1024, 2048),
            chunks=(1, 128, 256),
        )

        target_dir = FileObj("memory://target.levels")
        self.assertFalse(target_dir.exists())

        write_levels(source_ds=source_ds, target_path=target_dir.uri)

        self.assertTrue(target_dir.exists())

        levels_file = target_dir.for_path(".zlevels")
        self.assertTrue(levels_file.exists())
        levels_info = json.loads(levels_file.read())
        self.assertEqual(
            {
                "version": "1.0",
                "num_levels": 4,
                "agg_methods": {"chl": "mean", "tsm": "mean"},
                "use_saved_levels": False,
            },
            levels_info,
        )

        xy_dims = "lon", "lat"
        self.assert_level(target_dir.uri + "/0.zarr", 0, xy_dims=xy_dims)
        self.assert_level(target_dir.uri + "/1.zarr", 1, xy_dims=xy_dims)
        self.assert_level(target_dir.uri + "/2.zarr", 2, xy_dims=xy_dims)
        self.assert_level(target_dir.uri + "/3.zarr", 3, xy_dims=xy_dims)

    def test_default_lon_lat_no_crs_from_source_ds_with_offset(self):
        source_ds = make_test_dataset(
            uri=source_path,
            dims=("time", "lat", "lon"),
            shape=(10, 1024, 2048),
            chunks=(1, 128, 256),
        )

        target_dir = FileObj("memory://target.levels")
        self.assertFalse(target_dir.exists())

        write_levels(
            source_ds=source_ds,
            source_append_offset=7,
            target_path=target_dir.uri,
        )

        self.assertTrue(target_dir.exists())

        levels_file = target_dir.for_path(".zlevels")
        self.assertTrue(levels_file.exists())
        levels_info = json.loads(levels_file.read())
        self.assertEqual(
            {
                "version": "1.0",
                "num_levels": 4,
                "agg_methods": {"chl": "mean", "tsm": "mean"},
                "use_saved_levels": False,
            },
            levels_info,
        )

        xy_dims = "lon", "lat"
        self.assert_level(target_dir.uri + "/0.zarr", 0, xy_dims=xy_dims)
        self.assert_level(target_dir.uri + "/1.zarr", 1, xy_dims=xy_dims)
        self.assert_level(target_dir.uri + "/2.zarr", 2, xy_dims=xy_dims)
        self.assert_level(target_dir.uri + "/3.zarr", 3, xy_dims=xy_dims)

    def test_link_level_zero(self):
        source_dir = FileObj("memory://source.zarr")
        make_test_dataset(
            uri=source_dir.uri,
            dims=("time", "y", "x"),
            shape=(3, 1024, 2048),
            chunks=(1, 128, 256),
            crs="EPSG:4326",
        )

        target_dir = FileObj("memory://target.levels")
        self.assertFalse(target_dir.exists())

        write_levels(
            source_path=source_dir.uri,
            target_path=target_dir.uri,
            link_level_zero=True,
        )

        self.assertTrue(target_dir.exists())

        levels_file = target_dir.for_path(".zlevels")
        self.assertTrue(levels_file.exists())
        levels_info = json.loads(levels_file.read())
        self.assertEqual(
            {
                "version": "1.0",
                "num_levels": 4,
                "agg_methods": {"chl": "mean", "tsm": "mean"},
                "use_saved_levels": False,
            },
            levels_info,
        )

        level_zero_file = target_dir.for_path("0.link")
        self.assertTrue(level_zero_file.exists())
        self.assertEqual(b"../source.zarr", level_zero_file.read())
        self.assert_level(target_dir.uri + "/1.zarr", 1, has_crs=True)
        self.assert_level(target_dir.uri + "/2.zarr", 2, has_crs=True)
        self.assert_level(target_dir.uri + "/3.zarr", 3, has_crs=True)

    def test_link_level_zero_use_saved_levels(self):
        source_dir = FileObj("memory://source.zarr")
        make_test_dataset(
            uri=source_dir.uri,
            dims=("time", "lat", "lon"),
            shape=(3, 1024, 2048),
            chunks=(1, 128, 256),
        )

        target_dir = FileObj("memory://target.levels")
        self.assertFalse(target_dir.exists())

        write_levels(
            source_path=source_dir.uri,
            target_path=target_dir.uri,
            link_level_zero=True,
            use_saved_levels=True,
        )

        self.assertTrue(target_dir.exists())

        levels_file = target_dir.for_path(".zlevels")
        self.assertTrue(levels_file.exists())
        levels_info = json.loads(levels_file.read())
        self.assertEqual(
            {
                "version": "1.0",
                "num_levels": 4,
                "agg_methods": {"chl": "mean", "tsm": "mean"},
                "use_saved_levels": True,
            },
            levels_info,
        )

        xy_dims = "lon", "lat"
        level_zero_file = target_dir.for_path("0.link")
        self.assertTrue(level_zero_file.exists())
        self.assertEqual(b"../source.zarr", level_zero_file.read())
        self.assert_level(target_dir.uri + "/1.zarr", 1, xy_dims=xy_dims)
        self.assert_level(target_dir.uri + "/2.zarr", 2, xy_dims=xy_dims)
        self.assert_level(target_dir.uri + "/3.zarr", 3, xy_dims=xy_dims)

    def assert_level(self, uri: str, level: int, xy_dims=("x", "y"), has_crs=False):
        x_dim, y_dim = xy_dims
        dataset = xr.open_zarr(uri)
        z = 2**level
        f = 2 ** (3 - level)
        self.assertEqual({"time": 3, y_dim: 1024 // z, x_dim: 2048 // z}, dataset.sizes)
        self.assertEqual(
            {"time": 3 * (1,), y_dim: f * (128,), x_dim: f * (256,)}, dataset.chunksizes
        )
        self.assertEqual({x_dim, y_dim, "time"}, set(dataset.coords))
        if has_crs:
            self.assertEqual({"chl", "tsm", "crs"}, set(dataset.data_vars))
        else:
            self.assertEqual({"chl", "tsm"}, set(dataset.data_vars))
