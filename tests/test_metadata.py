# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

import math
import unittest
from typing import Any

import numcodecs
import numpy as np
import pytest
import xarray as xr

from zappend.config import Config
from zappend.metadata import DatasetMetadata


def make_config(config_dict: dict[str, Any]) -> Config:
    return Config({"target_dir": "memory://target.zarr", **config_dict})


class DatasetMetadataDimsTest(unittest.TestCase):
    def test_dims_without_fixed_dims_given(self):
        ds = xr.Dataset({"a": xr.DataArray(np.zeros((2, 3, 4)), dims=("z", "y", "x"))})
        self.assertEqual(
            {"z": 2, "y": 3, "x": 4},
            DatasetMetadata.from_dataset(ds, make_config({"append_dim": "z"})).sizes,
        )

    def test_dims_with_fixed_dims_given(self):
        ds = xr.Dataset({"a": xr.DataArray(np.zeros((2, 3, 4)), dims=("z", "y", "x"))})
        self.assertEqual(
            {"z": 2, "y": 3, "x": 4},
            DatasetMetadata.from_dataset(
                ds, make_config({"append_dim": "z", "fixed_dims": {"y": 3, "x": 4}})
            ).sizes,
        )

    # noinspection PyMethodMayBeStatic
    def test_it_raises_if_append_dim_not_found(self):
        ds = xr.Dataset(
            {"a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x"))}
        )
        with pytest.raises(
            ValueError, match="Append dimension 'z' not found in dataset"
        ):
            DatasetMetadata.from_dataset(ds, make_config({"append_dim": "z"}))

    # noinspection PyMethodMayBeStatic
    def test_it_raises_if_append_dim_is_fixed(self):
        ds = xr.Dataset(
            {"a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x"))}
        )
        with pytest.raises(
            ValueError, match="Size of append dimension 'time'" " must not be fixed"
        ):
            DatasetMetadata.from_dataset(
                ds,
                make_config(
                    {"fixed_dims": {"time": 2, "y": 3, "x": 4}, "append_dim": "time"}
                ),
            )

    # noinspection PyMethodMayBeStatic
    def test_it_raises_if_fixed_dim_not_found_in_ds(self):
        ds = xr.Dataset(
            {"a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x"))}
        )
        with pytest.raises(
            ValueError, match="Fixed dimension 'z' not found in dataset"
        ):
            DatasetMetadata.from_dataset(
                ds, make_config({"fixed_dims": {"y": 3, "z": 4}, "append_dim": "time"})
            )

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_wrong_size_found_in_ds(self):
        ds = xr.Dataset(
            {"a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x"))}
        )
        with pytest.raises(
            ValueError,
            match="Wrong size for fixed dimension 'x'"
            " in dataset: expected 5, found 4",
        ):
            DatasetMetadata.from_dataset(
                ds, make_config({"fixed_dims": {"y": 3, "x": 5}, "append_dim": "time"})
            )


class DatasetMetadataVariablesTest(unittest.TestCase):
    def test_add_missing_variables(self):
        self.assertEqual(
            {
                "attrs": {},
                "sizes": {"time": 2, "x": 4, "y": 3},
                "variables": {
                    "a": {
                        "attrs": {},
                        "dims": ("time", "y", "x"),
                        "encoding": {},
                        "shape": (2, 3, 4),
                    },
                    "b": {
                        "attrs": {},
                        "dims": ("time", "y", "x"),
                        "encoding": {},
                        "shape": (2, 3, 4),
                    },
                },
            },
            DatasetMetadata.from_dataset(
                xr.Dataset(
                    {
                        "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                        "b": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                    }
                ),
                make_config({"variables": {"a": {"dims": ["time", "y", "x"]}}}),
            ).to_dict(),
        )

    def test_merge_variable_metadata(self):
        config_vars = {
            "a": {"encoding": {"dtype": "uint16"}, "attrs": {"title": "A"}},
            "b": {"encoding": {"dtype": "int32"}, "attrs": {"title": "B"}},
        }
        ds = xr.Dataset(
            {
                "a": xr.DataArray(
                    np.zeros((2, 3, 4)), dims=("time", "y", "x"), attrs={"units": "m/s"}
                ),
                "b": xr.DataArray(
                    np.zeros((2, 3, 4)),
                    dims=("time", "y", "x"),
                    attrs={"units": "m/s^2"},
                ),
            }
        )
        ds.a.encoding.update(scale_factor=0.001)
        ds.b.encoding.update(add_offset=0.5)
        self.assertEqual(
            {
                "attrs": {},
                "sizes": {"time": 2, "x": 4, "y": 3},
                "variables": {
                    "a": {
                        "attrs": {"title": "A", "units": "m/s"},
                        "dims": ("time", "y", "x"),
                        "encoding": {
                            "dtype": np.dtype("uint16"),
                            "scale_factor": 0.001,
                        },
                        "shape": (2, 3, 4),
                    },
                    "b": {
                        "attrs": {"title": "B", "units": "m/s^2"},
                        "dims": ("time", "y", "x"),
                        "encoding": {"add_offset": 0.5, "dtype": np.dtype("int32")},
                        "shape": (2, 3, 4),
                    },
                },
            },
            DatasetMetadata.from_dataset(
                ds, make_config({"variables": config_vars})
            ).to_dict(),
        )

    def test_move_variable_encoding_from_attrs(self):
        self.assertEqual(
            {
                "attrs": {},
                "sizes": {"time": 2, "x": 4, "y": 3},
                "variables": {
                    "a": {
                        "attrs": {},
                        "dims": ("time", "y", "x"),
                        "encoding": {"_FillValue": 999},
                        "shape": (2, 3, 4),
                    },
                    "b": {
                        "attrs": {},
                        "dims": ("time", "y", "x"),
                        "encoding": {"_FillValue": -1},
                        "shape": (2, 3, 4),
                    },
                },
            },
            DatasetMetadata.from_dataset(
                xr.Dataset(
                    {
                        "a": xr.DataArray(
                            np.zeros((2, 3, 4)),
                            dims=("time", "y", "x"),
                            attrs={"_FillValue": 999},
                        ),
                        "b": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                    }
                ),
                make_config(
                    {
                        "variables": {
                            "a": {"dims": ["time", "y", "x"]},
                            "b": {
                                "dims": ["time", "y", "x"],
                                "attrs": {"_FillValue": -1},
                            },
                        }
                    }
                ),
            ).to_dict(),
        )

    def test_variable_defaults(self):
        self.assertEqual(
            {
                "attrs": {},
                "sizes": {"time": 2, "x": 4, "y": 3},
                "variables": {
                    "a": {
                        "attrs": {},
                        "dims": ("time", "y", "x"),
                        "encoding": {
                            "chunks": (16, 3, 4),
                            "dtype": np.dtype("float32"),
                        },
                        "shape": (2, 3, 4),
                    },
                    "b": {
                        "attrs": {},
                        "dims": ("time", "y", "x"),
                        "encoding": {
                            "chunks": (16, 3, 4),
                            "dtype": np.dtype("float64"),
                        },
                        "shape": (2, 3, 4),
                    },
                },
            },
            DatasetMetadata.from_dataset(
                xr.Dataset(
                    {
                        "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                        "b": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                    }
                ),
                make_config(
                    {
                        "variables": {
                            "*": {"encoding": {"chunks": [16, 3, 4]}},
                            "a": {"encoding": {"dtype": "float32"}},
                            "b": {"encoding": {"dtype": "float64"}},
                        }
                    }
                ),
            ).to_dict(),
        )

    def test_variable_encoding_from_netcdf(self):
        a = xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x"))
        a.encoding.update(chunksizes=(16, 2, 2))  # turned into "chunks"
        b = xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x"))
        b.encoding.update(contiguous=True, endian="big")  # logs warning
        self.assertEqual(
            {
                "attrs": {},
                "sizes": {"time": 2, "x": 4, "y": 3},
                "variables": {
                    "a": {
                        "attrs": {},
                        "dims": ("time", "y", "x"),
                        "encoding": {"chunks": (16, 2, 2)},
                        "shape": (2, 3, 4),
                    },
                    "b": {
                        "attrs": {},
                        "dims": ("time", "y", "x"),
                        "encoding": {},
                        "shape": (2, 3, 4),
                    },
                },
            },
            DatasetMetadata.from_dataset(
                xr.Dataset(
                    {
                        "a": a,
                        "b": b,
                    }
                ),
                make_config({}),
            ).to_dict(),
        )

    def test_variable_encoding_can_deal_with_chunk_size_none(self):
        # See https://github.com/bcdev/zappend/issues/77
        a = xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x"))
        b = xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x"))
        self.assertEqual(
            {
                "attrs": {},
                "sizes": {"time": 2, "x": 4, "y": 3},
                "variables": {
                    "a": {
                        "attrs": {},
                        "dims": ("time", "y", "x"),
                        "encoding": {"chunks": (1, 3, 4)},
                        "shape": (2, 3, 4),
                    },
                    "b": {
                        "attrs": {},
                        "dims": ("time", "y", "x"),
                        "encoding": {"chunks": (2, 2, 3)},
                        "shape": (2, 3, 4),
                    },
                },
            },
            DatasetMetadata.from_dataset(
                xr.Dataset(
                    {
                        "a": a,
                        "b": b,
                    }
                ),
                make_config(
                    {
                        "variables": {
                            "a": {"encoding": {"chunks": [1, None, None]}},
                            "b": {"encoding": {"chunks": [None, 2, 3]}},
                        },
                    }
                ),
            ).to_dict(),
        )

    def test_variable_encoding_normalisation(self):
        def normalize(k, v):
            metadata = DatasetMetadata.from_dataset(
                xr.Dataset(
                    {
                        "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                    }
                ),
                make_config({"variables": {"a": {"encoding": {k: v}}}}),
            )
            return getattr(metadata.variables["a"].encoding, k)

        self.assertEqual(np.dtype("int32"), normalize("dtype", "int32"))
        dtype = np.dtype("int32")
        self.assertIs(dtype, normalize("dtype", dtype))
        self.assertIs(None, normalize("chunks", None))
        self.assertIs(None, normalize("chunks", ()))
        self.assertEqual((1, 2, 3), normalize("chunks", [1, 2, 3]))
        self.assertEqual((1, 3), normalize("chunks", ((1, 1, 1), (3, 3, 2))))
        self.assertTrue(math.isnan(normalize("fill_value", "NaN")))
        self.assertEqual(3.0, normalize("add_offset", "3.0"))
        self.assertEqual(0.01, normalize("scale_factor", 0.01))
        self.assertIs(None, normalize("compressor", None))
        self.assertIs(None, normalize("compressor", {}))
        self.assertIsInstance(
            normalize(
                "compressor",
                {
                    "id": "blosc",
                    "cname": "lz4",
                    "clevel": 5,
                    "blocksize": 0,
                    "shuffle": 1,
                },
            ),
            numcodecs.Blosc,
        )
        compressor = numcodecs.Blosc()
        self.assertIs(compressor, normalize("compressor", compressor))
        self.assertIs(None, normalize("filters", None))
        self.assertIs(None, normalize("filters", []))
        filters = normalize(
            "filters",
            [{"id": "delta", "dtype": "int8"}, {"id": "delta", "dtype": "int16"}],
        )
        self.assertIsInstance(filters, list)
        self.assertEqual(2, len(filters))
        self.assertIsInstance(filters[0], numcodecs.Delta)
        self.assertIsInstance(filters[1], numcodecs.Delta)

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_unspecified_variable(self):
        with pytest.raises(
            ValueError,
            match="The following variables are neither"
            " configured nor contained in the dataset:"
            " b, c",
        ):
            DatasetMetadata.from_dataset(
                xr.Dataset(
                    {
                        "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                    }
                ),
                make_config(
                    {
                        "included_variables": ["a", "b", "c"],
                        "variables": {"a": {"dims": ["z", "y", "x"]}},
                    }
                ),
            )

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_wrong_size_found_in_ds(self):
        with pytest.raises(
            ValueError,
            match="Dimension mismatch for variable 'a':"
            " expected \\('z', 'y', 'x'\\),"
            " found \\('time', 'y', 'x'\\) in dataset",
        ):
            DatasetMetadata.from_dataset(
                xr.Dataset(
                    {
                        "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                    }
                ),
                make_config({"variables": {"a": {"dims": ["z", "y", "x"]}}}),
            )

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_missing_variable_dims(self):
        with pytest.raises(ValueError, match="Missing dimensions of variable 'b'"):
            DatasetMetadata.from_dataset(
                xr.Dataset(
                    {
                        "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                    }
                ),
                make_config(
                    {
                        "variables": {
                            "a": {"dims": ["time", "y", "x"]},
                            "b": {},
                        }
                    }
                ),
            )

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_dim_not_found(self):
        with pytest.raises(
            ValueError, match="Dimension 'Y' of" " variable 'b' not found in dataset"
        ):
            DatasetMetadata.from_dataset(
                xr.Dataset(
                    {
                        "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                    }
                ),
                make_config(
                    {
                        "variables": {
                            "a": {"dims": ["time", "y", "x"]},
                            "b": {"dims": ["time", "Y", "x"]},
                        }
                    }
                ),
            )

    # noinspection PyMethodMayBeStatic
    def test_it_raises_on_missing_dtype_or_fill_value(self):
        with pytest.raises(
            ValueError,
            match="Missing 'dtype' in encoding configuration" " of variable 'b'",
        ):
            DatasetMetadata.from_dataset(
                xr.Dataset(
                    {
                        "a": xr.DataArray(np.zeros((2, 3, 4)), dims=("time", "y", "x")),
                    }
                ),
                make_config(
                    {
                        "variables": {
                            "a": {"dims": ["time", "y", "x"]},
                            "b": {"dims": ["time", "y", "x"]},
                        },
                    }
                ),
            )


class DatasetMetadataSliceCompatibilityTest(unittest.TestCase):
    # noinspection PyMethodMayBeStatic
    def test_compatible(self):
        target_md = DatasetMetadata.from_dataset(
            xr.Dataset(
                {
                    "a": xr.DataArray(np.zeros((12, 3, 4)), dims=("time", "y", "x")),
                    "b": xr.DataArray(np.zeros((12, 3, 4)), dims=("time", "y", "x")),
                }
            ),
            make_config({}),
        )
        slice_md = DatasetMetadata.from_dataset(
            xr.Dataset(
                {
                    "a": xr.DataArray(np.zeros((1, 3, 4)), dims=("time", "y", "x")),
                    "b": xr.DataArray(np.zeros((1, 3, 4)), dims=("time", "y", "x")),
                }
            ),
            make_config({}),
        )

        # Should not raise
        target_md.assert_compatible_slice(slice_md, "time")

    # noinspection PyMethodMayBeStatic
    def test_raise_on_missing_dimension(self):
        target_md = DatasetMetadata.from_dataset(
            xr.Dataset(
                {
                    "a": xr.DataArray(np.zeros((12, 3, 4)), dims=("time", "y", "x")),
                    "b": xr.DataArray(np.zeros((12, 3, 4)), dims=("time", "y", "x")),
                }
            ),
            make_config({}),
        )
        slice_md = DatasetMetadata.from_dataset(
            xr.Dataset(
                {
                    "a": xr.DataArray(np.zeros((1, 3)), dims=("time", "y")),
                    "b": xr.DataArray(np.zeros((1, 3)), dims=("time", "y")),
                }
            ),
            make_config({}),
        )

        with pytest.raises(ValueError, match="Missing dimension 'x' in slice dataset"):
            target_md.assert_compatible_slice(slice_md, "time")

    # noinspection PyMethodMayBeStatic
    def test_raise_on_wrong_dimension_size(self):
        target_md = DatasetMetadata.from_dataset(
            xr.Dataset(
                {
                    "a": xr.DataArray(np.zeros((12, 3, 4)), dims=("time", "y", "x")),
                    "b": xr.DataArray(np.zeros((12, 3, 4)), dims=("time", "y", "x")),
                }
            ),
            make_config({}),
        )
        slice_md = DatasetMetadata.from_dataset(
            xr.Dataset(
                {
                    "a": xr.DataArray(np.zeros((12, 4, 4)), dims=("time", "y", "x")),
                    "b": xr.DataArray(np.zeros((12, 4, 4)), dims=("time", "y", "x")),
                }
            ),
            make_config({}),
        )

        with pytest.raises(
            ValueError,
            match="Wrong size for dimension 'y'"
            " in slice dataset: expected 3, but found 4",
        ):
            target_md.assert_compatible_slice(slice_md, "time")

    # noinspection PyMethodMayBeStatic
    def test_raise_on_wrong_var_dimensions(self):
        target_md = DatasetMetadata.from_dataset(
            xr.Dataset(
                {
                    "a": xr.DataArray(np.zeros((12, 3, 4)), dims=("time", "y", "x")),
                    "b": xr.DataArray(np.zeros((12, 3, 4)), dims=("time", "y", "x")),
                }
            ),
            make_config({}),
        )
        slice_md = DatasetMetadata.from_dataset(
            xr.Dataset(
                {
                    "a": xr.DataArray(np.zeros((1, 3)), dims=("time", "y")),
                    "b": xr.DataArray(np.zeros((1, 3, 4)), dims=("time", "y", "x")),
                }
            ),
            make_config({}),
        )

        with pytest.raises(
            ValueError,
            match="Wrong dimensions for variable 'a' in"
            " slice dataset:"
            " expected \\('time', 'y', 'x'\\),"
            " but found \\('time', 'y'\\)",
        ):
            target_md.assert_compatible_slice(slice_md, "time")
