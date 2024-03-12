# Copyright © 2024 Norman Fomferra and contributors
# Permissions are hereby granted under the terms of the MIT License:
# https://opensource.org/licenses/MIT.

from io import StringIO
import json
import unittest

import numpy as np
import pytest
import xarray as xr

from zappend.fsutil.fileobj import FileObj
from .helpers import clear_memory_fs

TEST_ZARR = "memory://test.zarr"


def read_json(path: str) -> dict:
    data = (FileObj(TEST_ZARR) / path).read(mode="rt")
    return json.load(StringIO(data))


def get_v_attrs():
    return read_json("v/.zattrs")


def get_v_meta():
    meta = read_json("v/.zarray")
    meta.pop("dimension_separator", None)  # new in xarray/zarr
    return meta


class XArrayEncodingTest(unittest.TestCase):
    """This test demonstrates some behaviour of xarray when serializing
    a self.ds to Zarr given encoding information via the many possible
    channels, as there are the variable data array, variable attributes,
    variable encoding, or encoding kwargs passed to ``to_zarr()``.
    """

    def setUp(self):
        clear_memory_fs()
        self.assertFalse(FileObj(TEST_ZARR).exists())

        self.ds = xr.Dataset(
            data_vars=dict(v=xr.DataArray(np.ones(100, dtype=np.float64), dims="x")),
            coords=dict(
                x=xr.DataArray(
                    np.linspace(0, 1, 100, endpoint=False, dtype=np.float32), dims="x"
                )
            ),
        )
        self.ds_v_attrs = {"_ARRAY_DIMENSIONS": ["x"]}
        self.ds_v_meta = {
            "zarr_format": 2,
            "shape": [100],
            "chunks": [100],
            "fill_value": "NaN",
            "dtype": "<f8",
            "order": "C",
            "compressor": {
                "id": "blosc",
                "cname": "lz4",
                "clevel": 5,
                "blocksize": 0,
                "shuffle": 1,
            },
            "filters": None,
        }

        self.ds_2 = xr.Dataset(
            data_vars=dict(
                v=xr.DataArray(np.full(100, 2.0, dtype=np.float64), dims="x")
            ),
            coords=dict(
                x=xr.DataArray(
                    np.linspace(1, 2, 100, endpoint=False, dtype=np.float32), dims="x"
                )
            ),
        )

    def test_no_encoding_given(self):
        self.ds.to_zarr(TEST_ZARR)
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual(self.ds_v_meta, get_v_meta())
        # test append
        self.ds_2.to_zarr(TEST_ZARR, mode="a", append_dim="x")
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual(
            {**self.ds_v_meta, "shape": [200], "chunks": [100]}, get_v_meta()
        )

    def test_chunks_in_encoding_kwargs_works(self):
        self.ds.to_zarr(TEST_ZARR, encoding=dict(v=dict(chunks=(20,))))
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual({**self.ds_v_meta, "chunks": [20]}, get_v_meta())
        # test append
        self.ds_2.to_zarr(TEST_ZARR, mode="a", append_dim="x")
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual(
            {**self.ds_v_meta, "shape": [200], "chunks": [20]}, get_v_meta()
        )

    def test_chunks_in_v_encoding_works(self):
        self.ds.v.encoding.update(chunks=(20,))
        self.ds.to_zarr(TEST_ZARR)
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual({**self.ds_v_meta, "chunks": [20]}, get_v_meta())
        # test append
        self.ds_2.to_zarr(TEST_ZARR, mode="a", append_dim="x")
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual(
            {**self.ds_v_meta, "shape": [200], "chunks": [20]}, get_v_meta()
        )

    def test_chunks_in_kwargs_override_v_encoding(self):
        self.ds.v.encoding.update(chunks=(20,))
        self.ds.to_zarr(TEST_ZARR, encoding=dict(v=dict(chunks=(30,))))
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual({**self.ds_v_meta, "chunks": [30]}, get_v_meta())
        # test append
        self.ds_2.to_zarr(TEST_ZARR, mode="a", append_dim="x")
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual(
            {**self.ds_v_meta, "shape": [200], "chunks": [30]}, get_v_meta()
        )

    # noinspection PyMethodMayBeStatic
    def test_overlapping_chunks_in_data_and_encoding_raises(self):
        self.ds["v"] = self.ds.v.chunk(x=10)
        self.ds.v.encoding.update(chunks=(20,))
        with pytest.raises(
            NotImplementedError,
            match="Specified zarr chunks"
            " encoding\\['chunks'\\]=\\(20,\\)"
            " for variable named 'v'",
        ):
            self.ds.to_zarr(TEST_ZARR)

    def test_packing_in_v_attrs_has_wrong_effect(self):
        self.ds.v.attrs.update(scale_factor=0.001, add_offset=5)
        self.ds.v.encoding.update(dtype=np.int16)
        self.ds.to_zarr(TEST_ZARR)
        self.assertEqual(
            {**self.ds_v_attrs, "scale_factor": 0.001, "add_offset": 5}, get_v_attrs()
        )
        self.assertEqual(
            {**self.ds_v_meta, "dtype": "<i2", "fill_value": None}, get_v_meta()
        )
        ds = xr.open_zarr(TEST_ZARR)
        self.assertEqual(np.dtype("float64"), ds.v.dtype)
        self.assertEqual([5.001, 5.001, 5.001], list(ds.v.values[0:3]))

    def test_packing_in_v_encoding_works(self):
        self.ds.v.encoding.update(scale_factor=0.001, add_offset=5, dtype=np.int16)
        self.ds.to_zarr(TEST_ZARR)
        self.assertEqual(
            {**self.ds_v_attrs, "scale_factor": 0.001, "add_offset": 5}, get_v_attrs()
        )
        self.assertEqual(
            {**self.ds_v_meta, "dtype": "<i2", "fill_value": None}, get_v_meta()
        )
        ds = xr.open_zarr(TEST_ZARR)
        self.assertEqual(np.dtype("float64"), ds.v.dtype)
        self.assertEqual([1.0, 1.0, 1.0], list(ds.v.values[0:3]))
        # test append
        self.ds_2.to_zarr(TEST_ZARR, mode="a", append_dim="x")
        self.assertEqual(
            {**self.ds_v_attrs, "scale_factor": 0.001, "add_offset": 5}, get_v_attrs()
        )
        self.assertEqual(
            {
                **self.ds_v_meta,
                "shape": [200],
                "chunks": [100],
                "dtype": "<i2",
                "fill_value": None,
            },
            get_v_meta(),
        )
        ds = xr.open_zarr(TEST_ZARR)
        self.assertEqual(np.dtype("float64"), ds.v.dtype)
        self.assertEqual([1.0, 1.0, 1.0], list(ds.v.values[0:3]))
        self.assertEqual([1.0, 2.0, 2.0], list(ds.v.values[99:102]))

    def test_fill_value_in_v_encoding_has_no_effect(self):
        self.ds.v.encoding.update(fill_value=0.0)
        self.ds.to_zarr(TEST_ZARR)
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual(self.ds_v_meta, get_v_meta())

    def test__FillValue_in_v_encoding_works(self):
        self.ds.v.encoding.update(_FillValue=0.0)
        self.ds.to_zarr(TEST_ZARR)
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual({**self.ds_v_meta, "fill_value": 0.0}, get_v_meta())
        # test append
        self.ds_2.to_zarr(TEST_ZARR, mode="a", append_dim="x")
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual(
            {**self.ds_v_meta, "shape": [200], "fill_value": 0.0}, get_v_meta()
        )

    def test__FillValue_in_v_attrs_works(self):
        self.ds.v.attrs.update(_FillValue=0.0)
        self.ds.to_zarr(TEST_ZARR)
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual({**self.ds_v_meta, "fill_value": 0.0}, get_v_meta())

    def test_append_with_different_dtype_does_not_change_dtype(self):
        self.ds.to_zarr(TEST_ZARR)
        self.ds_2.v.data = np.array(self.ds_2.v.data, dtype="uint8")
        self.ds_2.to_zarr(TEST_ZARR, mode="a", append_dim="x")
        self.assertEqual(self.ds_v_attrs, get_v_attrs())
        self.assertEqual(
            {**self.ds_v_meta, "shape": [200], "dtype": "<f8"}, get_v_meta()
        )
        ds = xr.open_zarr(TEST_ZARR)
        self.assertEqual(np.dtype("float64"), ds.v.dtype)
        self.assertEqual([1.0, 1.0, 1.0], list(ds.v.values[0:3]))
        self.assertEqual([1.0, 2.0, 2.0], list(ds.v.values[99:102]))
