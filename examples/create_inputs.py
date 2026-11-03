#  Copyright © 2024-2026 Brockmann Consult and contributors
#  Permissions are hereby granted under the terms of the MIT License:
#  https://opensource.org/licenses/MIT.

from pathlib import Path

import numpy as np
import xarray as xr

INPUT_DIR = Path(__file__).parent / "inputs"
START_TIME = np.datetime64("2024-01-01", "D")


def make_slice(time: np.datetime64, index: int) -> xr.Dataset:
    """Return one small daily slice with a predictable value pattern."""
    lon = np.arange(-179.95, 180, 0.1, dtype="float32")
    lat = np.arange(-89.95, 90, 0.1, dtype="float32")
    data = np.full((1, len(lat), len(lon)), index, dtype="uint8")
    return xr.Dataset(
        data_vars={
            "temperature": (
                ("time", "lat", "lon"),
                data.astype("float32"),
                {"units": "degC", "long_name": "example sea temperature"},
            ),
            "quality_flag": (
                ("time", "lat", "lon"),
                data,
                {"long_name": "example quality flag"},
            ),
        },
        coords={"time": [time], "lat": lat, "lon": lon},
        attrs={"title": "Zappend example slice"},
    ).chunk({"time": 1, "lat": 450, "lon": 450})


def make_zarr_v2_slices(n_slices: int) -> None:
    """Create *n_slices* daily Zarr v2 inputs from :func:`make_slice`."""
    times = START_TIME + np.arange(n_slices) * np.timedelta64(1, "D")
    directory = INPUT_DIR / "zarr-v2"
    directory.mkdir(parents=True, exist_ok=True)
    for index, time in enumerate(times):
        dataset = make_slice(time, index)
        dataset.to_zarr(
            directory / f"slice-{time!s}.zarr",
            mode="w",
            zarr_format=2,
            consolidated=True,
        )


def make_zarr_v3_slices(n_slices: int) -> None:
    """Create *n_slices* daily, sharded Zarr v3 inputs from :func:`make_slice`."""
    times = START_TIME + np.arange(n_slices) * np.timedelta64(1, "D")
    directory = INPUT_DIR / "zarr-v3"
    directory.mkdir(parents=True, exist_ok=True)

    for index, time in enumerate(times):
        # Xarray requires Dask write chunks to align with the outer Zarr v3
        # shard grid. The encoding below still defines 450 by 450 inner
        # chunks inside each 1800 by 3600 shard.
        dataset = make_slice(time, index).chunk({"time": 1, "lat": 1800, "lon": 3600})
        encoding = {
            name: {
                "chunks": (1, 450, 450),
                "shards": (1, 1800, 3600),
            }
            for name in dataset.data_vars
        }
        dataset.to_zarr(
            directory / f"slice-{time!s}.zarr",
            mode="w",
            encoding=encoding,
            zarr_format=3,
            consolidated=False,
        )


def make_netcdf_slices(n_slices: int) -> None:
    """Create *n_slices* daily NetCDF from :func:`make_slice`."""
    times = START_TIME + np.arange(n_slices) * np.timedelta64(1, "D")
    directory = INPUT_DIR / "netcdf"
    directory.mkdir(parents=True, exist_ok=True)

    for index, time in enumerate(times):
        dataset = make_slice(time, index)
        encoding = {name: {"chunksizes": (1, 450, 450)} for name in dataset.data_vars}
        dataset.to_netcdf(
            directory / f"slice-{time!s}.nc",
            engine="h5netcdf",
            encoding=encoding,
        )
