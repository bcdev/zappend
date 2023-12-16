import time
import uuid
from abc import abstractmethod, ABC

import fsspec
import xarray as xr
from .context import Context


def open_slice_zarr(ctx: Context, slice_obj: str | xr.Dataset) -> "SliceZarr":
    if isinstance(slice_obj, xr.Dataset):
        return InMemorySliceZarr(ctx, slice_obj)
    if isinstance(slice_obj, str):
        return PersistentSliceZarr(ctx, slice_obj)
    raise TypeError("slice_obj must be a str or xarray.Dataset")


class SliceZarr(ABC):

    def __init__(self, ctx: Context):
        self.ctx = ctx

    def __del__(self):
        """Overridden to call ``dispose()``."""
        self.dispose()

    def __enter__(self):
        return self.prepare()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dispose()

    def dispose(self):
        """Dispose this slice Zarr.
        This should include cleaning up of used resources.
        """
        if hasattr(self, "ctx"):
            self.ctx = None
            del self.ctx

    @abstractmethod
    def prepare(self) -> tuple[fsspec.AbstractFileSystem, str]:
        """Prepare this slice Zarr so it can be used.

        :return: a tuple (filesystem, path) that are used to
            access the Zarr.
        """


class PersistentSliceZarr(SliceZarr):
    """
    A slice Zarr that is persisted in some filesystem.

    :param ctx: Processing context
    :param slice_path: Path of the persistent slice
    """

    def __init__(self, ctx: Context, slice_path: str):
        super().__init__(ctx)
        self.slice_path = slice_path
        self.slice_fs, self.slice_fs_path = \
            self.ctx.get_slice_fs(self.slice_path)
        self.slice_ds: xr.Dataset | None = None
        self.slice_zarr: SliceZarr | None = None

    def prepare(self):
        interval, timeout = self.ctx.slice_polling
        if timeout:
            t0 = time.monotonic()
            while (time.monotonic() - t0) < timeout:
                try:
                    self.slice_ds = self.open_dataset()
                except OSError:
                    time.sleep(interval)
        else:
            self.slice_ds = self.open_dataset()

        if self.is_valid_zarr():
            return self.slice_fs, self.slice_fs_path
        else:
            self.slice_zarr = InMemorySliceZarr(self.ctx, self.slice_ds)
            return self.slice_zarr.prepare()

    def dispose(self):
        """Dispose"""
        if hasattr(self, "slice_zarr") and self.slice_zarr is not None:
            self.slice_zarr.dispose()
            self.slice_zarr = None
            del self.slice_zarr
        if hasattr(self, "slice_ds") and self.slice_ds is not None:
            self.slice_ds.close()
            self.slice_ds = None
            del self.slice_ds
        super().dispose()

    def is_valid_zarr(self):
        pass

    def open_dataset(self) -> xr.Dataset:
        return xr.open_dataset(self.slice_path,
                               storage_options=self.ctx.slice_fs_options,
                               decode_cf=False)

    def wait_for_arrival(self):
        pass


class InMemorySliceZarr(SliceZarr):
    """
    A slice Zarr that is available in-memory only as a xarray dataset.

    :param ctx: Processing context
    :param slice_ds: The in-memory dataset
    """

    def __init__(self, ctx: Context, slice_ds: xr.Dataset):
        super().__init__(ctx)
        self.slice_ds = slice_ds
        self.temp_path = None

    def prepare(self):
        self.temp_path = f"{self.ctx.temp_path}/{uuid.uuid4()}.zarr"
        to_zarr(self.ctx, self.slice_ds, self.temp_path)
        return self.ctx.temp_fs, self.temp_path

    def dispose(self):
        """Dispose"""
        if hasattr(self, "temp_path") and self.temp_path is not None:
            self.remove_temp_files()
            self.temp_path = None
            del self.temp_path
        super().dispose()

    def remove_temp_files(self):
        if self.temp_path is not None \
                and self.ctx.temp_fs.isdir(self.temp_path):
            self.ctx.temp_fs.rm(self.temp_path, recursive=True)


def to_zarr(ctx: Context, ds: xr.Dataset, path: str):
    zarr_version = ctx.config.get("zarr_version", 2)
    variables = ctx.config.get("variables", {})
    encoding = {var_name: var_info["encoding"]
                for var_name, var_info in variables.items()
                if "encoding" in var_info}
    ds.to_zarr(ctx.temp_fs.get_mapper(root=path, create=True),
               write_empty_chunks=False,
               encoding=encoding,
               zarr_version=zarr_version)
