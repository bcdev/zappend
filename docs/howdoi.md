# How do I ...

## ... create datacubes from a directory of GeoTIFFs

Files with GeoTIFF format cannot be opened directly by `zappend` unless
you add [rioxarray](https://corteva.github.io/rioxarray/) to your 
Python environment. 

Then write your own [slice source](guide.md#slice-sources) and 
use configuration setting [`slice_source`](config.md#slice_source):

```python
import glob
import numpy as np
import rioxarray as rxr
import xarray as xr
from zappend.api import zappend

def get_dataset_from_geotiff(tiff_path):
    ds = rxr.open_rasterio(tiff_path)
    # Add missing time dimension
    slice_time = get_slice_time(tiff_path)  
    slice_ds = ds.expand_dims("time", axis=0)
    slice_ds.coords["time"] = xr.Dataset(np.array([slice_time]), dims="time")
    try:
        yield slice_ds
    finally:
        ds.close()

zappend(sorted(glob.glob("inputs/*.tif")),
        slice_source=get_dataset_from_geotiff,
        target_dir="output/tif-cube.zarr")
```

In the example above, function `get_slice_time()` returns the time label
of a given GeoTIFF file as a value of type `np.datetime64`.

## ... create datacubes from datasets without append dimension

`zappend` expects the append dimension to exist in slice datasets and 
expects that at least one variable exists that makes use of that dimension. 
For example, if you are appending spatial 2-d images with dimensions x and y 
along a dimension time, you need to first expand the images into the time 
dimension. Here the 2-d image dataset is called `image_ds` and `slice_time` 
is its associated time value of type `np.datetime64`.

```python
slice_ds = image_ds.expand_dims("time", axis=0)
slice_ds.coords["time"] = xr.Dataset(np.array([slice_time]), dims="time")
```

See also [How do I create datacubes from a directory of GeoTIFFs](#create-datacubes-from-a-directory-of-geotiffs) 
above. 

## ... dynamically update global metadata attributes

Refer to section about [target attributes](guide.md#attributes)
in the user guide. 

## ... find out what is limiting the performance

Use the [logging](guide.md#logging) configuration see which processing steps
use most of the time.
Use the [profiling](guide.md#profiling) configuration to inspect in more 
detail which parts of the processing are the bottlenecks.

## ... write a log file

Use the following [logging](guide.md#logging) configuration:

```json
{
    "logging": {
        "version": 1,
        "formatters": {
            "normal": {
                "format": "%(asctime)s %(levelname)s %(message)s",
                "style": "%"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "normal"
            }, 
            "file": {
                "class": "logging.FileHandler",
                "formatter": "normal", 
                "filename": "zappend.log",
                "mode": "w",
                "encoding": "utf-8"
            }

        },
        "loggers": {
            "zappend": {
                "level": "INFO",
                "handlers": ["console", "file"]
            }
        }
    }
}
```

## ... address common errors

### Error `Target parent directory does not exist`

For security reasons, `zappend` does not create target directories 
automatically. You should make sure the parent directory exists before 
calling `zappend`.

### Error `Target is locked`

In this case the target lock file still exists, which means that a former 
rollback did not complete nominally. You can no longer trust the integrity of 
any existing target dataset. The recommended way is to remove the lock file 
and any target datasets artifact. You can do that manually or use the 
configuration setting `force_new`.

### Error `Append dimension 'foo' not found in dataset`

Refer to [How do I create datacubes from datasets without append dimension](#create-datacubes-from-datasets-without-append-dimension).
