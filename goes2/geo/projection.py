import os
import xarray as xr
from abc import ABC, abstractmethod

from rasterio.enums import Resampling

from goes2.sats import GOES19

import dask.array as da


class Projection(ABC):
    @abstractmethod
    def reproject(self, data: xr.Dataset):
        pass


class WebMercator(Projection):
    def _crop(self, data: xr.Dataset):
        bounds_future = data.rio.bounds()
        minx, miny, maxx, maxy = da.compute(*bounds_future)

        size = min(maxx - minx, maxy - miny) * 0.87
        box = (
            (minx + maxx)/2 - size/2,  # minx
            (miny + maxy)/2 - size/1.8,  # miny
            (minx + maxx)/2 + size/2,  # maxx
            (miny + maxy)/2 + size/1.8  # maxy
        )

        # Process in chunks
        return data.rio.clip_box(*box).persist()

    def reproject(self, data: xr.Dataset):
        x_meters = data.x.values * GOES19.height
        y_meters = data.y.values * GOES19.height
        data.rio.write_crs(GOES19.crs, inplace=True)

        data = data.assign_coords({
            'x': x_meters,
            'y': y_meters
        })

        data = data.rio.reproject(
            'EPSG:3857',
            resolution=2000,
            resampling=Resampling.nearest,
            chunks=data.chunks,
            num_threads=os.cpu_count() or 4,
        )

        return self._crop(data)
