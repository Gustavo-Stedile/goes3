from pathlib import Path
from .rasterizer import Rasterizer

import xarray as xr
import numpy as np

from uuid import uuid4

from typing import Tuple

import subprocess
import os

class GDALTiles(Rasterizer):
    def __init__(
        self, 
        format: str = 'PNG', 
        zoom_range: Tuple[int] = (4, 6),
        max_workers = os.cpu_count() | 4
    ):
        self._format = format.upper()
        self._min_zoom, self._max_zoom = zoom_range
        self._max_workers = max_workers

    def to_raster(self, data_array: xr.DataArray, path):
        data_array = data_array.transpose('band', 'y', 'x')
        data_array = data_array.astype(np.uint8)

        temp_path = Path('temp')
        temp_path.mkdir(exist_ok=True, parents=True)

        tif_path = temp_path/(str(uuid4())+'.tif')

        data_array.rio.write_crs("EPSG:3857", inplace=True)
        data_array.rio.to_raster(tif_path)
        
        subprocess.run([
            "gdal2tiles.py", 
            f"--zoom={self._min_zoom}-{self._max_zoom}",
            f"--processes={self._max_workers}",
            "--webviewer=none",
            tif_path,
            path
        ])

        os.remove(tif_path)        
