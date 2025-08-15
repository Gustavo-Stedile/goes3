from .rasterizer import Rasterizer

import PIL.Image
import xarray as xr

import numpy as np


class Image(Rasterizer):
    def __init__(self, format: str):
        self._format = format.upper()

        if self._format == 'JPG':
            self._format = 'JPEG'

    def to_raster(self, data_array: xr.DataArray, path: str):
        data_array = data_array.astype(np.uint8)
        data_array = data_array.fillna(0)

        PIL.Image.fromarray(
            data_array.values,
            mode='RGBA'
        ).save(path + f'.{self._format.lower()}', self._format)
