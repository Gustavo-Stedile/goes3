from dataclasses import dataclass
from typing import Tuple, Union

from abc import ABC, abstractmethod
from matplotlib.colors import Colormap
import xarray as xr

from goes2.raster.cpt_utils import load_cpt
from pathlib import Path

import matplotlib.pyplot as plt


@dataclass
class Product(ABC):
    name: str
    uses: Union[Tuple[str], str]

    def apply_palette(self, data, palette_path, range=(0, 1)):
        if isinstance(palette_path, Colormap):
            palette = palette_path

        elif isinstance(palette_path, str):
            palette_path = Path(palette_path)

            palette = (
                load_cpt(palette_path)
                if palette_path.exists()
                else plt.colormaps[str(palette_path)]
            )

        vmin, vmax = range
        if vmin is not None or vmax is not None:
            norm = plt.Normalize(vmin=vmin, vmax=vmax)
            normalized_values = norm(data)

        return xr.DataArray(
            data=palette(normalized_values) * 255,
            coords={
                **data.coords,
                'band': ['R', 'G', 'B', 'A'],  # Assuming RGBA (4 bands)
            },
            dims=data.dims + ("band",),
            attrs=data.attrs
        )

    @abstractmethod
    def create(self, data) -> xr.DataArray:
        pass
