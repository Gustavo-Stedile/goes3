from dataclasses import dataclass
from typing import Tuple
from .product import Product

import xarray as xr


@dataclass
class TrueColor(Product):
    uses: Tuple[str] = (
        'ABI-L2-CMIPF/C01',
    )

    name: str = "truecolor"

    def create(self, data) -> xr.DataArray:
        da = data['CMI']
        da = da * 255
        return da
