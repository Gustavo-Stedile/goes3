from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Union

from goes2.geo.projection import Projection, WebMercator
from .aws import AWSRepository

import asyncio

from goes2.product import Product
from goes2.raster import Rasterizer
from goes2.storage import Storage, TimeSeriesStorage

import xarray as xr

class GOES2:
    def __init__(self, rasterizer: Rasterizer):
        self._repo = AWSRepository()
        self._projection = WebMercator()
        self._rasterizer = rasterizer
        self._semaphore = asyncio.Semaphore(4)
        self._store = TimeSeriesStorage(at='static', max_size=12)

    def to(self, rasterizer: Rasterizer):
        self._rasterizer = rasterizer

    def _generate(self, product: Product, data, date: datetime):

        reprojs = []
        for datum in data:
            reproj = self._projection.reproject(datum)
            reprojs.append(reproj)
        
        result = product.create(*reprojs)

        path = self._store.new(product.name, date)
        self._rasterizer.to_raster(result, path)

    async def _handle_product(
        self, 
        product: Product, 
        date: datetime, 
    ):
        already_exists = self._store.find_by_date(product.name, date, False)
        if already_exists:
            print(f'{product.name} das {date} já existe')
            return

        paths = await self._repo.get(product.uses, date)

        async with self._semaphore:
            data = [xr.open_dataset(path, chunks='auto') for path in paths]

            print(f'produzindo {product}')
            await asyncio.to_thread(self._generate, product, data, date)

    def on_projection(self, projection: Projection):
        self._projection = projection
        return self

    def at_date(self, date: datetime):
        self._date = date
        return self

    def use_store(self, store: Storage):
        self._store = store

    def _flatten_requests(self, products):
        # necessário, pois CMI.in_range retorna uma lista
        flattened = []
        for product in products:
            if isinstance(product, list):
                for p in product:
                    flattened.append(p)
            else:
                flattened.append(product)

        return flattened

    async def produce_in_parallel(
        self,
        products: Union[List[Product], Product],
    ):
        if not isinstance(products, list):
            products = [products]

        products = self._flatten_requests(products)

        tasks = []
        for product in products:
            task = asyncio.create_task(
                self._handle_product(product, self._date),
            )
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=True)

    async def dispose(self):
        await self._repo.dispose()
