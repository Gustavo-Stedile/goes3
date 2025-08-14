from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Callable, List, Union

from goes2.geo.projection import Projection, WebMercator
from .aws import AWSRepository

import asyncio

from goes2.product import Product

from goes2.raster import Rasterizer


class GOES2:
    def __init__(self, rasterizer: Rasterizer):
        self._repo = AWSRepository()
        self._projection = WebMercator()
        self._rasterizer = rasterizer
        self._semaphore = asyncio.Semaphore(2)

    def to(self, rasterizer: Rasterizer):
        self._rasterizer = rasterizer

    def _generate(self, product: Product, data):
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            reprojs = []

            for datum in data:
                futures.append(executor.submit(
                    self._projection.reproject, datum
                ))

            for future in futures:
                reprojs.append(future.result())

        result = product.create(*reprojs)
        self._rasterizer.to_raster(result, 'output.png')

    async def _handle_product(self, product: Product, date: datetime):
        data = await self._repo.get(product.uses, date)

        async with self._semaphore:
            print(f'produzindo {product}')
            await asyncio.to_thread(self._generate, product, data)

    def on_projection(self, projection: Projection):
        self._projection = projection
        return self

    def at_date(self, date: datetime):
        self._date = date
        return self

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
                self._handle_product(product, self._date)
            )
            tasks.append(task)

        await asyncio.gather(*tasks)

    async def dispose(self):
        await self._repo.dispose()
