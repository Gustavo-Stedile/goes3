import asyncio
from datetime import datetime, timedelta, timezone

from goes2 import GOES2
from goes2.geo.projection import WebMercator
from goes2.product import CMI
from goes2.raster import Image


async def main():
    date = datetime(2025, 8, 14, 19, tzinfo=timezone.utc)
    gen = GOES2(Image('png')).at_date(date).on_projection(WebMercator())

    await gen.produce_in_parallel([
        CMI.of('C01')
    ])

    await gen.dispose()

if __name__ == '__main__':
    asyncio.run(main())
