import asyncio
from datetime import datetime, timedelta, timezone

from goes2 import GOES2
from goes2.geo.projection import WebMercator
from goes2.product import CMI
from goes2.raster import Image


async def main():
    date = datetime.now(timezone.utc) - timedelta(minutes=20)
    gen = GOES2(Image('png')).at_date(date).on_projection(WebMercator())

    await gen.produce_in_parallel([
        CMI.in_range(1, 8)
    ])

    await gen.dispose()

if __name__ == '__main__':
    asyncio.run(main())
