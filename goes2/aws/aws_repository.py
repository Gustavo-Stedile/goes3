from datetime import datetime, timezone
from typing import Optional, Tuple, Union

import aioboto3

from botocore import UNSIGNED
from botocore.config import Config

from goes2.aws.download_manager import DownloadManager

import asyncio


class AWSRepository:
    def __init__(self):
        self._session = aioboto3.Session()

        self._bucket_name = 'noaa-goes19'
        self._download_manager = DownloadManager(self._bucket_name)

        self._s3 = None

    def _flatten_request(self, product_request: str):
        if '/' in product_request:
            return product_request.split('/')
        else:
            return product_request, None

    async def _get_s3_resource(self):
        if self._s3 is None:
            self._s3 = await self._session.resource(
                's3',
                config=Config(signature_version=UNSIGNED)
            ).__aenter__()
        return self._s3

    async def _is_channel_in_key(self, product):
        await self._get_s3_resource()
        bucket = await self._s3.Bucket(self._bucket_name)

        async for obj in bucket.objects.filter(Prefix=product).limit(1):
            return 'M6C' in obj.key

    async def _find_key(self, product: str, channel: str, date: datetime):
        channel_in_key = await self._is_channel_in_key(product)

        if channel_in_key and not channel:
            raise ValueError(
                f'produto {product} deve especificar um canal'
            )

        bucket = await self._s3.Bucket(self._bucket_name)

        date = date.replace(minute=(date.minute // 10) * 10)
        prefix = f'{product}/{date.strftime("%Y/%j/%H")}'
        date_in_key = f'_s{date.strftime("%Y%j%H%M")}'

        async for obj in bucket.objects.filter(Prefix=prefix):
            if date_in_key in obj.key:
                if channel:
                    if channel in obj.key:
                        return obj.key
                else:
                    return obj.key

        raise Exception(
            f'produto {product}/{channel} não encontrado às {date}'
        )

    async def _fetch_product(self, product: str, channel: str, date: datetime):
        key = await self._find_key(product, channel, date)
        return await self._download_manager.get_file(key)

    async def get(
        self,
        product_requests: Union[Tuple[str], str],
        date: Optional[datetime] = None
    ):
        if date is None:
            # TODO: encontrar o mais recente de todos os produtos
            date = datetime.now().astimezone(timezone.utc)
            pass

        if not isinstance(product_requests, tuple):
            product_requests = (product_requests,)

        tasks = []
        for req in product_requests:
            product, channel = self._flatten_request(req)

            download_task = asyncio.create_task(
                self._fetch_product(product, channel, date)
            )
            tasks.append(download_task)

        return await asyncio.gather(*tasks)

    async def dispose(self):
        await self._download_manager.dispose()

        if self._s3:
            await self._s3.__aexit__(None, None, None)
