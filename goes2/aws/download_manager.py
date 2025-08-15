import aioboto3
from botocore import UNSIGNED
from botocore.config import Config

import asyncio

from typing import Dict, Set
from pathlib import Path

import xarray as xr


class DownloadManager:
    def __init__(self, bucket_name: str = 'noaa-goes19'):
        self._session = aioboto3.Session()
        self._s3 = None

        self._downloaded_files: Set[str] = set()
        self._pending_downloads: Dict[str, asyncio.Event] = {}
        self._download_tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

        self._temp_path = Path('temp')
        self._temp_path.mkdir(exist_ok=True, parents=True)

        self._bucket_name = bucket_name

    async def _get_s3_resource(self):
        if self._s3 is None:
            self._s3 = await self._session.resource(
                's3',
                config=Config(signature_version=UNSIGNED)
            ).__aenter__()
        return self._s3

    def _get_cached_file(self, file_key: str):
        path = self._temp_path / file_key.split('/')[-1]
        return path

    async def _download_file(self, file_key: str, event: asyncio.Event):
        try:
            await self._get_s3_resource()
            bucket = await self._s3.Bucket(self._bucket_name)
            await bucket.download_file(
                file_key,
                self._temp_path / file_key.split('/')[-1]
            )

            self._downloaded_files.add(file_key)
            self._pending_downloads.pop(file_key)

            event.set()

        except Exception as e:
            print(e)

    async def get_file(self, file_key: str):
        async with self._lock:
            # arquivo já foi baixado
            if file_key in self._downloaded_files:
                return self._get_cached_file(file_key)

            # arquivo está sendo baixado
            if file_key in self._pending_downloads:
                event = self._pending_downloads[file_key]
                await event.wait()
                return self._get_cached_file(file_key)

            # arquivo precisa ser baixado
            event = asyncio.Event()
            self._pending_downloads[file_key] = event
            task = asyncio.create_task(self._download_file(file_key, event))
            self._download_tasks[file_key] = task

        await event.wait()

        if file_key in self._downloaded_files:
            return self._get_cached_file(file_key)

    async def dispose(self):
        async with self._lock:
            for task in self._download_tasks.values():
                task.cancel()

            await asyncio.gather(
                *self._download_tasks.values(),
                return_exceptions=True
            )

            if self._s3:
                await self._s3.__aexit__(None, None, None)
