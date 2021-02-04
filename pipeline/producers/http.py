import json
import logging
import os
from typing import Any

from aiohttp import ClientSession

from .base import Producer
from pipeline import settings


LOG = logging.getLogger(__name__)


class HttpProducer(Producer):
    '''
    Connects to the Compis API and produces messages as encoded JSON.
    '''
    async def _setup(self) -> None:
        await super()._setup()

    async def connect(self) -> None:
        ''' Connect to producer destination '''
        LOG.info('Connected.')

    async def disconnect(self) -> None:
        ''' Disconnect from producer destination '''
        LOG.info('Disconnected.')

    async def produce_data(self, data: dict, target: str = None) -> None:
        await super().produce_data(data=data, target=target)

        LOG.debug('Trying to produce data: %s', data)
        url = f'{self.target}/compis/'

        headers = {
            'authorization': f'Token {self.password}'
        }

        async with ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                await response.text()
