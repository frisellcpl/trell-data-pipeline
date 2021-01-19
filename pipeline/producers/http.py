import logging
import json
from typing import Any

from aiohttp import ClientSession

from .base import Producer


LOG = logging.getLogger(__name__)


class HttpProducer(Producer):
    '''
    Connects to a kafka cluster and roduces messages as an encoded JSON structure.
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
        async with ClientSession() as session, session.post(url, json=data) as response:
            await response.text()
