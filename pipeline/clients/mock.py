import logging
import asyncio
import time
from typing import List, Callable

from .base import Client


LOG = logging.getLogger(__name__)


async def _gen_mock_data():
    return {
        'ts': int(time.time()),
        'id': 'a81758fffe048c45',
        't': 21.3,
        'h': 42.1,
        'b': 3021,
        'c': 551.32,
        'l': 15,
        'm': 1,
    }


class MockClient(Client):
    async def _produce_mock_messages(self):
        while self.connected:
            data = await _gen_mock_data()
            await self.on_message(self, data)
            await asyncio.sleep(5)

    async def connect(self, topics: List) -> None:
        self.connected = True
        await self._produce_mock_messages()
        LOG.info('Mock client connected')

    async def disconnect(self) -> None:
        self.connected = False
        LOG.info('Mock client disconnected')
