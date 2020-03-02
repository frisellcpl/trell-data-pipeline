import random
import logging
import asyncio
import time
from typing import List, Callable

from .base import Client


LOG = logging.getLogger(__name__)


class MockDataGenerator():
    def __init__(self):
        self.sensor_ids = [
            'a000000000000001',
            'a000000000000002',
            'a000000000000003',
            'a000000000000004',
            'a000000000000005',
            'a000000000000006',
        ]

        self.data = self._init_data()

    def _init_data(self):
        return [
            {
                'ts': int(time.time()),
                'id': sensor_id,
                't': round(random.uniform(19.5, 22.5), 2),
                'h': round(random.uniform(40.0, 50.0), 2),
                'b': 3021,
                'c': round(random.uniform(450, 900), 0),
                'l': round(random.uniform(2, 150), 0),
                'm': 1,
            } for sensor_id in self.sensor_ids
        ]

    async def refresh(self):
        for d in self.data:
            d['t'] += round(random.uniform(-0.05, 0.05), 2)
            d['h'] += round(random.uniform(-0.05, 0.05), 2)
            d['c'] += round(random.uniform(-2, 2), 0)
            d['l'] += round(random.uniform(-0.15, 0.15), 0)


class MockClient(Client):
    async def _produce_mock_messages(self):
        while self.connected:
            await self.mock_data_generator.refresh()
            for data in self.mock_data_generator.data:
                await self.on_message(self, data)

            await asyncio.sleep(60)

    async def connect(self, topics: List) -> None:
        self.connected = True
        self.mock_data_generator = MockDataGenerator()
        await self._produce_mock_messages()
        LOG.info('Mock client connected')

    async def disconnect(self) -> None:
        self.connected = False
        LOG.info('Mock client disconnected')
