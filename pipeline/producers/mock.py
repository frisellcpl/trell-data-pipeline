import asyncio
import logging

from .base import Producer


log = logging.getLogger(__name__)


class MockProducer(Producer):
    async def connect(self) -> None:
        log.debug('Connected to mock producer')
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False

    async def produce_message(self, message: dict, topic: str=None) -> None:
        log.debug('Mocking a message on topic {topic}: {message}'.format(
            topic=topic,
            message=message,
        ))
