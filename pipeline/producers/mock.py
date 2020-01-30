import logging

from .base import Producer


LOG = logging.getLogger(__name__)


class MockProducer(Producer):
    ''' Produces log messages instead of pushing an existing source '''
    async def connect(self) -> None:
        LOG.debug('Connected to mock producer')
        self.connected = True

    async def disconnect(self) -> None:
        self.connected = False

    async def produce_message(self, message: dict, topic: str = None) -> None:
        LOG.debug('Mocking a message on topic %s: %s', topic, message)
