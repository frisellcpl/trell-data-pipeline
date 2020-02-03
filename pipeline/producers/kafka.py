import logging
import json

from aiokafka import AIOKafkaProducer

from .base import Producer


LOG = logging.getLogger(__name__)


class KafkaProducer(Producer):
    '''
    Connects to a kafka cluster and roduces messages as an encoded JSON structure.
    '''
    async def connect(self) -> None:
        LOG.debug('Connecting to Kafka stream.')
        self.producer = AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=self.hosts
        )
        await self.producer.start()
        self.connected = True

    async def disconnect(self) -> None:
        await self.producer.stop()
        self.connected = False

    async def produce_message(self, data: dict, target: str = None) -> None:
        LOG.debug('Trying to produce message: %s', message)

        if not self.connected:
            LOG.debug('Producer not connected')
            return

        await super().produce_message(message=message, topic=topic)
        await self.producer.send_and_wait(target, json.dumps(data).encode())
