import logging
from typing import (
    List,
    Any,
)
from aiokafka import AIOKafkaConsumer

from .base import Client


LOG = logging.getLogger(__name__)


def _on_subscribe(client, topics, **kwargs):
    LOG.debug('%s: SUBSCRIBED TO TOPICS: %s', client._client_id, topics)


class KafkaClient(Client):
    ''' Client for consuming records from a Kafka topic and
    pipe it to configured producer. '''
    def __init__(
            self,
            group_id: str,
            **kwargs: Any,
    ):
        super().__init__(on_subscribe=_on_subscribe, **kwargs)

        self.consumer = None
        self.group_id = group_id
        self.hosts = self.uri.split(',')

    async def connect(self, topics: List) -> None:
        #TODO: Seems like kafka doesnt listen to stop signal so we need to
        # send STOP in here and await it. Or move message handlong out to the main function
        self.consumer = AIOKafkaConsumer(
            *topics,
            group_id=self.group_id,
            bootstrap_servers=self.hosts,
            loop=self.loop,
        )

        await self.consumer.start()

        self.on_connect(self.consumer._client)
        self.on_subscribe(self.consumer._client, topics)
        self.connected = True

    async def disconnect(self) -> None:
        await self.producer.disconnect()
        await self.consumer.stop()
        self.connected = False
        self.on_disconnect(self.consumer._client)
