import logging
from typing import (
    List,
    Any,
)
from aiokafka import AIOKafkaConsumer

from .base import Client


LOG = logging.getLogger(__name__)

class KafkaClient(Client):
    ''' Client for consuming records from a Kafka topic and
    pipe it to configured producer. '''
    def __init__(
            self,
            group_id: str,
            **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.consumer = None
        self.group_id = group_id
        self.hosts = self.uri.split(',')

    async def connect(self, topics: List) -> None:
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
        try:
            async for msg in self.consumer:
                await self.on_message(msg)
        finally:
            self.disconnect()

    async def disconnect(self) -> None:
        self.consumer.stop()
        self.connected = False
        self.on_disconnect(self.consumer._client)
