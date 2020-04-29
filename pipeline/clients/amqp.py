import logging
from typing import (
    List,
    Any,
)
import aiormq

from .base import Client


LOG = logging.getLogger(__name__)


class AMQPClient(Client):
    ''' Client for consuming records from a Kafka topic and
    pipe it to configured producer. '''
    def __init__(
            self,
            **kwargs: Any,
    ):
        super().__init__(**kwargs)

        self.connection = None
        self.channel = None
        self.connstring = f'amqp://{self.username}:{self.password}@{self.uri}'

    async def connect(self, topics: List) -> None:
        self.connection = await aiormq.connect(self.connstring)

        self.channel = await self.connection.channel()

        for topic in topics:
            deaclare_ok = await channel.queue_declare(topic)
            consume_ok = await channel.basic_consume(
                deaclare_ok.queue,
                self.on_message,
                no_ack=False,
                durable=True,
            )
        
        self.on_connect(self.channel)
        self.connected = True

    async def disconnect(self) -> None:
        await self.connection.close()
        self.connected = False
        self.on_disconnect(self.consumer._client)
