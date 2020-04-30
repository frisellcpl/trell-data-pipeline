import logging
from typing import (
    List,
    Any,
    Callable,
)
import aiormq

from .base import Client


LOG = logging.getLogger(__name__)


def _on_disconnect(connection):
    LOG.debug('%s: DISCONNECTED', connection)


def _on_connect(connection):
    LOG.debug('%s: CONNECTED', connection)


class AMQPClient(Client):
    ''' Client for consuming records from a Kafka topic and
    pipe it to configured producer. '''
    def __init__(
            self,
            on_connect: Callable = _on_connect,
            on_disconnect: Callable = _on_disconnect,
            **kwargs: Any,
    ):
        super().__init__(
            on_connect=on_connect,
            on_disconnect=on_disconnect,
            **kwargs,
        )

        self.connection = None
        self.connstring = f'amqp://{self.username}:{self.password}@{self.uri}'

    async def connect(self, topics: str, no_ack: bool = True, durable: bool = True, prefetch_count: int = 10) -> tuple:
        self.connection = await aiormq.connect(self.connstring)

        channel = await self.connection.channel()
        await channel.basic_qos(prefetch_count=prefetch_count)

        declare_ok = await channel.queue_declare(topics, durable=durable)
        consume_ok = await channel.basic_consume(
            declare_ok.queue,
            self.on_message,
            no_ack=no_ack,
        )
        
        self.on_connect(self.connection)
        self.connected = True

    async def disconnect(self) -> None:
        await self.connection.close()
        self.on_disconnect(self.connection)
        self.connected = False
