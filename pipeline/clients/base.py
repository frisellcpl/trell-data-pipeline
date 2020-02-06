import logging
import asyncio
import json
from typing import (
    Callable,
    Tuple,
    Any,
    List,
)
from pipeline.producers import get_producer


LOG = logging.getLogger(__name__)


def _on_connect(client, *args, **kwargs):
    LOG.debug('%s: CONNECTED', client._client_id)


async def _on_message(msg, *args, **kwargs):
    LOG.debug('RECV: %s', msg)


def _on_disconnect(client, *args, **kwargs):
    LOG.debug('%s: DISCONNECTED', client._client_id)


def _on_subscribe(client, topics, **kwargs):
    LOG.debug('%s: SUBSCRIBED TO: %s', client._client_id, topics)


class Consumer:
    ''' Base class for consumers.'''
    def __init__(
            self,
            on_connect: Callable = None,
            on_message: Callable = None,
            on_disconnect: Callable = None,
            on_subscribe: Callable = None,
    ):
        self.consumer = None
        self.connected = False

        self.producer = get_producer()

        self.on_connect = on_connect or _on_connect
        self.on_message = on_message or _on_message
        self.on_disconnect = on_disconnect or _on_disconnect
        self.on_subscribe = on_subscribe or _on_subscribe

        self.loop = asyncio.get_event_loop()

    async def pipe_message(self, data: dict, target: str) -> None:
        ''' Pipes mesage to configured producer. '''
        await self.producer.produce_message(
            data=data,
            target=target,
        )

    async def connect(self, topics: List) -> None:
        raise NotImplementedError("No driver specified")

    async def disconnect(self) -> None:
        raise NotImplementedError("No driver specified")
