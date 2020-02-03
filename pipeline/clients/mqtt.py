import logging
import json
from typing import (
    Callable,
    Tuple,
)

from gmqtt import Client, Subscription
from gmqtt.mqtt.constants import MQTTv311

from pipeline.producers import get_producer


LOG = logging.getLogger(__name__)


def _on_connect(client, flags, rc, properties):
    LOG.debug('%s: CONNECTED', client._client_id)


def _on_message(client, topic, payload, qos, properties):
    LOG.debug('RECV: %s', payload)


def _on_disconnect(client, packet, exc=None):
    LOG.debug('%s: DISCONNECTED', client._client_id)


def _on_subscribe(client, mid, qos):
    LOG.debug('%: SUBSCRIBED', client_id=client._client_id)


class MQTTClient(Client):
    ''' Client for subscribing to a broker and pipe incomming messages to configured producer. '''
    def __init__(
            self,
            username: str = None,
            password: str = None,
            connected: bool = False,
            on_connect: Callable = None,
            on_message: Callable = None,
            on_disconnect: Callable = None,
            on_subscribe: Callable = None,
    ) -> None:
        super().__init__(None)

        if not username is None:
            self.set_auth_credentials(username, password)

        self.producer = get_producer()
        self.connected = connected

        self.on_connect = on_connect or _on_connect
        self.on_message = on_message or _on_message
        self.on_disconnect = on_disconnect or _on_disconnect
        self.on_subscribe = on_subscribe or _on_subscribe

    async def pipe_message(self, data: dict, target: str) -> None:
        ''' Pipes mesage to configured producer. '''
        await self.producer.produce_message(
            data=data,
            target=target,
        )

    async def connect(self, uri: str, topics: Tuple[str, int]) -> None:
        ''' Connects to broker. '''
        LOG.debug('Connecting to MQTT broker.')
        try:
            await super().connect(uri, 1883, keepalive=60, version=MQTTv311)
            await self.producer.connect()

            subscriptions = [Subscription(t[0], qos=t[1]) for t in topics]
            self.subscribe(subscriptions, subscription_identifier=1)
            self.connected = True
        except:
            self.connected = False


async def serialize(data):
    ''' Decodes recieved data to utf-8 and returns it as a dict. '''
    data = data.decode('utf-8')
    try:
        return json.loads(data)
    except json.decoder.JSONDecodeError:
        return data
