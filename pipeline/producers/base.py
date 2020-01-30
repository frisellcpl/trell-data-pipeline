import asyncio

from pipeline import settings


class Producer():
    ''' Base class for producers. '''
    def __init__(self):
        self.hosts = settings.PRODUCER_HOSTS
        self.topic = settings.PRODUCER_TOPIC
        self.producer = None
        self.connected = False
        self.loop = asyncio.get_event_loop()

    async def connect(self) -> None:
        ''' Connect to producer destination '''
        raise NotImplementedError("No driver specified")

    async def disconnect(self) -> None:
        ''' Disconnect from producer destination '''
        raise NotImplementedError("No driver specified")

    async def produce_message(self, message: dict, topic: str = None) -> None:
        ''' Override this method to produce messages to specific producer destination. '''
        if not topic:
            topic = settings.PRODUCER_TOPIC
