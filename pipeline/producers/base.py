import asyncio

from pipeline import settings


class Producer():
    def __init__(self):
        self.hosts = settings.PRODUCER_HOSTS
        self.topic = settings.PRODUCER_TOPIC
        self.producer = None
        self.connected = False
        self.loop = asyncio.get_event_loop()

    async def connect(self) -> None:
        raise NotImplemented("No driver specified")

    async def disconnect(self) -> None:
        raise NotImplemented("No driver specified")

    async def produce_message(self, message: dict, topic: str=None) -> None:
        if not topic:
            topic = settings.PRODUCER_TOPIC
