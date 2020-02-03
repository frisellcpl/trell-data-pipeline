import asyncio

from pipeline import settings


class Producer():
    ''' Base class for producers. '''
    def __init__(self):
        self.hosts = settings.PRODUCER_HOSTS
        self.target = settings.PRODUCER_TARGET
        self.producer = None
        self.connected = False
        self.loop = asyncio.get_event_loop()

    async def connect(self) -> None:
        ''' Connect to producer destination '''
        raise NotImplementedError("No driver specified")

    async def disconnect(self) -> None:
        ''' Disconnect from producer destination '''
        raise NotImplementedError("No driver specified")

    async def produce_data(self, data: dict, target: str = None) -> None:
        ''' Override this method to produce data to specific producer destination, which we randomly call target. '''
        if not target:
            target = settings.PRODUCER_TARGET
