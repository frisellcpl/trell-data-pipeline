from pipeline import settings
from .kafka import KafkaProducer
from .mock import MockProducer


def get_producer() -> Producer:
    if settings.PRODUCER_DRIVER == 'kafka':
        return KafkaProducer()
    else:
        return MockProducer()
