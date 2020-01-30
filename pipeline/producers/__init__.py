from pipeline import settings
from .base import Producer
from .kafka import KafkaProducer
from .mock import MockProducer


def get_producer() -> Producer:
    ''' Returns chosen producer based applied config '''
    if settings.PRODUCER_DRIVER == 'kafka':
        return KafkaProducer()

    return MockProducer()
