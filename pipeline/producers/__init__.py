from pipeline import settings
from .base import Producer
from .kafka import KafkaProducer
from .mock import MockProducer
from .firehose import FirehoseProducer
from .timescale import TimescaleProducer
from .http import HttpProducer
from .kinesis_data_stream import KinesisDataStreamProducer



async def create_producer(cls: Producer) -> Producer:
    '''
    Creates an instance and runs setup instructions of supplied producer class.
    '''
    producer = cls()
    await producer._setup()
    return producer


async def get_producer() -> Producer:
    ''' Returns chosen producer based applied config '''
    if settings.PRODUCER_DRIVER == 'kafka':
        return await create_producer(KafkaProducer)

    if settings.PRODUCER_DRIVER == 'timescale':
        return await create_producer(TimescaleProducer)

    if settings.PRODUCER_DRIVER == 'aws_firehose':
        return await create_producer(FirehoseProducer)

    if settings.PRODUCER_DRIVER == 'aws_kinesis_data_stream':
        return await create_producer(KinesisDataStreamProducer)

    if settings.PRODUCER_DRIVER == 'http':
        return await create_producer(HttpProducer)

    return await create_producer(MockProducer)
