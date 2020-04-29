import logging
import json
from typing import Any

from boto3 import client

from .base import Producer


LOG = logging.getLogger(__name__)


class FirehoseProducer(Producer):
    '''
    Connects to a kafka cluster and roduces messages as an encoded JSON structure.
    '''
    async def _setup(self) -> None:
        await super()._setup()
        self.client = client('firehose', region_name=self.region)

    async def connect(self) -> None:
        ''' Connect to producer destination '''
        LOG.info('Connected.')

    async def disconnect(self) -> None:
        ''' Disconnect from producer destination '''
        LOG.info('Disconnected.')

    async def produce_data(self, data: bytes, target: str = None) -> None:
        await super().produce_data(data=data, target=target)
        
        LOG.debug('Trying to produce data: %s', data)
        self.client.put_record(
            DeliveryStreamName=self.target,
            Record={
                'Data': data,
            }
        )

