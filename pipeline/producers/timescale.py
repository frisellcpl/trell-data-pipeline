import logging
from datetime import datetime

from jsonschema import validate
from jsonschema.exceptions import ValidationError
from asyncpg import create_pool

from .base import Producer


LOG = logging.getLogger(__name__)


SCHEMAS = [
    {
        'name': 'indoor_data',
        'schema': {
            "type": "object",
            "properties": {
                "ts": {
                    "type": "number",
                },
                "id": {
                    "type": "string",
                },
                "t": {
                    "type": ["number", "null"],
                },
                "h": {
                    "type": ["number", "null"],
                },
                "c": {
                    "type": ["number", "null"],
                },
                "b": {
                    "type": ["number", "null"],
                },
                "m": {
                    "type": ["number", "null"],
                },
                "l": {
                    "type": ["number", "null"],
                },
            },
            "required": ["ts", "id"],
        }
    },
]


async def _extract_schema(data: dict) -> str:
    ''' Tries to validate data towards all defined schemas and returns the first hit.
    '''
    for schema in SCHEMAS:
        try:
            validate(data, schema['schema'])
            return schema['name']
        except ValidationError:
            pass
    raise ValidationError('No matching schema could be matched with %s' % data)


async def _build_query(data: dict, target: str, data_schema: str) -> str:
    ''' Builds a database query based on provided data, target and extracted schema. '''
    if data_schema == 'indoor_data':
        return "INSERT INTO {target}(t,h,l,m,c,b,id,ts)" \
            "VALUES({t},{h},{l},{m},{c},{b},'{id}','{ts}')".format(
                target=target,
                t=data.get('t', None),
                h=data.get('h', None),
                l=data.get('l', None),
                m=data.get('m', None),
                c=data.get('c', None),
                b=data.get('b', None),
                id=data['id'],
                ts=datetime.fromtimestamp(data['ts']),
            )


class TimescaleProducer(Producer):
    '''
    Uses connections from a shared connection pool to connect to a timescale database.

    Exposes a number of methods for ease of use implementation by a client.
    TODO: Document interface.
    '''
    async def _setup(self) -> None:
        await super()._setup()
        self.pool = await create_pool(
            host=self.hosts,
            database=self.database,
            user=self.username,
            password=self.password,
            loop=self.loop,
            min_size=10,
            max_size=100,
        )

    async def connect(self) -> None:
        self.connected = True

    async def disconnect(self) -> None:
        await self.producer.stop()
        self.connected = False

    async def produce_data(self, data: dict, target: str = None) -> None:
        '''
        Stores data into configured database and target table.
        Depending on the target it formats the data according to a scheme.
        If no scheme can be applied it raises an validation error.
        '''

        await super().produce_data(data=data, target=target)

        data_schema = await _extract_schema(data=data)
        query = await _build_query(
            data=data,
            target=self.target,
            data_schema=data_schema,
        )

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(query)
