from datetime import datetime
import asyncio
import pytest
from jsonschema.exceptions import ValidationError

from pipeline.clients.mock import MockClient


@pytest.mark.asyncio
async def test_that_mock_client_dispatches_message():
    data = {
        'ts': 1581412003,
        'id': 'abc123',
        't': 21.3,
        'h': 42.1,
        'b': 3021,
        'c': 551.32,
        'l': 15,
        'm': 1,
    }

    async def mocked_on_message(client: MockClient, msg: str) -> None:
        assert msg == data
        client.connected = False

    client = MockClient(uri='mock://mocker.test.test', on_message=mocked_on_message)
    await client.connect(topics=['topic1'])
