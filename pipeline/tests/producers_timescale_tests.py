from datetime import datetime
import asyncio
import pytest
from jsonschema.exceptions import ValidationError

from pipeline.producers.timescale import (
    _extract_schema,
    _build_query,
)
    


@pytest.mark.asyncio
async def test_extract_valid_indoor_data_schema():
    data = {
        'ts': 1581412003,
        'id': 'abc123',
        't': 21.3,
        'h': 42.1,
        'b': 3021,
        'c': 551.32,
        'l': 21.3,
        'm': 21.3,
    }

    schema = await _extract_schema(data)
    assert schema == 'indoor_data'

    
@pytest.mark.asyncio
async def test_extract_valid_indoor_data_schema_with_nulls():
    data = {
        'ts': 1581412003,
        'id': 'abc123',
        't': None,
        'h': None,
        'b': None,
        'c': None,
        'l': None,
        'm': None,
    }

    schema = await _extract_schema(data)
    assert schema == 'indoor_data'

@pytest.mark.asyncio
async def test_extract_invalid_indoor_data_schema_with_no_ts():
    data = {
        'id': 'abc123',
        't': 21.3,
        'h': 42.1,
        'b': 3021,
        'c': 551.32,
        'l': 21.3,
        'm': 21.3,
    }

    with pytest.raises(ValidationError):
        assert await _extract_schema(data)


@pytest.mark.asyncio
async def test_extract_invalid_indoor_data_schema_with_text_temperature():
    data = {
        'ts': 1581412003,
        'id': 'abc123',
        't': '21.3',
        'h': 42.1,
        'b': 3021,
        'c': 551.32,
        'l': 21.3,
        'm': 21.3,
    }

    with pytest.raises(ValidationError):
        assert await _extract_schema(data)

@pytest.mark.asyncio
async def test_extract_valid_indoor_data_schema_without_temperature():
    data = {
        'ts': 1581412003,
        'id': 'abc123',
        'h': 42.1,
        'b': 3021,
        'c': 551.32,
        'l': 21.3,
        'm': 21.3,
    }

    schema = await _extract_schema(data)
    assert schema == 'indoor_data'

@pytest.mark.asyncio
async def test_build_indoor_data_query():
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

    query = await _build_query(
        data=data,
        target='trell_office_indoor_data',
        data_schema='indoor_data',
    )

    assert query == "INSERT INTO trell_office_indoor_data(t,h,l,m,c,b,id,ts)" \
        "VALUES(21.3,42.1,15,1,551.32,3021,'abc123','2020-02-11 10:06:43')"
    
