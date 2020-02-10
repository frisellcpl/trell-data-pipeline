from .base import Client


async def init_client(cls: Client) -> Client:
    '''
    Creates an instance and runs setup instructions of supplied client class.
    '''
    client = cls()
    await client._setup()
    return client


