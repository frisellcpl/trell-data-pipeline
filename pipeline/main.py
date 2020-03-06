import functools
import logging
import asyncio
import signal
from typing import Coroutine

LOG = logging.getLogger(__name__)


def cancel_all_tasks(loop: asyncio.BaseEventLoop):
    LOG.debug('Cancelling tasks.')
    tasks = asyncio.gather(
        *asyncio.Task.all_tasks(loop=loop),
        loop=loop,
        return_exceptions=True,
    )

    tasks.cancel()


async def task_wrapper(task: Coroutine):
    ''' Wraps a coroutine as a task and passes the global stop signal to it '''
    try:
        await task()
    except asyncio.CancelledError:
        LOG.debug('Cancelled: %s', task.__name__)
    except asyncio.TimeoutError:
        LOG.debug('Timed out: %s. Restarting.', task.__name__)


def main(task):
    '''
    Main entry point for a task.
    Adds signal handlers and starts the task.
    '''
    loop = asyncio.get_event_loop()

    # connect signal handler for stopping program
    stop_signals = [signal.SIGINT, signal.SIGHUP]
    stop_handler = functools.partial(cancel_all_tasks, loop)
    for value in stop_signals:
        loop.add_signal_handler(value, stop_handler)

    LOG.debug('Starting loop.')
    loop.run_until_complete(asyncio.gather(
        task_wrapper(task),
    ))

    LOG.debug('Main finished.')
