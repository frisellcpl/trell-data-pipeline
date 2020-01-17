import logging
import asyncio
import functools
import signal
from typing import Coroutine

from pipeline import STOP


log = logging.getLogger(__name__)


async def task_wrapper(task: Coroutine):
    try:
        await task(stop_signal=STOP)
    except asyncio.CancelledError:
        log.debug('Cancelled: {task}'.format(task=task.__name__))
    except asyncio.TimeoutError:
        log.debug('Timed out: {task}. Restarting.'.format(task=task.__name__))

        
def ask_exit(*args):
    STOP.set()

    
def main(task):
    loop = asyncio.get_event_loop()

    # connect signal handler for stopping program
    stop_signals = [signal.SIGINT, signal.SIGHUP]
    for value in stop_signals:
        loop.add_signal_handler(value, ask_exit)

    log.debug('Starting loop.')
    loop.run_until_complete(asyncio.gather(
        task_wrapper(task),
    ))

    log.debug('Main finished.')
