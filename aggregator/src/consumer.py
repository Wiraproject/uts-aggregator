import asyncio
import json
import logging
from typing import Any
from .dedup_store import DedupStore
from .stats import Stats

logger = logging.getLogger('aggregator.consumer')

class Consumer:
    def __init__(self, queue: asyncio.Queue, dedup: DedupStore, stats: Stats):
        self.queue = queue
        self.dedup = dedup
        self.stats = stats
        self._stop = False

    async def run(self):
        logger.info('Consumer started')
        while not self._stop:
            try:
                event = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            await self._handle_event(event)
            self.queue.task_done()

    async def _handle_event(self, event: dict):
        topic = event['topic']
        event_id = event['event_id']
        
        logger.debug('Processing event topic=%s event_id=%s', topic, event_id)

        self.stats.unique_processed += 1

    def stop(self):
        self._stop = True