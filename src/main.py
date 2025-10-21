import os
import asyncio
import json
import logging
from fastapi import FastAPI, HTTPException, Query
from fastapi import BackgroundTasks
from fastapi.responses import JSONResponse
from typing import List, Optional, Union
from .models import Event
from .dedup_store import DedupStore
from .consumer import Consumer
from .stats import Stats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('aggregator')

DB_PATH = os.environ.get('DEDUP_DB', './data.db')

dedup = DedupStore(DB_PATH)
stats = Stats()
queue: asyncio.Queue = asyncio.Queue()
consumer = Consumer(queue, dedup, stats)

app = FastAPI()

@app.on_event('startup')
async def startup():
    # start consumer task
    app.state.consumer_task = asyncio.create_task(consumer.run())
    app.state.start_time = stats.start_time

@app.on_event('shutdown')
async def shutdown():
    consumer.stop()
    task = app.state.consumer_task
    task.cancel()
    try:
        await task
    except Exception:
        pass

@app.post('/publish')
async def publish(events: Union[Event, List[Event]]):
    # accept single or list (FastAPI will coerce single into model)
    evs = events if isinstance(events, list) else [events]
    enqueued = 0
    for ev in evs:
        # validation already done by Pydantic
        stats.received += 1
        await queue.put(ev.dict())
        enqueued += 1
    return JSONResponse({'enqueued': enqueued})

@app.get('/events')
async def get_events(topic: Optional[str] = Query(None)):
    items = dedup.list_by_topic(topic)
    return JSONResponse({'events': items})

@app.get('/stats')
async def get_stats():
    data = stats.to_dict()
    data['topics'] = dedup.topics()
    return JSONResponse(data)