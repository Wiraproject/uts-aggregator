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
    evs = events if isinstance(events, list) else [events]
    enqueued = 0
    duplicates_rejected = 0
    
    for ev in evs:
        try:
            stats.received += 1

            topic = ev.topic
            event_id = ev.event_id

            if isinstance(ev.timestamp, str):
                timestamp = ev.timestamp
            elif hasattr(ev.timestamp, 'isoformat'):
                timestamp = ev.timestamp.isoformat()
            else:
                timestamp = str(ev.timestamp)
            
            source = ev.source

            try:
                payload_json = json.dumps(ev.payload, separators=(',', ':'))
            except (TypeError, ValueError) as e:
                logger.error(f'Failed to serialize payload for event {event_id}: {e}')
                stats.duplicate_dropped += 1 
                continue

            inserted = dedup.add_if_new(topic, event_id, timestamp, source, payload_json)
            
            if not inserted:
                stats.duplicate_dropped += 1
                duplicates_rejected += 1
                logger.info(f'Duplicate rejected at ingestion: topic={topic} event_id={event_id}')
                continue

            await queue.put(ev.dict())
            enqueued += 1
            
        except Exception as e:
            logger.error(f'Error processing event {ev.event_id if hasattr(ev, "event_id") else "unknown"}: {e}')
            stats.duplicate_dropped += 1 
            continue
    
    return JSONResponse({
        'enqueued': enqueued,
        'duplicates_rejected': duplicates_rejected
    })

@app.get('/events')
async def get_events(topic: Optional[str] = Query(None)):
    items = dedup.list_by_topic(topic)
    return JSONResponse({'events': items})

@app.get('/stats')
async def get_stats():
    data = stats.to_dict()
    data['topics'] = dedup.topics()
    return JSONResponse(data)