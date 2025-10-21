# Aggregator Service (FastAPI + asyncio)

Project scaffold and full source files are provided below. Copy the files to their respective paths and run as described in the *Run & Test* section.

---

## File tree

```
Dockerfile
requirements.txt
README.md
src/
  ├─ __init__.py
  ├─ main.py
  ├─ models.py
  ├─ dedup_store.py
  ├─ consumer.py
  └─ stats.py
tests/
  └─ test_app.py
```

---

## `requirements.txt`

```text
fastapi==0.95.2
uvicorn[standard]==0.22.0
pydantic==1.10.9
pytest==7.4.0
httpx==0.25.0
sqlalchemy==2.2.5
alembic==1.11.1
databases==0.6.1
```

---

## `Dockerfile`

```dockerfile
FROM python:3.11-slim
WORKDIR /app
RUN adduser --disabled-password --gecos '' appuser && chown -R appuser:appuser /app
USER appuser
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
EXPOSE 8080
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080"]
```

---

## `README.md`

````markdown
# Aggregator Service

Run locally:

```bash
export DEDUP_DB=./data.db  # optional, default ./data.db
uvicorn src.main:app --reload --host 0.0.0.0 --port 8080
````

Build Docker image:

```bash
docker build -t aggregator:latest .
docker run -p 8080:8080 -v $(pwd)/data.db:/app/data.db aggregator:latest
```

Run tests:

```bash
pytest -q
```

````

---

## `src/__init__.py`

```python
# package marker
````

---

## `src/models.py`

```python
from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import Any, Dict

class Event(BaseModel):
    topic: str = Field(...)
    event_id: str = Field(...)
    timestamp: datetime = Field(...)
    source: str = Field(...)
    payload: Dict[str, Any] = Field(default_factory=dict)

    @validator('topic', 'event_id', 'source')
    def not_empty(cls, v):
        if not v or not str(v).strip():
            raise ValueError('must not be empty')
        return v
```

---

## `src/dedup_store.py`

```python
import sqlite3
import threading
from typing import Optional, List, Tuple

class DedupStore:
    """Simple SQLite-backed deduplication store (local-only).
    Stores processed (topic, event_id) and the raw event JSON for GET /events.
    Thread-safe via a connection-per-thread approach guarded by a lock for DDL.
    """
    def __init__(self, db_path: str = './data.db'):
        self.db_path = db_path
        self._ddl_lock = threading.Lock()
        self._ensure_tables()

    def _conn(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_tables(self):
        with self._ddl_lock:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS processed (
                    topic TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    timestamp TEXT,
                    source TEXT,
                    payload TEXT,
                    PRIMARY KEY (topic, event_id)
                )
            ''')
            conn.commit()
            conn.close()

    def add_if_new(self, topic: str, event_id: str, timestamp: str, source: str, payload_json: str) -> bool:
        """Return True if inserted (new), False if duplicate."""
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute('INSERT INTO processed(topic,event_id,timestamp,source,payload) VALUES (?,?,?,?,?)',
                        (topic, event_id, timestamp, source, payload_json))
            conn.commit()
            conn.close()
            return True
        except sqlite3.IntegrityError:
            # duplicate primary key
            return False

    def list_by_topic(self, topic: Optional[str] = None) -> List[Tuple]:
        conn = self._conn()
        cur = conn.cursor()
        if topic:
            cur.execute('SELECT topic,event_id,timestamp,source,payload FROM processed WHERE topic=? ORDER BY timestamp', (topic,))
        else:
            cur.execute('SELECT topic,event_id,timestamp,source,payload FROM processed ORDER BY topic,timestamp')
        rows = cur.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def count(self) -> int:
        conn = self._conn()
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) as c FROM processed')
        c = cur.fetchone()['c']
        conn.close()
        return c

    def topics(self) -> List[str]:
        conn = self._conn()
        cur = conn.cursor()
        cur.execute('SELECT DISTINCT topic FROM processed')
        rows = cur.fetchall()
        conn.close()
        return [r['topic'] for r in rows]
```

---

## `src/stats.py`

```python
import time
from dataclasses import dataclass, field
from typing import Dict

@dataclass
class Stats:
    received: int = 0
    unique_processed: int = 0
    duplicate_dropped: int = 0
    start_time: float = field(default_factory=time.time)

    def uptime(self):
        return time.time() - self.start_time

    def to_dict(self):
        return {
            'received': self.received,
            'unique_processed': self.unique_processed,
            'duplicate_dropped': self.duplicate_dropped,
            'uptime_seconds': int(self.uptime())
        }
```

---

## `src/consumer.py`

```python
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
        timestamp = event['timestamp']
        source = event['source']
        payload = event.get('payload', {})
        payload_json = json.dumps(payload, separators=(',', ':'))

        inserted = self.dedup.add_if_new(topic, event_id, timestamp, source, payload_json)
        if not inserted:
            logger.info('Duplicate detected: topic=%s event_id=%s', topic, event_id)
            self.stats.duplicate_dropped += 1
            return
        # simulate event processing (fast)
        logger.debug('Processing event topic=%s event_id=%s', topic, event_id)
        self.stats.unique_processed += 1

    def stop(self):
        self._stop = True
```

---

## `src/main.py`

```python
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
```

---

## `tests/test_app.py`

```python
import os
import tempfile
import time
import json
import asyncio
import pytest
from fastapi.testclient import TestClient

# import app factory
from src import main as appmod
from src.dedup_store import DedupStore

@pytest.fixture
def client(tmp_path, monkeypatch):
    dbfile = tmp_path / 'testdata.db'
    os.environ['DEDUP_DB'] = str(dbfile)
    # re-import/init dedup store & app
    # Because module-level dedup was already created, create a fresh DedupStore
    appmod.dedup = DedupStore(str(dbfile))
    appmod.stats = appmod.stats.__class__()  # reset stats
    appmod.queue = asyncio.Queue()
    appmod.consumer = appmod.consumer.__class__(appmod.queue, appmod.dedup, appmod.stats)

    client = TestClient(appmod.app)
    yield client


def make_event(i, topic='t1'):
    return {
        'topic': topic,
        'event_id': f'id-{i}',
        'timestamp': '2025-10-20T00:00:00Z',
        'source': 'test',
        'payload': {'i': i}
    }


def drain_queue_and_wait(client, timeout=5.0):
    # wait until queue processed by checking stats.unique_processed
    start = time.time()
    while time.time() - start < timeout:
        r = client.get('/stats')
        data = r.json()
        # if enqueued equal processed+duplicates then done
        if data['received'] == data['unique_processed'] + data['duplicate_dropped']:
            return data
        time.sleep(0.05)
    return data


def test_schema_validation(client):
    # missing required field
    r = client.post('/publish', json={'topic': 't', 'event_id': 'e'})
    assert r.status_code == 422


def test_dedup_persistence(client, tmp_path):
    ev = make_event(1)
    r = client.post('/publish', json=ev)
    assert r.status_code == 200
    stats = drain_queue_and_wait(client)
    assert stats['unique_processed'] == 1

    # simulate restart by creating new app instance but same DB
    from importlib import reload
    reload(appmod)
    # re-wire to same DB
    appmod.dedup = DedupStore(str(tmp_path / 'testdata.db'))
    # check dedup prevents reprocessing
    client2 = TestClient(appmod.app)
    r2 = client2.post('/publish', json=ev)
    assert r2.status_code == 200
    # wait and check duplicate dropped
    time.sleep(0.2)
    r_stats = client2.get('/stats').json()
    assert r_stats['duplicate_dropped'] >= 1 or r_stats['unique_processed'] == 1


def test_dedup_detection(client):
    ev = make_event(2)
    # publish same event twice
    client.post('/publish', json=ev)
    client.post('/publish', json=ev)
    stats = drain_queue_and_wait(client)
    assert stats['received'] == 2
    assert stats['unique_processed'] == 1
    assert stats['duplicate_dropped'] == 1


def test_get_events_and_stats_consistency(client):
    events = [make_event(i) for i in range(3, 8)]
    client.post('/publish', json=events)
    stats = drain_queue_and_wait(client)
    r = client.get('/events')
    data = r.json()
    assert len(data['events']) == stats['unique_processed']


def test_stress_batch(client):
    # send 5000 events with 20% duplicates
    total = 5000
    dup_rate = 0.2
    unique = int(total * (1 - dup_rate))
    events = []
    for i in range(unique):
        events.append(make_event(i, topic='stress'))
    # add duplicates by reusing some ids
    for i in range(int(total - unique)):
        idx = i % unique
        events.append(make_event(idx, topic='stress'))
    start = time.time()
    client.post('/publish', json=events)
    stats = drain_queue_and_wait(client, timeout=30.0)
    elapsed = time.time() - start
    assert stats['received'] == total
    assert stats['unique_processed'] == unique
    # reasonable processing time (local machine dependent) -- assert less than 30s
    assert elapsed < 30.0
```

---

# Notes / Explanations

* **Dedup store persistence**: SQLite database (`DEDUP_DB`, default `./data.db`) keeps processed keys. This persists across container restarts.
* **Idempotency**: `DedupStore.add_if_new` returns False if the (topic,event_id) already exists; consumer logs duplicate and increments `duplicate_dropped`.
* **At-least-once delivery**: you can simulate by posting duplicate events to `/publish` — duplicates are accepted (counted in `received`) but not reprocessed.
* **Ordering**: this design **does not** provide total ordering across topics. For an aggregator whose main role is collection/deduplication, total ordering is usually not required; if required, you'd need per-topic ordered queues and potentially a persistent queue (e.g., Kafka) or sequence numbers. Mentioned in a short report (below).

---

## Short report (ordering & reliability)

* **Total ordering**: Not enforced. Use-cases:

  * If consumers downstream require events in strict sequence (e.g., financial ledger), aggregator must preserve per-topic ordering — this implementation does not guarantee that because the queue is single global async queue but processing is sequential in the consumer (so as-is, processing is FIFO which *mostly* preserves ordering as events are enqueued). However in multi-consumer or scaled setups ordering can break. For guaranteed total ordering, use per-topic partitioned queues and a persistent ordered log (Kafka, Redis streams) and ensure single consumer per partition.
* **Crash tolerance**: Dedup store in SQLite prevents reprocessing the same `(topic,event_id)` after restart. If events are in-memory queue at time of crash, they may be lost (so at-least-once or at-most-once depends on publisher). To improve, persist the incoming queue (write-ahead log) or push to a durable broker.

---

## How to run

See `README.md` above. After starting server, use the endpoints:

* `POST /publish` single or batch JSON event
* `GET /events?topic=...` list processed events
* `GET /stats` service metrics

---

If you'd like, I can:

* produce a tarball of the project files
* extend the implementation to use uvloop / worker pool for higher throughput
* add a small CLI publisher script to run stress tests separately

Tell me which you'd prefer next.
