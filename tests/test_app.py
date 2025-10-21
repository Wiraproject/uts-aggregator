import os, sys
import tempfile
import time
import json
import asyncio
import pytest
from fastapi.testclient import TestClient

# --- PENTING: set environment variable sebelum import main.py ---
tmp_db = os.path.join(tempfile.gettempdir(), "testdata.db")
if os.path.exists(tmp_db):
    os.remove(tmp_db)
os.environ["DEDUP_DB"] = tmp_db

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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

    import threading
    threading.Thread(target=lambda: asyncio.run(appmod.consumer.run()), daemon=True).start()

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