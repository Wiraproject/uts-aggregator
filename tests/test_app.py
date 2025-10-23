import os, sys
import tempfile
import time
import json
import asyncio
import pytest
from fastapi.testclient import TestClient

current_dir = os.path.dirname(os.path.abspath(__file__))
aggregator_dir = os.path.join(current_dir, '..', 'aggregator')
sys.path.insert(0, aggregator_dir)

tmp_db = os.path.join(tempfile.gettempdir(), "testdata.db")
if os.path.exists(tmp_db):
    os.remove(tmp_db)
os.environ["DEDUP_DB"] = tmp_db

from src import main as appmod
from src.dedup_store import DedupStore

@pytest.fixture
def client(tmp_path, monkeypatch):
    dbfile = tmp_path / 'testdata.db'
    os.environ['DEDUP_DB'] = str(dbfile)
    appmod.dedup = DedupStore(str(dbfile))
    appmod.stats = appmod.stats.__class__()
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
    start = time.time()
    while time.time() - start < timeout:
        r = client.get('/stats')
        data = r.json()
        if data['received'] == data['unique_processed'] + data['duplicate_dropped']:
            return data
        time.sleep(0.05)
    return data

def test_schema_validation(client):
    r = client.post('/publish', json={'topic': 't', 'event_id': 'e'})
    assert r.status_code == 422

def test_dedup_persistence(tmp_path):
    db_path = str(tmp_path / 'test.db')

    store1 = DedupStore(db_path)
    inserted = store1.add_if_new('t1', 'id-1', '2025-01-01', 'test', '{}')
    assert inserted == True

    store2 = DedupStore(db_path)
    inserted = store2.add_if_new('t1', 'id-1', '2025-01-01', 'test', '{}')
    assert inserted == False

    assert store2.count() == 1

def test_dedup_detection(client):
    ev = make_event(2)
    client.post('/publish', json=ev)
    client.post('/publish', json=ev)
    stats = drain_queue_and_wait(client)
    assert stats['received'] == 2
    assert stats['unique_processed'] == 1
    assert stats['duplicate_dropped'] == 1

def test_get_events_by_topic(client):
    client.post('/publish', json=make_event(100, 'topic-a'))
    client.post('/publish', json=make_event(101, 'topic-a'))
    client.post('/publish', json=make_event(102, 'topic-b'))
    
    drain_queue_and_wait(client)
    
    r = client.get('/events?topic=topic-a')
    data = r.json()
    assert len(data['events']) == 2
    assert all(e['topic'] == 'topic-a' for e in data['events'])
    
    r = client.get('/events?topic=topic-b')
    data = r.json()
    assert len(data['events']) == 1
    assert data['events'][0]['topic'] == 'topic-b'
    
def test_get_events_and_stats_consistency(client):
    events = [make_event(i) for i in range(3, 8)]
    client.post('/publish', json=events)
    stats = drain_queue_and_wait(client)
    r = client.get('/events')
    data = r.json()
    assert len(data['events']) == stats['unique_processed']

def test_stress_batch(client):
    total = 5000
    dup_rate = 0.2
    unique = int(total * (1 - dup_rate))
    events = []
    for i in range(unique):
        events.append(make_event(i, topic='stress'))
    for i in range(int(total - unique)):
        idx = i % unique
        events.append(make_event(idx, topic='stress'))
    start = time.time()
    client.post('/publish', json=events)
    stats = drain_queue_and_wait(client, timeout=30.0)
    elapsed = time.time() - start
    assert stats['received'] == total
    assert stats['unique_processed'] == unique
    assert elapsed < 30.0