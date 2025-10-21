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