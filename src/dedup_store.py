import sqlite3
import threading
from typing import Optional, List, Tuple

class DedupStore:
    def __init__(self, db_path: str = './data.db'):
        self.db_path = db_path
        self._ddl_lock = threading.Lock()
        self._ensure_tables()

    def _conn(self):
        conn = sqlite3.connect(
            self.db_path, 
            check_same_thread=False,
            timeout=30.0 
        )
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=30000')
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
            cur.execute('''
                CREATE INDEX IF NOT EXISTS idx_topic_timestamp 
                ON processed(topic, timestamp)
            ''')
            conn.commit()
            conn.close()

    def exists(self, topic: str, event_id: str) -> bool:
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(
                'SELECT 1 FROM processed WHERE topic=? AND event_id=? LIMIT 1',
                (topic, event_id)
            )
            result = cur.fetchone() is not None
            return result
        finally:
            conn.close()

    def add_if_new(self, topic: str, event_id: str, timestamp: str, source: str, payload_json: str) -> bool:
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO processed(topic,event_id,timestamp,source,payload) VALUES (?,?,?,?,?)',
                (topic, event_id, timestamp, source, payload_json)
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
        except Exception as e:
            import logging
            logger = logging.getLogger('dedup_store')
            logger.error(f'Database error in add_if_new: {e}')
            return False
        finally:
            conn.close()

    def list_by_topic(self, topic: Optional[str] = None) -> List[Tuple]:
        conn = self._conn()
        try:
            cur = conn.cursor()
            if topic:
                cur.execute(
                    'SELECT topic,event_id,timestamp,source,payload FROM processed WHERE topic=? ORDER BY timestamp',
                    (topic,)
                )
            else:
                cur.execute(
                    'SELECT topic,event_id,timestamp,source,payload FROM processed ORDER BY topic,timestamp'
                )
            rows = cur.fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def count(self) -> int:
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) as c FROM processed')
            c = cur.fetchone()['c']
            return c
        finally:
            conn.close()

    def topics(self) -> List[str]:
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute('SELECT DISTINCT topic FROM processed')
            rows = cur.fetchall()
            return [r['topic'] for r in rows]
        finally:
            conn.close()