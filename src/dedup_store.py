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

    def exists(self, topic: str, event_id: str) -> bool:
        """
        Check if an event already exists in the store.
        
        Args:
            topic: Event topic
            event_id: Event ID
            
        Returns:
            True if exists, False otherwise
        """
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            'SELECT 1 FROM processed WHERE topic=? AND event_id=? LIMIT 1',
            (topic, event_id)
        )
        result = cur.fetchone() is not None
        conn.close()
        return result

    def add_if_new(self, topic: str, event_id: str, timestamp: str, source: str, payload_json: str) -> bool:
        """
        Insert event if new, return False if duplicate.
        
        Args:
            topic: Event topic
            event_id: Event ID
            timestamp: ISO8601 timestamp
            source: Event source
            payload_json: JSON string of payload
            
        Returns:
            True if inserted (new), False if duplicate
        """
        try:
            conn = self._conn()
            cur = conn.cursor()
            cur.execute(
                'INSERT INTO processed(topic,event_id,timestamp,source,payload) VALUES (?,?,?,?,?)',
                (topic, event_id, timestamp, source, payload_json)
            )
            conn.commit()
            conn.close()
            return True
        except sqlite3.IntegrityError:
            # duplicate primary key
            return False

    def list_by_topic(self, topic: Optional[str] = None) -> List[Tuple]:
        """
        List all events, optionally filtered by topic.
        
        Args:
            topic: Optional topic filter
            
        Returns:
            List of event dictionaries
        """
        conn = self._conn()
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
        conn.close()
        return [dict(row) for row in rows]

    def count(self) -> int:
        """
        Get total count of processed events.
        
        Returns:
            Total number of events in store
        """
        conn = self._conn()
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) as c FROM processed')
        c = cur.fetchone()['c']
        conn.close()
        return c

    def topics(self) -> List[str]:
        """
        Get list of all unique topics.
        
        Returns:
            List of topic names
        """
        conn = self._conn()
        cur = conn.cursor()
        cur.execute('SELECT DISTINCT topic FROM processed')
        rows = cur.fetchall()
        conn.close()
        return [r['topic'] for r in rows]