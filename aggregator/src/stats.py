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