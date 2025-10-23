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