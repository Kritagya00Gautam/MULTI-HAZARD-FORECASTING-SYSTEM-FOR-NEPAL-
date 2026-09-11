from future import annotations
import hashlib
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Iterator
import httpx
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

log = logging.getLogger(__name__)
UTC = timezone.utc
CODE_VERSION = os.getenv("GIT_SHA", "dev")

@dataclass
class Fetched:
    url : str
    stats: int
    content_type: str
    body_json: Any | None = None
    body_text: str | None = None

    def sha256(self) -> str:
        raw = (
            json.dumps(self.body_json, sort_keys = True)
            if self.body_json is not None
            else(self.body_text or "")
        )
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()
@dataclass
class Reject:
    reason : str
    payload : dict

@dataclass
class RunStats:
    fetched: int = 0
    written: int = 0
    rejected: int = 0
    rejects: list[Reject] = field(default_factory=list)

class Connector(ABC):
    source_id : str
    overlap: timedelta = timedelta(hours=6)
    cold_start : timedelta = timedelta(days = 30)
    