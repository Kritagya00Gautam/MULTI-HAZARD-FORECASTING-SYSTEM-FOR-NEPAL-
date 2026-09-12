from __future__ import annotations
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

    def __init__(self, dsn: str):
        self.dsn = dsn

   

    @abstractmethod
    def fetch(self, window_start: datetime, window_end: datetime) -> Iterable[Fetched]:
        """Hit the upstream source. Yield raw responses. Do NOT parse here."""

    @abstractmethod
    def parse(self, payload: Fetched) -> Iterator[dict | Reject]:
      """Turn one raw response into normalised records (or Rejects)."""
      

    @abstractmethod
    def write(self, cur, records: list[dict], run_id: str) -> int:
        """Idempotent upsert into silver. Return rows written."""

    def run(self, window_start: datetime | None = None,
                window_end: datetime | None = None) -> RunStats:
            stats = RunStats()
            with psycopg.connect(self.dsn, row_factory=dict_row) as conn:
                window_end = window_end or datetime.now(UTC)
                window_start = window_start or self._resume_point(conn, window_end)
    
                run_id = self._open_run(conn, window_start, window_end)
                log.info("[%s] run %s window %s → %s",
                         self.source_id, run_id, window_start, window_end)
                try:
                    for payload in self.fetch(window_start, window_end):
                        stats.fetched += 1
                        with conn.cursor() as cur:
                            is_new = self._archive(cur, run_id, payload)
                        conn.commit()
                        if not is_new:
                            log.info("[%s] payload unchanged, skipping parse", self.source_id)
                            continue
    
                        records: list[dict] = []
                        for item in self.parse(payload):
                            if isinstance(item, Reject):
                                stats.rejects.append(item)
                            else:
                                records.append(item)
    
                        if records:
                            with conn.cursor() as cur:
                                stats.written += self.write(cur, records, run_id)
                            conn.commit()
    
                    with conn.cursor() as cur:
                        for rj in stats.rejects:
                            cur.execute(
                                "INSERT INTO meta.reject (run_id, source_id, reason, payload)"
                                " VALUES (%s,%s,%s,%s)",
                                (run_id, self.source_id, rj.reason, Jsonb(rj.payload)),
                            )
                        stats.rejected = len(stats.rejects)
                        self._advance_watermark(cur, window_end)
                        self._close_run(cur, run_id, "success", stats)
                    conn.commit()
    
                except Exception as exc:                      # noqa: BLE001
                    conn.rollback()
                    with conn.cursor() as cur:
                        self._close_run(cur, run_id, "failed", stats, error=repr(exc))
                    conn.commit()
                    log.exception("[%s] run failed", self.source_id)
                    raise
    
            log.info("[%s] fetched=%d written=%d rejected=%d",
                     self.source_id, stats.fetched, stats.written, stats.rejected)
            return stats
    
        # ---- helpers ---------------------------------------------------------
    
    def http_get(self, url: str, *, params: dict | None = None,
                     retries: int = 4, timeout: float = 45.0,
                     as_json: bool = True) -> Fetched:
            """GET with exponential backoff. Every connector should use this."""
            delay = 2.0
            last: Exception | None = None
            headers = {"User-Agent": "nepal-hazard-forecast/0.1 (research)"}
            for attempt in range(retries):
                try:
                    r = httpx.get(url, params=params, timeout=timeout,
                                  headers=headers, follow_redirects=True)
                    r.raise_for_status()
                    return Fetched(
                        url=str(r.url),
                        status=r.status_code,
                        content_type=r.headers.get("content-type", ""),
                        body_json=r.json() if as_json else None,
                        body_text=None if as_json else r.text,
                    )
                except Exception as exc:                      # noqa: BLE001
                    last = exc
                    log.warning("[%s] GET failed (%d/%d): %s",
                                self.source_id, attempt + 1, retries, exc)
                    time.sleep(delay)
                    delay *= 2
            raise RuntimeError(f"{self.source_id}: GET {url} failed after {retries}") from last
    
    def _resume_point(self, conn, window_end: datetime) -> datetime:
            with conn.cursor() as cur:
                cur.execute("SELECT watermark FROM meta.ingest_state WHERE source_id=%s",
                            (self.source_id,))
                row = cur.fetchone()
            if row:
                return row["watermark"] - self.overlap
            return window_end - self.cold_start
    
    def _open_run(self, conn, ws: datetime, we: datetime) -> str:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO meta.ingest_run (source_id, window_start, window_end, code_version)"
                    " VALUES (%s,%s,%s,%s) RETURNING run_id",
                    (self.source_id, ws, we, CODE_VERSION),
                )
                run_id = cur.fetchone()["run_id"]
            conn.commit()
            return run_id
    
    def _close_run(self, cur, run_id, status, stats: RunStats, error: str | None = None):
            cur.execute(
                "UPDATE meta.ingest_run SET finished_at=now(), status=%s,"
                " rows_fetched=%s, rows_written=%s, rows_rejected=%s, error=%s"
                " WHERE run_id=%s",
                (status, stats.fetched, stats.written, len(stats.rejects), error, run_id),
            )
    
    def _archive(self, cur, run_id, p: Fetched) -> bool:
            """Store raw. Returns False if we have seen this exact payload before."""
            cur.execute(
                "INSERT INTO bronze.payload"
                " (run_id, source_id, request_url, http_status, content_type,"
                "  content_sha256, body, body_text)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                " ON CONFLICT (source_id, content_sha256) DO NOTHING"
                " RETURNING payload_id",
                (run_id, self.source_id, p.url, p.status, p.content_type,
                 p.sha256(), Jsonb(p.body_json) if p.body_json is not None else None,
                 p.body_text),
            )
            return cur.fetchone() is not None
    
    def _advance_watermark(self, cur, to: datetime):
            cur.execute(
                "INSERT INTO meta.ingest_state (source_id, watermark) VALUES (%s,%s)"
                " ON CONFLICT (source_id) DO UPDATE"
                " SET watermark=EXCLUDED.watermark, updated_at=now()",
                (self.source_id, to),
            )
    
