#!/usr/bin/env python3
"""Minimal backend for Lovable event ingestion and admin review.

It accepts the existing /__l5e/trackevents and /__l5e/replay payloads,
stores them in SQLite, and serves a lightweight admin UI at /admin.
"""

from __future__ import annotations

import json
import mimetypes
import os
import sqlite3
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlparse


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "responses.sqlite3"
ADMIN_HTML_PATH = ROOT / "admin.html"
INDEX_HTML_PATH = ROOT / "index.html"
MAX_BODY_BYTES = 20 * 1024 * 1024


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def ensure_database() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS batches (
                batch_id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                sent_at TEXT,
                received_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                event_name TEXT NOT NULL,
                event_schema_version INTEGER,
                event_time TEXT,
                anonymous_id TEXT,
                session_id TEXT,
                page_view_id TEXT,
                replay_id TEXT,
                trace_id TEXT,
                span_id TEXT,
                parent_span_id TEXT,
                choice_summary TEXT,
                properties_json TEXT NOT NULL,
                received_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                anonymous_id TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                last_page_view_id TEXT,
                last_path TEXT,
                event_count INTEGER NOT NULL DEFAULT 0,
                choice_count INTEGER NOT NULL DEFAULT 0,
                form_submit_count INTEGER NOT NULL DEFAULT 0,
                latest_choice_text TEXT,
                latest_choice_event_id TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS replays (
                replay_id TEXT NOT NULL,
                chunk_sequence INTEGER NOT NULL,
                chunk_id TEXT,
                session_id TEXT,
                page_view_id TEXT,
                anonymous_id TEXT,
                sent_at TEXT,
                received_at TEXT NOT NULL,
                event_count INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (replay_id, chunk_sequence)
            );

            CREATE INDEX IF NOT EXISTS idx_events_session_time
            ON events(session_id, event_time);

            CREATE INDEX IF NOT EXISTS idx_events_name_time
            ON events(event_name, event_time);

            CREATE INDEX IF NOT EXISTS idx_sessions_last_seen
            ON sessions(last_seen_at DESC);

            CREATE INDEX IF NOT EXISTS idx_replays_session_time
            ON replays(session_id, received_at);
            """
        )


def open_database() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def read_body(handler: BaseHTTPRequestHandler) -> bytes:
    length = int(handler.headers.get("content-length") or 0)
    if length > MAX_BODY_BYTES:
        raise ValueError("Request body too large")
    return handler.rfile.read(length) if length else b""


def parse_json_body(handler: BaseHTTPRequestHandler) -> Any:
    body = read_body(handler)
    if not body:
        return None
    return json.loads(body.decode("utf-8"))


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    handler.send_response(status)
    handler.send_header("content-type", "application/json; charset=utf-8")
    handler.send_header("content-length", str(len(data)))
    handler.send_header("cache-control", "no-store")
    handler.end_headers()
    handler.wfile.write(data)


def text_response(handler: BaseHTTPRequestHandler, status: int, text: str, content_type: str = "text/plain; charset=utf-8") -> None:
    data = text.encode("utf-8")
    handler.send_response(status)
    handler.send_header("content-type", content_type)
    handler.send_header("content-length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def load_file(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def is_choice_event(event: Dict[str, Any]) -> bool:
    return event.get("event_name") in {"lovable.interaction", "lovable.form_submitted"}


def as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def first_non_empty(*values: Any) -> Optional[str]:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int, float, bool)):
            return str(value)
    return None


def summarize_choice(event: Dict[str, Any]) -> str:
    properties = as_dict(event.get("properties"))
    event_name = event.get("event_name") or "event"
    if event_name == "lovable.form_submitted":
        values = properties.get("values")
        if isinstance(values, dict) and values:
            parts = []
            for key in sorted(values):
                item = values[key]
                if isinstance(item, (dict, list)):
                    rendered = json.dumps(item, ensure_ascii=False, separators=(",", ":"))
                else:
                    rendered = str(item)
                parts.append(f"{key}={rendered}")
            return "; ".join(parts)

    if isinstance(properties.get("isChecked"), bool) and isinstance(properties.get("text"), str):
        state = "checked" if properties["isChecked"] else "unchecked"
        return f"{properties['text']} ({state})"

    candidate = first_non_empty(
        properties.get("choice"),
        properties.get("selected_value"),
        properties.get("value"),
        properties.get("text"),
        properties.get("label"),
        properties.get("option"),
        properties.get("answer"),
        properties.get("target_text"),
        properties.get("target_selector"),
        properties.get("selector"),
    )
    if candidate:
        return candidate

    if properties:
        return json.dumps(properties, ensure_ascii=False, separators=(",", ":"))
    return event_name


def normalise_event(raw_event: Dict[str, Any], received_at: str) -> Dict[str, Any]:
    properties = as_dict(raw_event.get("properties"))
    event = {
        "event_id": raw_event.get("event_id") or raw_event.get("id") or f"missing-{received_at}",
        "event_name": raw_event.get("event_name") or raw_event.get("name") or "unknown",
        "event_schema_version": raw_event.get("event_schema_version"),
        "event_time": raw_event.get("event_time") or raw_event.get("sent_at") or received_at,
        "anonymous_id": raw_event.get("anonymous_id"),
        "session_id": raw_event.get("session_id"),
        "page_view_id": raw_event.get("page_view_id"),
        "replay_id": raw_event.get("replay_id"),
        "trace_id": raw_event.get("trace_id"),
        "span_id": raw_event.get("span_id"),
        "parent_span_id": raw_event.get("parent_span_id"),
        "choice_summary": summarize_choice(raw_event) if is_choice_event(raw_event) else None,
        "properties_json": json.dumps(properties, ensure_ascii=False, separators=(",", ":")),
        "received_at": received_at,
    }
    if event["event_name"] == "lovable.page_viewed" and isinstance(properties.get("url_path"), str):
        event["page_path"] = properties["url_path"]
    return event


def upsert_session(conn: sqlite3.Connection, event: Dict[str, Any]) -> None:
    session_id = event.get("session_id")
    if not session_id:
        return

    properties = json.loads(event["properties_json"])
    path = properties.get("url_path") if isinstance(properties, dict) else None
    choice_summary = event.get("choice_summary")
    now = event["received_at"]
    is_choice = bool(choice_summary)
    is_form = event["event_name"] == "lovable.form_submitted"

    conn.execute(
        """
        INSERT INTO sessions (
            session_id, anonymous_id, first_seen_at, last_seen_at,
            last_page_view_id, last_path, event_count, choice_count,
            form_submit_count, latest_choice_text, latest_choice_event_id, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
        ON CONFLICT(session_id) DO UPDATE SET
            anonymous_id = COALESCE(excluded.anonymous_id, sessions.anonymous_id),
            last_seen_at = excluded.last_seen_at,
            last_page_view_id = COALESCE(excluded.last_page_view_id, sessions.last_page_view_id),
            last_path = COALESCE(excluded.last_path, sessions.last_path),
            event_count = sessions.event_count + 1,
            choice_count = sessions.choice_count + excluded.choice_count,
            form_submit_count = sessions.form_submit_count + excluded.form_submit_count,
            latest_choice_text = COALESCE(excluded.latest_choice_text, sessions.latest_choice_text),
            latest_choice_event_id = COALESCE(excluded.latest_choice_event_id, sessions.latest_choice_event_id),
            updated_at = excluded.updated_at
        """,
        (
            session_id,
            event.get("anonymous_id"),
            now,
            now,
            event.get("page_view_id"),
            path,
            1 if is_choice else 0,
            1 if is_form else 0,
            choice_summary,
            event.get("event_id") if is_choice else None,
            now,
        ),
    )


def store_track_batch(conn: sqlite3.Connection, payload: Dict[str, Any]) -> int:
    received_at = utc_now()
    batch_id = payload.get("batch_id") or payload.get("id") or f"batch-{received_at}"
    sent_at = payload.get("sent_at")
    events = payload.get("events") if isinstance(payload.get("events"), list) else []
    conn.execute(
        "INSERT OR REPLACE INTO batches (batch_id, kind, sent_at, received_at, payload_json) VALUES (?, 'track', ?, ?, ?)",
        (str(batch_id), sent_at, received_at, json.dumps(payload, ensure_ascii=False, separators=(",", ":"))),
    )

    inserted = 0
    for raw_event in events:
        if not isinstance(raw_event, dict):
            continue
        event = normalise_event(raw_event, received_at)
        conn.execute(
            """
            INSERT OR REPLACE INTO events (
                event_id, event_name, event_schema_version, event_time, anonymous_id,
                session_id, page_view_id, replay_id, trace_id, span_id, parent_span_id,
                choice_summary, properties_json, received_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event["event_id"],
                event["event_name"],
                event["event_schema_version"],
                event["event_time"],
                event["anonymous_id"],
                event["session_id"],
                event["page_view_id"],
                event["replay_id"],
                event["trace_id"],
                event["span_id"],
                event["parent_span_id"],
                event["choice_summary"],
                event["properties_json"],
                event["received_at"],
            ),
        )
        upsert_session(conn, event)
        inserted += 1
    return inserted


def store_replay_batch(conn: sqlite3.Connection, payload: Dict[str, Any]) -> int:
    received_at = utc_now()
    replay_id = str(payload.get("replay_id") or payload.get("chunk_id") or f"replay-{received_at}")
    chunk_sequence = int(payload.get("chunk_sequence") or 0)
    events = payload.get("events") if isinstance(payload.get("events"), list) else []
    conn.execute(
        """
        INSERT OR REPLACE INTO replays (
            replay_id, chunk_sequence, chunk_id, session_id, page_view_id, anonymous_id,
            sent_at, received_at, event_count, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            replay_id,
            chunk_sequence,
            payload.get("chunk_id"),
            payload.get("session_id"),
            payload.get("page_view_id"),
            payload.get("anonymous_id"),
            payload.get("sent_at"),
            received_at,
            len(events),
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        ),
    )
    return len(events)


def row_to_session_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "session_id": row["session_id"],
        "anonymous_id": row["anonymous_id"],
        "first_seen_at": row["first_seen_at"],
        "last_seen_at": row["last_seen_at"],
        "last_page_view_id": row["last_page_view_id"],
        "last_path": row["last_path"],
        "event_count": row["event_count"],
        "choice_count": row["choice_count"],
        "form_submit_count": row["form_submit_count"],
        "latest_choice_text": row["latest_choice_text"],
        "latest_choice_event_id": row["latest_choice_event_id"],
    }


def fetch_sessions(conn: sqlite3.Connection, limit: int = 100) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM sessions
        ORDER BY last_seen_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [row_to_session_dict(row) for row in rows]


def fetch_session_events(conn: sqlite3.Connection, session_id: str, limit: int = 200) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT event_id, event_name, event_time, page_view_id, replay_id, choice_summary, properties_json, received_at
        FROM events
        WHERE session_id = ?
        ORDER BY event_time ASC, received_at ASC
        LIMIT ?
        """,
        (session_id, limit),
    ).fetchall()
    results = []
    for row in rows:
        results.append(
            {
                "event_id": row["event_id"],
                "event_name": row["event_name"],
                "event_time": row["event_time"],
                "page_view_id": row["page_view_id"],
                "replay_id": row["replay_id"],
                "choice_summary": row["choice_summary"],
                "properties": json.loads(row["properties_json"]),
                "received_at": row["received_at"],
            }
        )
    return results


def fetch_dashboard_summary(conn: sqlite3.Connection) -> Dict[str, Any]:
    counts = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM sessions) AS session_count,
            (SELECT COUNT(*) FROM events) AS event_count,
            (SELECT COUNT(*) FROM replays) AS replay_count,
            (SELECT COUNT(*) FROM events WHERE event_name IN ('lovable.interaction', 'lovable.form_submitted')) AS choice_event_count
        """
    ).fetchone()
    return {
        "session_count": counts["session_count"],
        "event_count": counts["event_count"],
        "replay_count": counts["replay_count"],
        "choice_event_count": counts["choice_event_count"],
    }


def serve_static_file(handler: BaseHTTPRequestHandler, relative_path: str) -> bool:
    candidate = (ROOT / relative_path.lstrip("/")).resolve()
    if ROOT not in candidate.parents and candidate != ROOT:
        return False
    if not candidate.exists() or not candidate.is_file():
        return False
    content = candidate.read_bytes()
    content_type, _ = mimetypes.guess_type(str(candidate))
    handler.send_response(HTTPStatus.OK)
    handler.send_header("content-type", content_type or "application/octet-stream")
    handler.send_header("content-length", str(len(content)))
    handler.send_header("cache-control", "no-store")
    handler.end_headers()
    handler.wfile.write(content)
    return True


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "LovableBackend/1.0"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header("access-control-allow-origin", "*")
        self.send_header("access-control-allow-methods", "GET,POST,OPTIONS")
        self.send_header("access-control-allow-headers", "content-type")
        self.end_headers()

    def do_HEAD(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        if path in {"/", "/admin"}:
            target = "index.html" if path == "/" else "admin.html"
            candidate = (ROOT / target).resolve()
            if candidate.exists() and candidate.is_file():
                content_type, _ = mimetypes.guess_type(str(candidate))
                self.send_response(HTTPStatus.OK)
                self.send_header("content-type", content_type or "text/html; charset=utf-8")
                self.send_header("cache-control", "no-store")
                self.end_headers()
                return
        if path == "/api/health":
            self.send_response(HTTPStatus.OK)
            self.send_header("content-type", "application/json; charset=utf-8")
            self.send_header("cache-control", "no-store")
            self.end_headers()
            return
        self.send_response(HTTPStatus.NOT_FOUND)
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        if path == "/":
            if INDEX_HTML_PATH.exists():
                return serve_static_file(self, "index.html")
            return text_response(self, HTTPStatus.OK, "Backend is running. Open /admin for the dashboard.\n")
        if path == "/admin":
            return serve_static_file(self, "admin.html")
        if path == "/api/health":
            return json_response(self, HTTPStatus.OK, {"ok": True, "time": utc_now()})
        if path == "/api/sessions":
            with open_database() as conn:
                payload = {
                    "summary": fetch_dashboard_summary(conn),
                    "sessions": fetch_sessions(conn, limit=200),
                }
            return json_response(self, HTTPStatus.OK, payload)
        if path.startswith("/api/sessions/"):
            session_id = path.removeprefix("/api/sessions/")
            with open_database() as conn:
                session = conn.execute("SELECT * FROM sessions WHERE session_id = ?", (session_id,)).fetchone()
                if not session:
                    return json_response(self, HTTPStatus.NOT_FOUND, {"error": "session_not_found"})
                payload = {
                    "session": row_to_session_dict(session),
                    "events": fetch_session_events(conn, session_id),
                }
            return json_response(self, HTTPStatus.OK, payload)
        if path == "/api/responses":
            with open_database() as conn:
                rows = conn.execute(
                    """
                    SELECT event_id, event_time, properties_json, received_at
                    FROM events
                    WHERE event_name = 'lovable.form_submitted'
                    ORDER BY event_time DESC, received_at DESC
                    """
                ).fetchall()
            responses = []
            for row in rows:
                properties = json.loads(row["properties_json"])
                values = properties.get("values") if isinstance(properties, dict) else None
                values = values if isinstance(values, dict) else {}
                responses.append(
                    {
                        "event_id": row["event_id"],
                        "received_at": row["received_at"],
                        "name": values.get("name"),
                        "message": values.get("message"),
                        "activities": values.get("activities"),
                        "details": values.get("details"),
                    }
                )
            return json_response(self, HTTPStatus.OK, {"responses": responses})

        if path == "/api/export":
            with open_database() as conn:
                rows = conn.execute("SELECT * FROM events ORDER BY event_time ASC, received_at ASC").fetchall()
                events = []
                for row in rows:
                    events.append({
                        "event_id": row["event_id"],
                        "event_name": row["event_name"],
                        "event_time": row["event_time"],
                        "anonymous_id": row["anonymous_id"],
                        "session_id": row["session_id"],
                        "page_view_id": row["page_view_id"],
                        "replay_id": row["replay_id"],
                        "choice_summary": row["choice_summary"],
                        "properties": json.loads(row["properties_json"]),
                        "received_at": row["received_at"],
                    })
            return json_response(self, HTTPStatus.OK, {"events": events})

        if serve_static_file(self, path.lstrip("/")):
            return
        return text_response(self, HTTPStatus.NOT_FOUND, "Not found\n")

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = unquote(parsed.path)
        try:
            payload = parse_json_body(self)
        except json.JSONDecodeError:
            return json_response(self, HTTPStatus.BAD_REQUEST, {"error": "invalid_json"})
        except ValueError as exc:
            return json_response(self, HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": str(exc)})

        if path == "/__l5e/trackevents":
            if not isinstance(payload, dict):
                return json_response(self, HTTPStatus.BAD_REQUEST, {"error": "expected_object"})
            with open_database() as conn:
                inserted = store_track_batch(conn, payload)
                conn.commit()
            return json_response(self, HTTPStatus.OK, {"ok": True, "events_stored": inserted})

        if path == "/__l5e/replay":
            if not isinstance(payload, dict):
                return json_response(self, HTTPStatus.BAD_REQUEST, {"error": "expected_object"})
            with open_database() as conn:
                event_count = store_replay_batch(conn, payload)
                conn.commit()
            return json_response(self, HTTPStatus.OK, {"ok": True, "events_stored": event_count})

        return json_response(self, HTTPStatus.NOT_FOUND, {"error": "not_found"})


def main() -> None:
    ensure_database()
    port = int(os.environ.get("PORT", "8787"))
    server = ThreadingHTTPServer(("0.0.0.0", port), RequestHandler)
    print(f"Serving on http://127.0.0.1:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()