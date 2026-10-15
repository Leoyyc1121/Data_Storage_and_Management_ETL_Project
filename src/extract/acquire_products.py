"""Task 2: restartable acquisition manifest, policy-gated HTTP and authorized snapshots.

No product HTTP request is made without BOTH permission evidence and robots approval.
The run subcommand organizes prepare -> crawl -> export -> verification.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
from html.parser import HTMLParser
import json
import logging
import math
import os
from pathlib import Path
import re
import socket
import ssl
import sqlite3
import sys
import time
from urllib import error, request
from urllib.parse import quote, urljoin, urlsplit
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
from protego import Protego
import truststore

from register_source import hash_file, utc_now

ROOT = Path(__file__).resolve().parents[2]
VERSION = "1.1.0"
IDENTITY_RULE = "task2-body-asin-and-product-v1"
STATUSES = ("success", "not_found", "restricted_access", "timeout",
            "parsing_failure", "not_collected", "network_error", "http_error")
RETRYABLE = {"timeout", "network_error", "http_error"}
SCHEMA = pa.schema([
    ("asin", pa.string()), ("original_url", pa.string()), ("final_url", pa.string()),
    ("status", pa.string()), ("http_status", pa.int64()), ("attempts", pa.int64()),
    ("acquired_at", pa.string()), ("snapshot_path", pa.string()),
    ("content_hash", pa.string()), ("acquisition_mode", pa.string()),
    ("reason", pa.string()), ("evidence_path", pa.string()),
    ("verified_asin", pa.string()), ("template_signature", pa.string()),
    ("redirect_chain", pa.string()), ("review_count", pa.int64()),
    ("first_review_id", pa.string()), ("source_sha256", pa.string()),
    ("source_run_id", pa.string()), ("last_run_id", pa.string()),
])


def save_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def timestamp(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Evidence timestamp must include a timezone")
    return result


def digest_json(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True).encode()).hexdigest()


def origin(url):
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
        raise ValueError("Only HTTPS URLs without embedded credentials are supported")
    return f"https://{parts.netloc.lower()}"


def load_config(path):
    config = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    config.setdefault("robots_failure_cache_seconds", 30.0)
    config.setdefault("robots_max_attempts", 3)
    for name in ("source_run", "work_dir", "raw_dir", "report_dir"):
        config[name] = str((ROOT / config[name]).resolve())
    config["base_url"] = origin(config["base_url"])
    if config["concurrency"] != 1:
        raise ValueError("This bounded single-worker collector requires concurrency=1")
    for name in ("request_interval_seconds", "timeout_seconds", "max_retry_wait_seconds", "robots_cache_seconds", "robots_failure_cache_seconds"):
        value = config[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"{name} must be finite and positive")
    for name in ("max_attempts", "max_redirects", "max_response_bytes", "robots_max_attempts"):
        if isinstance(config[name], bool) or not isinstance(config[name], int) or config[name] <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if config["max_retry_wait_seconds"] > 60:
        raise ValueError("Retry waits are bounded to 60 seconds")
    if config["robots_agent"] != "ChatGPT-User" or "ChatGPT-User" not in config["user_agent"]:
        raise ValueError("Preserve the AI-assisted collector identity; changing it is not an access workaround")
    for name in ("work_dir", "raw_dir", "report_dir"):
        if Path(config[name]) == ROOT or Path(config[name]) == Path(config["source_run"]):
            raise ValueError("Output paths must not overwrite the project root or Task 1 run")
    return config


@contextmanager
def run_lock(work_dir):
    path = Path(work_dir)
    path.mkdir(parents=True, exist_ok=True)
    stream = (path / "run.lock").open("a+b")
    try:
        if os.name == "nt":
            import msvcrt
            stream.seek(0)
            if not stream.read(1):
                stream.write(b"0")
                stream.flush()
            stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        stream.close()


def open_db(work_dir):
    path = Path(work_dir)
    path.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path / "acquisition.sqlite3")
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA journal_mode=WAL")
    choices = ",".join(repr(value) for value in STATUSES)
    db.executescript(f"""
      CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS products(
        asin TEXT PRIMARY KEY, original_url TEXT NOT NULL, final_url TEXT,
        status TEXT NOT NULL DEFAULT 'not_collected' CHECK(status IN ({choices})),
        http_status INTEGER, attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
        acquired_at TEXT, snapshot_path TEXT, content_hash TEXT,
        acquisition_mode TEXT, reason TEXT, evidence_path TEXT,
        verified_asin TEXT, template_signature TEXT, redirect_chain TEXT,
        review_count INTEGER NOT NULL, first_review_id TEXT,
        source_sha256 TEXT NOT NULL, source_run_id TEXT NOT NULL, last_run_id TEXT);
      CREATE INDEX IF NOT EXISTS products_status ON products(status,asin);
      CREATE TABLE IF NOT EXISTS input_batches(batch_id INTEGER PRIMARY KEY, payload_hash TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS attempts(
        attempt_id TEXT PRIMARY KEY, asin TEXT NOT NULL REFERENCES products(asin),
        run_id TEXT NOT NULL, mode TEXT NOT NULL, started_at TEXT NOT NULL,
        finished_at TEXT, status TEXT, evidence_path TEXT, payload TEXT);
      CREATE INDEX IF NOT EXISTS attempts_asin ON attempts(asin,mode);
      CREATE TABLE IF NOT EXISTS runs(
        run_id TEXT PRIMARY KEY, started_at TEXT NOT NULL, finished_at TEXT,
        phase TEXT NOT NULL, status TEXT NOT NULL, config_json TEXT NOT NULL);
    """)
    return db


def meta(db, key):
    row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def set_meta(db, key, value):
    db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, json.dumps(value)))


def source_batches(config):
    root = Path(config["source_run"])
    with closing(sqlite3.connect((root / "checkpoint.sqlite3").as_uri() + "?mode=ro", uri=True)) as source:
        state = json.loads(source.execute("SELECT payload FROM state").fetchone()[0])
        batches = [json.loads(row[0]) for row in source.execute("SELECT payload FROM batches ORDER BY batch_id")]
    if state["status"] != "complete" or state["max_records"] is not None:
        raise ValueError("Task 2 requires a completed full-dataset Task 1 run")
    if sum(batch["records"] for batch in batches) != state["records_read"]:
        raise ValueError("Task 1 checkpoint count mismatch")
    fingerprint = digest_json({"source_sha256": state["source_sha256"],
        "run_id": state["run_id"], "parser_version": state["parser_version"],
        "rule_version": state["rule_version"], "batches": batches,
        "base_url": config["base_url"]})
    return state, batches, fingerprint


def prepare(db, config, stop_after_batches=None):
    """Checkpoint input batches and ASIN aggregates in the SAME transaction."""
    state, batches, fingerprint = source_batches(config)
    existing = meta(db, "source_fingerprint")
    if existing is not None and existing != fingerprint:
        raise ValueError("Input run or URL rules changed; use a new Task 2 work directory")
    with db:
        set_meta(db, "source_fingerprint", fingerprint)
        set_meta(db, "source_state", state)
    processed = 0
    for batch in batches:
        payload_hash = digest_json(batch)
        old = db.execute("SELECT payload_hash FROM input_batches WHERE batch_id=?", (batch["batch_id"],)).fetchone()
        counts, first = Counter(), {}
        for file in batch["files"]:
            path = (Path(config["source_run"]) / file["path"]).resolve()
            if not path.is_relative_to(Path(config["source_run"])) or hash_file(path) != file["sha256"]:
                raise ValueError(f"Task 1 output missing or changed: {path}")
            parquet = pq.ParquetFile(path)
            if parquet.metadata.num_rows != file["rows"]:
                raise ValueError("Task 1 file row count mismatch")
            if not old:
                for part in parquet.iter_batches(batch_size=25000, columns=["product_id", "source_review_id"]):
                    for asin, review_id in zip(part.column(0).to_pylist(), part.column(1).to_pylist()):
                        if asin is not None and asin != "":
                            counts[asin] += 1
                            first.setdefault(asin, review_id)
        if old:
            if old[0] != payload_hash:
                raise ValueError("Committed input batch changed")
            continue
        values = [(asin, f"{config['base_url']}/dp/{quote(asin, safe='')}", count,
            first[asin], state["source_sha256"], state["run_id"]) for asin, count in counts.items()]
        with db:
            db.executemany("""INSERT INTO products(asin,original_url,review_count,first_review_id,source_sha256,source_run_id)
                VALUES (?,?,?,?,?,?) ON CONFLICT(asin) DO UPDATE SET review_count=review_count+excluded.review_count""", values)
            db.execute("INSERT INTO input_batches VALUES (?,?)", (batch["batch_id"], payload_hash))
        processed += 1
        if processed % 25 == 0 or batch == batches[-1]:
            logging.info("ASIN manifest input batches committed: %s/%s", batch["batch_id"], len(batches))
        if stop_after_batches and processed >= stop_after_batches:
            raise KeyboardInterrupt("Intentional manifest preparation interruption")
    with db:
        set_meta(db, "prepared", True)
    return db.execute("SELECT COUNT(*) FROM products").fetchone()[0]


@dataclass
class Response:
    url: str
    status: int | None = None
    body: bytes = b""
    headers: dict = field(default_factory=dict)
    error: str | None = None
    truncated: bool = False
    error_detail: str | None = None


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class HTTPClient:
    """One worker, TLS verification on, no cookies/authentication or automatic redirects."""
    def __init__(self, config, sleep=time.sleep, monotonic=time.monotonic):
        self.config, self.sleep, self.monotonic = config, sleep, monotonic
        self.interval = config["request_interval_seconds"]
        self.last_request = None
        self.request_count = 0
        # Native trust can complete legitimate chains; certificate verification stays ON.
        self.ssl_context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.opener = request.build_opener(NoRedirect(), request.HTTPSHandler(context=self.ssl_context))

    def get(self, url):
        origin(url)
        if self.last_request is not None:
            self.sleep(max(0, self.interval - (self.monotonic() - self.last_request)))
        self.last_request = self.monotonic()
        self.request_count += 1
        req = request.Request(url, headers={"User-Agent": self.config["user_agent"],
            "Accept": "text/html,text/plain;q=0.9", "Accept-Encoding": "identity"})
        try:
            try:
                stream = self.opener.open(req, timeout=self.config["timeout_seconds"])
            except error.HTTPError as exc:
                stream = exc
            with stream:
                body = stream.read(self.config["max_response_bytes"] + 1)
                headers = {key.lower(): value for key, value in stream.headers.items()
                    if key.lower() in {"content-type", "content-language", "location", "retry-after", "date", "etag", "last-modified"}}
                return Response(url, stream.code, body[:self.config["max_response_bytes"]], headers,
                    truncated=len(body) > self.config["max_response_bytes"])
        except (TimeoutError, socket.timeout):
            return Response(url, error="timeout")
        except error.URLError as exc:
            return Response(url, error="timeout" if isinstance(exc.reason, (TimeoutError, socket.timeout)) else "network_error",
                error_detail=f"{type(exc.reason).__name__}: {str(exc.reason)[:500]}")
        except (OSError, ValueError) as exc:
            return Response(url, error="network_error", error_detail=f"{type(exc).__name__}: {str(exc)[:500]}")


def save_body(raw_dir, body, category="responses", suffix=".html"):
    digest = hashlib.sha256(body).hexdigest()
    path = Path(raw_dir) / category / (digest + suffix)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if hash_file(path) != digest:
            raise ValueError(f"Content-addressed evidence was modified: {path}")
    else:
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_bytes(body)
        os.replace(temporary, path)
    return str(path.resolve()), digest


class Policy:
    def __init__(self, config, client):
        self.config, self.client = config, client
        self.parser = None
        self.evidence = None

    @staticmethod
    def parse_rules(result):
        if result.error or result.truncated:
            return None
        if result.status in {404, 410}:
            # RFC 9309: unavailable file is distinct from unreachable server (5xx).
            # No published robots rules does NOT grant permission under site terms.
            return Protego.parse("")
        if result.status == 200:
            text = result.body.decode("utf-8-sig", errors="replace")
            if "<html" not in text.lower() and "user-agent:" in text.lower():
                return Protego.parse(text)
        return None

    def preflight(self, force_refresh=False):
        url = self.config["base_url"] + "/robots.txt"
        path = Path(self.config["raw_dir"]) / "policy" / "robots_latest.json"
        cached = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
        result = None
        cache_lifetime = self.config["robots_cache_seconds"] if cached and cached.get("rules_usable") else self.config.get("robots_failure_cache_seconds", 30)
        if not force_refresh and cached and cached["url"] == url and cached["user_agent"] == self.config["user_agent"] and \
                0 <= (datetime.now(timezone.utc) - timestamp(cached["observed_at"])).total_seconds() < cache_lifetime:
            body_path = Path(cached["snapshot_path"]) if cached.get("snapshot_path") else None
            if body_path and hash_file(body_path) == cached["content_hash"]:
                result = Response(url, cached["http_status"], body_path.read_bytes(),
                    error=cached.get("error"), truncated=cached.get("truncated", False))
                self.evidence = cached
        if self.evidence is None:
            attempts = []
            for number in range(self.config.get("robots_max_attempts", 3)):
                result = self.client.get(url)
                snapshot, digest = save_body(self.config["raw_dir"], result.body, "policy", ".txt")
                evidence = {"url": url, "observed_at": utc_now(), "http_status": result.status,
                    "error": result.error, "error_detail": result.error_detail,
                    "truncated": result.truncated, "response_headers": result.headers,
                    "snapshot_path": snapshot, "content_hash": digest,
                    "user_agent": self.config["user_agent"], "robots_agent": self.config["robots_agent"],
                    "collector_version": VERSION, "request_attempt": number + 1,
                    "rules_usable": self.parse_rules(result) is not None}
                immutable = path.parent / (uuid.uuid4().hex + ".json")
                evidence["evidence_path"] = str(immutable.resolve())
                save_json(immutable, evidence)
                attempts.append(str(immutable.resolve()))
                self.evidence = dict(evidence, attempt_evidence_paths=list(attempts))
                temporary_error = result.error in {"timeout", "network_error"} or result.status in {500, 502, 503, 504}
                if not temporary_error or number + 1 >= self.config.get("robots_max_attempts", 3):
                    break
                wait = retry_wait(result, number + 1, self.config)
                if wait is None:
                    break
                logging.info("robots preflight transient failure status=%s; retry %s after %.1fs", result.status, number + 2, wait)
                self.client.sleep(wait)
            save_json(path, self.evidence)
        self.parser = self.parse_rules(result)
        if self.parser is not None:
            delay = self.parser.crawl_delay(self.config["robots_agent"]) or 0
            rate = self.parser.request_rate(self.config["robots_agent"])
            interval = rate.seconds / rate.requests if rate and rate.requests else 0
            self.client.interval = max(self.client.interval, delay, interval)
        return self.evidence

    def decision(self, url):
        if origin(url) != self.config["base_url"]:
            return False, "redirect_outside_approved_origin"
        if self.parser is None:
            return False, "robots_unavailable_or_unparseable_fail_closed"
        if not self.parser.can_fetch(url, self.config["robots_agent"]):
            return False, "robots_disallow"
        evidence_path = self.config.get("permission_evidence")
        if not evidence_path:
            return False, "source_terms_not_reviewed"
        permission = json.loads((ROOT / evidence_path).read_text(encoding="utf-8-sig"))
        if permission.get("origin") != self.config["base_url"] or permission.get("allowed") is not True or \
                not all(permission.get(key) for key in ("reviewed_by", "reviewed_at", "basis")):
            return False, "live_permission_evidence_invalid"
        timestamp(permission["reviewed_at"])
        return True, "robots_and_permission_approved"


class ProductHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.asins, self.canonicals, self.structure = set(), [], set()
        self.product_marker = False
        self.script = None
        self.scripts = []

    def handle_starttag(self, tag, attributes):
        attrs = dict(attributes)
        element_id = attrs.get("id", "")
        self.structure.add((tag, element_id))
        if element_id in {"productTitle", "title_feature_div"}:
            self.product_marker = True
        if tag == "input" and (element_id.upper() == "ASIN" or attrs.get("name", "").upper() == "ASIN"):
            self.asins.add(attrs.get("value", "").strip().upper())
        if tag == "meta" and attrs.get("name", "").lower() == "asin":
            self.asins.add(attrs.get("content", "").strip().upper())
        if tag == "link" and "canonical" in attrs.get("rel", "").lower().split():
            self.canonicals.append(attrs.get("href", ""))
        if tag == "script" and attrs.get("type", "").lower() == "application/ld+json":
            self.script = []

    def handle_data(self, data):
        if self.script is not None:
            self.script.append(data)

    def handle_endtag(self, tag):
        if tag == "script" and self.script is not None:
            self.scripts.append("".join(self.script))
            self.script = None


def classify(asin, response):
    result = {"status": "parsing_failure", "reason": "unknown_product_template",
        "verified_asin": None, "template_signature": None}
    if response.error:
        return dict(result, status=response.error, reason=response.error)
    if response.status in {401, 403, 429}:
        return dict(result, status="restricted_access", reason=f"http_{response.status}_stop_access")
    if response.status in {404, 410}:
        return dict(result, status="not_found", reason=f"http_{response.status}")
    if response.status != 200:
        return dict(result, status="http_error", reason=f"http_{response.status}")
    if response.truncated:
        return dict(result, reason="response_exceeds_size_limit")
    charset = re.search(r"charset\s*=\s*([\w.-]+)", response.headers.get("content-type", ""), re.I)
    try:
        text = response.body.decode(charset.group(1) if charset else "utf-8", errors="strict")
    except (UnicodeDecodeError, LookupError):
        return dict(result, reason="undecodable_html_preserved_raw")
    low = text.lower()
    if any(token in low for token in ("validatecaptcha", "robot check", "automated access",
        "enter the characters you see", "sorry, we just need to make sure", "access denied")) or \
            "/ap/signin" in urlsplit(response.url).path:
        return dict(result, status="restricted_access", reason="challenge_or_login_page_http_200")
    if any(token in low for token in ("sorry! we couldn't find that page", "page not found", "sorry, we couldn't find that page")):
        return dict(result, status="not_found", reason="error_page_http_200")
    if "html" not in response.headers.get("content-type", "").lower():
        return dict(result, reason="unexpected_content_type")
    parser = ProductHTML()
    parser.feed(text)
    for script in parser.scripts:
        try:
            objects = json.loads(script)
        except (ValueError, TypeError):
            continue
        queue = list(objects) if isinstance(objects, list) else [objects]
        for obj in queue:
            if not isinstance(obj, dict):
                continue
            queue.extend(obj.get("@graph", []) if isinstance(obj.get("@graph"), list) else [])
            types = obj.get("@type", [])
            if types == "Product" or isinstance(types, list) and "Product" in types:
                parser.product_marker = True
                for key in ("asin", "sku", "productID"):
                    if isinstance(obj.get(key), str) and re.fullmatch(r"[A-Z0-9]{10}", obj[key]):
                        parser.asins.add(obj[key])
    signature = digest_json(sorted(parser.structure))
    result["template_signature"] = signature
    url_asins = []
    for url in [response.url, *parser.canonicals]:
        match = re.search(r"/(?:dp|gp/product)/([A-Z0-9]{10})(?:[/?#]|$)", url, re.I)
        if match:
            url_asins.append(match.group(1).upper())
    if any(value != asin for value in url_asins) or parser.asins - {asin, ""}:
        return dict(result, reason="asin_mismatch_or_ambiguous_identity")
    if asin not in parser.asins:
        return dict(result, reason="body_asin_not_verified")
    if not parser.product_marker:
        return result
    return dict(result, status="success", reason="product_body_asin_verified", verified_asin=asin)


def begin_attempt(db, asin, run_id, mode):
    attempt_id = uuid.uuid4().hex
    with db:
        db.execute("INSERT INTO attempts(attempt_id,asin,run_id,mode,started_at) VALUES (?,?,?,?,?)",
            (attempt_id, asin, run_id, mode, utc_now()))
        db.execute("UPDATE products SET attempts=attempts+1,last_run_id=? WHERE asin=?", (run_id, asin))
    return attempt_id


def finish_attempt(db, config, asin, run_id, attempt_id, response, mode, chain, extra=None):
    result = classify(asin, response)
    snapshot, digest = (None, None)
    if response.status is not None:
        snapshot, digest = save_body(config["raw_dir"], response.body)
    old = db.execute("SELECT template_signature FROM products WHERE asin=?", (asin,)).fetchone()[0]
    evidence = {"attempt_id": attempt_id, "asin": asin, "run_id": run_id,
        "observed_at": utc_now(), "mode": mode, "final_url": response.url,
        "http_status": response.status, "response_headers": response.headers,
        "snapshot_path": snapshot, "content_hash": digest, "truncated": response.truncated,
        "redirect_chain": chain, "identity_rule": IDENTITY_RULE, **result,
        "previous_template_signature": old,
        "template_changed": old is not None and old != result["template_signature"],
        "additional_evidence": extra}
    path = Path(config["raw_dir"]) / "attempts" / (attempt_id + ".json")
    save_json(path, evidence)
    acquired_at = extra["acquired_at"] if mode == "authorized_snapshot" else evidence["observed_at"] if response.status is not None else None
    with db:
        db.execute("""UPDATE products SET final_url=?,status=?,http_status=?,acquired_at=?,
            snapshot_path=?,content_hash=?,acquisition_mode=?,reason=?,evidence_path=?,
            verified_asin=?,template_signature=?,redirect_chain=?,last_run_id=? WHERE asin=?""",
            (response.url, result["status"], response.status, acquired_at, snapshot, digest, mode,
             result["reason"], str(path.resolve()), result["verified_asin"], result["template_signature"],
             json.dumps(chain), run_id, asin))
        db.execute("UPDATE attempts SET finished_at=?,status=?,evidence_path=?,payload=? WHERE attempt_id=?",
            (utc_now(), result["status"], str(path.resolve()), json.dumps(evidence), attempt_id))
    logging.info("ASIN=%s mode=%s status=%s reason=%s", asin, mode, result["status"], result["reason"])
    return result


def fetch_product(url, config, client, policy):
    chain = []
    for index in range(config["max_redirects"] + 1):
        allowed, reason = policy.decision(url)
        if not allowed:
            return Response(url, error="restricted_access"), chain, reason
        response = client.get(url)
        if response.status not in {301, 302, 303, 307, 308}:
            return response, chain, None
        location = response.headers.get("location")
        raw_path, digest = save_body(config["raw_dir"], response.body)
        chain.append({"url": url, "status": response.status, "location": location,
            "observed_at": utc_now(), "snapshot_path": raw_path, "content_hash": digest})
        if not location:
            return Response(url, error="parsing_failure"), chain, "redirect_without_location"
        destination = urljoin(url, location)
        try:
            if origin(destination) != config["base_url"]:
                return Response(url, error="restricted_access"), chain, "redirect_outside_approved_origin"
        except ValueError:
            return Response(url, error="restricted_access"), chain, "unsafe_redirect"
        if index == config["max_redirects"]:
            return Response(url, error="parsing_failure"), chain, "redirect_limit_exceeded"
        url = destination
    raise AssertionError("unreachable")


def require_prepared(db, config=None):
    if not meta(db, "prepared"):
        raise ValueError("Run prepare before acquisition")
    if config is not None and source_batches(config)[2] != meta(db, "source_fingerprint"):
        raise ValueError("Task 2 source binding changed; use a new work directory")


def recover_attempts(db, config):
    for row in db.execute("SELECT * FROM attempts WHERE finished_at IS NULL").fetchall():
        evidence = {"attempt_id": row["attempt_id"], "asin": row["asin"], "run_id": row["run_id"],
            "mode": row["mode"], "started_at": row["started_at"], "observed_at": utc_now(),
            "status": "interrupted", "reason": "No committed response; server receipt is unknown. This consumed attempt is not refunded."}
        path = Path(config["raw_dir"]) / "attempts" / (row["attempt_id"] + ".interrupted.json")
        save_json(path, evidence)
        with db:
            db.execute("UPDATE attempts SET finished_at=?,status='interrupted',evidence_path=?,payload=? WHERE attempt_id=?",
                (utc_now(), str(path.resolve()), json.dumps(evidence), row["attempt_id"]))


def validate_cached_successes(db):
    for row in db.execute("SELECT snapshot_path,content_hash FROM products WHERE status='success'"):
        if not row[0] or hash_file(row[0]) != row[1]:
            raise ValueError("Cached successful response missing or changed; preserve evidence and repair explicitly")


def retry_wait(response, attempt, config):
    wait = min(2 ** attempt, config["max_retry_wait_seconds"])
    value = response.headers.get("retry-after")
    if value:
        try:
            requested = float(value)
        except ValueError:
            try:
                requested = (parsedate_to_datetime(value) - datetime.now(timezone.utc)).total_seconds()
            except (ValueError, TypeError, OverflowError):
                return None
        if not math.isfinite(requested) or requested > config["max_retry_wait_seconds"]:
            return None
        wait = max(wait, requested)
    return max(wait, config["request_interval_seconds"])


def apply_policy_blocks(db, blocked):
    """Unavailable rules/absent permission mean pending, not proven product access denial."""
    values = []
    for reason, evidence, run_id, asin in blocked:
        status = "restricted_access" if reason == "robots_disallow" else "not_collected"
        values.append((status, reason, evidence, run_id, asin, status, reason, evidence))
    with db:
        db.executemany("""UPDATE products SET status=?,reason=?,evidence_path=?,last_run_id=?
            WHERE asin=? AND (status IS NOT ? OR reason IS NOT ? OR evidence_path IS NOT ?)""", values)


def crawl(db, config, run_id, max_products=None, retry_failed=False, client=None, stop_after_products=None, force_policy_refresh=False):
    require_prepared(db, config)
    recover_attempts(db, config)
    validate_cached_successes(db)
    client = client or HTTPClient(config)
    if meta(db, "access_stop"):
        report = {"run_id": run_id, "observed_at": utc_now(), "network_requests": 0,
            "stopped_on_access_restriction": True, "access_stop": meta(db, "access_stop"),
            "note": "Persistent origin stop: retry-failed does not authorize bypassing a prior ban or challenge."}
        save_json(Path(config["report_dir"]) / "acquisition_policy.json", report)
        return report
    policy = Policy(config, client)
    evidence = policy.preflight(force_refresh=force_policy_refresh)
    statuses = ["not_collected"] + (["restricted_access", "timeout", "network_error", "http_error", "parsing_failure"] if retry_failed else [])
    placeholders = ",".join("?" for _ in statuses)
    eligible = db.execute(f"SELECT asin,original_url FROM products WHERE status IN ({placeholders}) ORDER BY asin", statuses)
    processed = 0
    # Evaluate each URL: do not assume a denial for one ASIN applies to another path.
    blocked = []
    for row in eligible:
        allowed, reason = policy.decision(row["original_url"])
        if not allowed:
            blocked.append((reason, evidence["evidence_path"], run_id, row["asin"]))
            if len(blocked) >= 1000:
                apply_policy_blocks(db, blocked)
                blocked.clear()
            continue
        if max_products is not None and processed >= max_products:
            continue
        if not re.fullmatch(r"[A-Z0-9]{10}", row["asin"]):
            with db:
                db.execute("UPDATE products SET status='parsing_failure',reason='invalid_asin',last_run_id=? WHERE asin=?", (run_id, row["asin"]))
            continue
        used = db.execute("SELECT COUNT(*) FROM attempts WHERE asin=? AND mode='live'", (row["asin"],)).fetchone()[0]
        for number in range(used, config["max_attempts"]):
            attempt_id = begin_attempt(db, row["asin"], run_id, "live")
            response, chain, restriction = fetch_product(row["original_url"], config, client, policy)
            result = finish_attempt(db, config, row["asin"], run_id, attempt_id, response, "live", chain,
                {"policy_evidence": evidence["evidence_path"], "restriction": restriction,
                 "permission_evidence": config.get("permission_evidence")})
            if result["status"] == "restricted_access":
                # Stop affected-origin access after a ban/challenge. Untouched ASINs remain not_collected.
                with db:
                    set_meta(db, "access_stop", {"asin": row["asin"], "run_id": run_id,
                        "reason": result["reason"], "observed_at": utc_now()})
                if blocked:
                    apply_policy_blocks(db, blocked)
                report = {"run_id": run_id, "observed_at": utc_now(), "policy": evidence,
                    "processed_products": processed + 1, "network_requests": client.request_count,
                    "stopped_on_access_restriction": True, "access_stop": meta(db, "access_stop")}
                save_json(Path(config["report_dir"]) / "acquisition_policy.json", report)
                return report
            if result["status"] not in RETRYABLE or response.status is not None and response.status < 500:
                break
            wait = retry_wait(response, number + 1, config)
            if wait is None or number + 1 >= config["max_attempts"]:
                break
            client.sleep(wait)
        processed += 1
        if stop_after_products and processed >= stop_after_products:
            raise KeyboardInterrupt("Intentional acquisition interruption")
    if blocked:
        apply_policy_blocks(db, blocked)
    report = {"run_id": run_id, "observed_at": utc_now(), "policy": evidence,
        "permission_evidence_supplied": bool(config.get("permission_evidence")), "processed_products": processed,
        "network_requests": client.request_count, "stopped_on_access_restriction": False,
        "note": "Policy rejection is not a product HTTP attempt. Restricted rows may have attempts=0 and NULL HTTP/acquisition fields."}
    save_json(Path(config["report_dir"]) / "acquisition_policy.json", report)
    return report


def import_snapshots(db, config, manifest_path, run_id):
    """Explicit provenance is mandatory; synthetic test pages are never imported into real runs."""
    require_prepared(db, config)
    recover_attempts(db, config)
    manifest_path = Path(manifest_path).resolve()
    imported, cached = 0, 0
    with manifest_path.open(encoding="utf-8-sig") as stream:
        for line in stream:
            if not line.strip():
                continue
            item = json.loads(line)
            required = ("asin", "original_url", "final_url", "http_status", "acquired_at",
                "snapshot_path", "sha256", "authorization_note", "provider", "content_type")
            if any(item.get(key) is None or item.get(key) == "" for key in required):
                raise ValueError("Incomplete authorized snapshot provenance")
            timestamp(item["acquired_at"])
            if isinstance(item["http_status"], bool) or not isinstance(item["http_status"], int) or not 100 <= item["http_status"] <= 599:
                raise ValueError("Snapshot HTTP status must be an actual integer HTTP code")
            row = db.execute("SELECT * FROM products WHERE asin=?", (item["asin"],)).fetchone()
            if row is None or row["original_url"] != item["original_url"]:
                raise ValueError("Snapshot ASIN/URL is not in the acquisition manifest")
            if origin(item["final_url"]) != config["base_url"]:
                raise ValueError("Snapshot final URL must belong to the product source")
            source = (manifest_path.parent / item["snapshot_path"]).resolve()
            if not source.is_relative_to(manifest_path.parent):
                raise ValueError("Snapshot path escapes its supplied package")
            if hash_file(source) != item["sha256"] or source.stat().st_size > config["max_response_bytes"]:
                raise ValueError("Snapshot checksum mismatch or excessive size")
            if row["status"] == "success":
                if row["content_hash"] != item["sha256"]:
                    raise ValueError("Different cached snapshot; use a new versioned run instead of overwriting")
                if hash_file(row["snapshot_path"]) != row["content_hash"]:
                    raise ValueError("Cached snapshot changed")
                cached += 1
                continue
            # Idempotency also applies to unsuccessful offline evidence.
            if db.execute("SELECT 1 FROM attempts WHERE asin=? AND mode='authorized_snapshot' AND status IS NOT NULL AND json_extract(payload,'$.additional_evidence.sha256')=?",
                    (item["asin"], item["sha256"])).fetchone():
                cached += 1
                continue
            attempt_id = begin_attempt(db, item["asin"], run_id, "authorized_snapshot")
            finish_attempt(db, config, item["asin"], run_id, attempt_id,
                Response(item["final_url"], item["http_status"], source.read_bytes(), {"content-type": item["content_type"]}),
                "authorized_snapshot", item.get("redirect_chain", []), item)
            imported += 1
    return {"imported": imported, "cached": cached}


def export(db, config):
    require_prepared(db, config)
    path = Path(config["work_dir"]) / "product_manifest.parquet"
    temporary = path.with_suffix(".parquet.tmp")
    fingerprint = hashlib.sha256()
    cursor = db.execute("SELECT " + ",".join(SCHEMA.names) + " FROM products ORDER BY asin")
    with pq.ParquetWriter(temporary, SCHEMA, compression="zstd") as writer:
        while rows := cursor.fetchmany(5000):
            values = [dict(row) for row in rows]
            for value in values:
                fingerprint.update((json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":")) + "\n").encode())
            writer.write_table(pa.Table.from_pylist(values, schema=SCHEMA))
    os.replace(temporary, path)
    counts = dict.fromkeys(STATUSES, 0)
    counts.update(dict(db.execute("SELECT status,COUNT(*) FROM products GROUP BY status")))
    total = sum(counts.values())
    source = meta(db, "source_state")
    modes = {row[0]: row[1] for row in db.execute("SELECT acquisition_mode,COUNT(*) FROM products WHERE status='success' GROUP BY acquisition_mode")}
    stats = {"observed_at": utc_now(), "collector_version": VERSION, "identity_rule": IDENTITY_RULE,
        "source_sha256": source["source_sha256"], "source_run_id": source["run_id"],
        "source_fingerprint": meta(db, "source_fingerprint"), "source_records": source["records_read"],
        "reviews_with_product_id": db.execute("SELECT SUM(review_count) FROM products").fetchone()[0],
        "distinct_asins": total, "official_reference_asins": 253059,
        "difference_from_official": total - 253059, "statuses": counts,
        "reason_counts": {reason: count for reason, count in db.execute("SELECT COALESCE(reason,'not_yet_processed'),COUNT(*) FROM products GROUP BY reason")},
        "status_counts_reconcile": total == db.execute("SELECT COUNT(*) FROM products").fetchone()[0],
        "attempts_total": db.execute("SELECT COALESCE(SUM(attempts),0) FROM products").fetchone()[0],
        "live_product_attempts": db.execute("SELECT COUNT(*) FROM attempts WHERE mode='live'").fetchone()[0],
        "successful_live": modes.get("live", 0), "successful_authorized_snapshots": modes.get("authorized_snapshot", 0),
        "success_rate_all_products": counts["success"] / total if total else None,
        "live_success_rate_all_products": modes.get("live", 0) / total if total else None,
        "offline_success_rate_all_products": modes.get("authorized_snapshot", 0) / total if total else None,
        "policy_denied_without_product_request": db.execute("SELECT COUNT(*) FROM products WHERE status='restricted_access' AND attempts=0").fetchone()[0],
        "policy_pending_without_product_request": db.execute("SELECT COUNT(*) FROM products WHERE status='not_collected' AND attempts=0 AND evidence_path IS NOT NULL").fetchone()[0],
        "pending_attempts": db.execute("SELECT COUNT(*) FROM attempts WHERE finished_at IS NULL").fetchone()[0],
        "product_manifest": {"path": str(path.resolve()), "file_size_bytes": path.stat().st_size,
            "sha256": hash_file(path), "rows": total, "ordered_rows_sha256": fingerprint.hexdigest()},
        "raw_evidence_location": config["raw_dir"], "checkpoint_location": str((Path(config["work_dir"]) / "acquisition.sqlite3").resolve()),
        "access_stop": meta(db, "access_stop"),
        "scope_note": "A complete manifest is not complete page acquisition. Policy evidence is shared; no HTTP status or acquisition time is invented for unrequested product pages. Test fixtures are excluded.",
        "recovery_instructions": "Use the same config and source versions. prepare resumes committed input batches; crawl skips completed rows; --retry-failed is explicit and cannot override robots or permission. Rebuild a lost checkpoint in a new work_dir; import permitted snapshots with their provenance manifest."}
    metadata_report = Path(config["report_dir"]) / "metadata_statistics.json"
    if metadata_report.exists():
        supplemental = json.loads(metadata_report.read_text(encoding="utf-8"))
        stats["third_party_metadata"] = {key: supplemental[key] for key in
            ("matched_asins", "matched_records", "coverage_all_target_products", "output", "counts_as_successful_product_pages",
             "identity_conflict_records", "identity_conflict_asins", "asins_without_identity_conflicts")}
    save_json(Path(config["report_dir"]) / "full_statistics.json", stats)
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "crawl", "import-snapshots", "export", "run"])
    parser.add_argument("--config", type=Path, default=ROOT / "config/task2.example.json")
    parser.add_argument("--snapshot-manifest", type=Path)
    parser.add_argument("--metadata-config", type=Path, help="Optional explicitly offered public research archive; counted separately from pages")
    parser.add_argument("--max-products", type=int)
    parser.add_argument("--retry-failed", action="store_true")
    parser.add_argument("--refresh-policy", action="store_true", help="Fetch current robots with the same declared identity; never overrides a denial")
    parser.add_argument("--stop-after-batches", type=int)
    parser.add_argument("--stop-after-products", type=int)
    args = parser.parse_args()
    for value in (args.max_products, args.stop_after_batches, args.stop_after_products):
        if value is not None and value <= 0:
            parser.error("Limits must be positive")
    config = load_config(args.config)
    reports = Path(config["report_dir"])
    reports.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(reports / "execution.log", encoding="utf-8")], force=True)
    run_id = uuid.uuid4().hex
    started = time.monotonic()
    with run_lock(config["work_dir"]):
        db = open_db(config["work_dir"])
        with db:
            db.execute("INSERT INTO runs VALUES (?,?,NULL,?,'running',?)", (run_id, utc_now(), args.action,
                json.dumps(dict(config, collector_version=VERSION, identity_rule=IDENTITY_RULE))))
        status = "complete"
        try:
            if args.action in {"prepare", "run"}:
                logging.info("Stage prepare: validate full Task 1 inputs and build complete ASIN manifest")
                prepare(db, config, args.stop_after_batches)
            if args.action in {"crawl", "run"}:
                logging.info("Stage crawl: policy preflight, bounded acquisition and persistent cache")
                crawl(db, config, run_id, args.max_products, args.retry_failed,
                    stop_after_products=args.stop_after_products, force_policy_refresh=args.refresh_policy)
            if args.action == "import-snapshots":
                if not args.snapshot_manifest:
                    raise ValueError("--snapshot-manifest is required")
                import_snapshots(db, config, args.snapshot_manifest, run_id)
            if args.metadata_config:
                logging.info("Stage public metadata acquisition: preserve research archive and exact-ASIN evidence separately")
                from acquire_product_metadata import acquire
                acquire(config, args.metadata_config)
        except KeyboardInterrupt:
            status = "interrupted"
            logging.warning("Interrupted; committed batches and per-ASIN attempts are preserved")
        except Exception:
            status = "failed"
            logging.exception("Task 2 failed; completed evidence is preserved, export is attempted if prepared")
            raise
        finally:
            try:
                if meta(db, "prepared"):
                    logging.info("Stage export: materialize complete manifest and full-denominator statistics")
                    stats = export(db, config)
                    logging.info("Manifest total=%s statuses=%s success_rate=%s", stats["distinct_asins"], stats["statuses"], stats["success_rate_all_products"])
                    if status == "complete":
                        logging.info("Stage verify: exact ASIN membership, counts and retained evidence")
                        sys.path.insert(0, str(ROOT / "src/quality"))
                        from verify_task2 import verify
                        save_json(reports / "full_verification.json", verify(config))
            except Exception:
                status = "failed"
                logging.exception("Export/verification failed; run is not marked complete")
                raise
            finally:
                with db:
                    db.execute("UPDATE runs SET finished_at=?,status=? WHERE run_id=?", (utc_now(), status, run_id))
                save_json(reports / "last_execution.json", {"run_id": run_id, "phase": args.action,
                    "status": status, "finished_at": utc_now(), "elapsed_seconds": time.monotonic() - started,
                    "collector_version": VERSION, "config_sha256": hash_file(args.config)})
                db.close()
    return 130 if status == "interrupted" else 0


if __name__ == "__main__":
    sys.exit(main())
