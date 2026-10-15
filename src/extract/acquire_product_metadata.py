"""Acquire explicitly offered public research metadata; NEVER count it as a page.

The publisher's linked archive, robots evidence and citation are retained. Source
records are matched by ASIN only, without movie classification or entity merging.
"""
from __future__ import annotations

import argparse
from contextlib import closing
import gzip
import hashlib
from html.parser import HTMLParser
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import sys
import time
from urllib import error, request
from urllib.parse import urljoin, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq

from acquire_products import HTTPClient, Policy, Response, ROOT, load_config, meta, origin, require_prepared, retry_wait, run_lock, save_body, save_json
from register_source import hash_file, utc_now

SCHEMA = pa.schema([
    ("asin", pa.string()), ("source_id", pa.string()), ("archive_sha256", pa.string()),
    ("record_sequence", pa.int64()), ("byte_start", pa.int64()), ("byte_end", pa.int64()),
    ("raw_record_sha256", pa.string()), ("raw_record", pa.binary()),
    ("source_downloaded_at", pa.string()),
])


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.hrefs.extend(value for key, value in attrs if key == "href" and value)


def read_source_spec(path):
    spec = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not re.fullmatch(r"[a-z0-9_]+", spec["source_id"]) or spec["format"] != "jsonl_gzip":
        raise ValueError("Unsupported source identifier or format")
    for key in ("publisher_page", "download_url"):
        origin(spec[key])
    if Path(spec["archive_name"]).name != spec["archive_name"] or not spec["archive_name"].endswith(".gz"):
        raise ValueError("Archive must be a local gzip filename")
    for key in ("max_archive_bytes", "max_decompressed_bytes", "max_record_bytes"):
        if isinstance(spec[key], bool) or not isinstance(spec[key], int) or spec[key] <= 0:
            raise ValueError("Source size limits must be positive integers")
    if not spec.get("citation"):
        raise ValueError("Publisher citation must be retained")
    return spec


def source_policy(config, client, url, root):
    settings = dict(config, base_url=origin(url), raw_dir=str(root))
    policy = Policy(settings, client)
    evidence = policy.preflight()
    if policy.parser is None or not policy.parser.can_fetch(url, config["robots_agent"]):
        raise ValueError(f"Public research source robots rules do not allow this request: {url}")
    return evidence


def download_source(config, spec, client=None):
    """Download only a resource explicitly linked by the archived publisher page."""
    client = client or HTTPClient(config)
    root = Path(config["raw_dir"]) / "third_party" / spec["source_id"]
    root.mkdir(parents=True, exist_ok=True)
    registration = root / "source_manifest.json"
    archive = root / spec["archive_name"]
    if registration.exists():
        result = json.loads(registration.read_text(encoding="utf-8"))
        if result["download_url"] != spec["download_url"] or result["publisher_page"] != spec["publisher_page"]:
            raise ValueError("Source URLs changed; use a new source_id")
        if not archive.exists() or hash_file(archive) != result["archive_sha256"]:
            raise ValueError("Cached research archive missing or changed")
        return result
    if archive.exists():
        raise ValueError("Unregistered archive preserved; do not silently overwrite it")
    if spec.get("publisher_reference"):
        # Already-observed publisher material can be supplied as an auditable reference.
        # This avoids recrawling the publisher; it NEVER bypasses archive-origin rules.
        reference = json.loads((ROOT / spec["publisher_reference"]).read_text(encoding="utf-8"))
        if reference.get("publisher_page") != spec["publisher_page"] or reference.get("download_url") != spec["download_url"] or reference.get("source_type") != "third_party_product_metadata":
            raise ValueError("Publisher reference does not support this archive")
        publisher_path = root / "publisher/source_reference.json"
        save_json(publisher_path, reference)
        publisher_snapshot, publisher_hash = str(publisher_path.resolve()), hash_file(publisher_path)
        publisher_policy = {"observation_method": reference["observation_method"],
            "scope": "Publisher reference already observed; no native publisher HTML request is made."}
        publisher_format = "research_reference_json_not_http_snapshot"
    else:
        publisher_policy = source_policy(config, client, spec["publisher_page"], root / "publisher_policy")
        publisher = client.get(spec["publisher_page"])
        publisher_snapshot, publisher_hash = save_body(root, publisher.body, "publisher", ".html")
        if publisher.status != 200 or publisher.error or publisher.truncated:
            raise ValueError("Publisher page unavailable; cannot establish public download basis")
        links = Links()
        links.feed(publisher.body.decode("utf-8", errors="strict"))
        linked = {urljoin(spec["publisher_page"], href) for href in links.hrefs}
        if spec["download_url"] not in linked:
            raise ValueError("Archive is not explicitly linked by the publisher page")
        publisher_format = "raw_html"
    archive_policy = source_policy(config, client, spec["download_url"], root / "archive_policy")
    partial = archive.with_suffix(archive.suffix + ".part")
    transfer_path = root / "download_transfer.json"
    transfer = json.loads(transfer_path.read_text(encoding="utf-8")) if transfer_path.exists() else {}
    for number in range(config["max_attempts"]):
        offset = partial.stat().st_size if partial.exists() else 0
        if offset and (not transfer.get("etag") or transfer["etag"].startswith("W/")):
            # Without a strong validator, restart our partial file rather than splice versions.
            offset = 0
        headers = {"User-Agent": config["user_agent"], "Accept-Encoding": "identity"}
        if offset:
            headers["Range"] = f"bytes={offset}-"
            if transfer.get("etag"):
                headers["If-Range"] = transfer["etag"]
        if client.last_request is not None:
            client.sleep(max(0, client.interval - (client.monotonic() - client.last_request)))
        client.last_request = client.monotonic()
        client.request_count += 1
        try:
            response = client.opener.open(request.Request(spec["download_url"], headers=headers), timeout=config["timeout_seconds"])
            with response:
                if response.code not in {200, 206}:
                    raise ValueError(f"Unexpected archive status {response.code}")
                if response.code == 206:
                    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("Content-Range", ""))
                    if not match or int(match[1]) != offset or offset == 0:
                        raise ValueError("Invalid archive resume Content-Range")
                    if transfer.get("etag") and response.headers.get("ETag") != transfer["etag"]:
                        raise ValueError("Archive version changed during Range resume")
                    expected_total = int(match[3])
                    mode = "ab"
                else:
                    offset, mode = 0, "wb"
                    length = response.headers.get("Content-Length")
                    expected_total = int(length) if length else None
                if expected_total is not None and expected_total > spec["max_archive_bytes"]:
                    raise ValueError("Archive exceeds configured size limit")
                transfer = {"url": spec["download_url"], "observed_at": utc_now(),
                    "http_status": response.code, "etag": response.headers.get("ETag"),
                    "content_type": response.headers.get("Content-Type"),
                    "expected_total": expected_total, "resumed_from": offset}
                save_json(transfer_path, transfer)
                total, last_log = offset, offset
                with partial.open(mode) as output:
                    while chunk := response.read(1024 * 1024):
                        total += len(chunk)
                        if total > spec["max_archive_bytes"]:
                            raise ValueError("Archive exceeds configured size limit")
                        output.write(chunk)
                        if total - last_log >= 16 * 1024 * 1024:
                            logging.info("Public metadata archive downloaded %.1f MiB", total / 1024**2)
                            last_log = total
                if expected_total is not None and total != expected_total:
                    raise OSError("Incomplete archive response")
                with partial.open("rb") as incoming:
                    if incoming.read(2) != b"\x1f\x8b":
                        raise ValueError("Archive response is not gzip; do not treat an error page as data")
            os.replace(partial, archive)
            break
        except error.HTTPError as exc:
            with exc:
                body = exc.read(config["max_response_bytes"])
            snapshot, digest = save_body(root, body, "download_failures", ".html")
            save_json(root / f"download_failure_{number + 1}.json", {"observed_at": utc_now(),
                "url": spec["download_url"], "http_status": exc.code, "snapshot_path": snapshot, "content_hash": digest})
            if exc.code in {401, 403, 429} or exc.code < 500 or number + 1 >= config["max_attempts"]:
                raise
            wait = retry_wait(Response(spec["download_url"], exc.code,
                headers={"retry-after": exc.headers.get("Retry-After")} if exc.headers.get("Retry-After") else {}), number + 1, config)
            if wait is None:
                raise
            client.sleep(wait)
        except (OSError, error.URLError):
            if number + 1 >= config["max_attempts"]:
                raise
            client.sleep(min(2 ** (number + 1), config["max_retry_wait_seconds"]))
    result = {"source_id": spec["source_id"], "source_type": "third_party_product_metadata",
        "publisher_page": spec["publisher_page"], "publisher_snapshot": publisher_snapshot,
        "publisher_snapshot_sha256": publisher_hash, "download_url": spec["download_url"],
        "publisher_evidence_format": publisher_format,
        "downloaded_at": utc_now(), "archive_path": str(archive.resolve()),
        "archive_size_bytes": archive.stat().st_size, "archive_sha256": hash_file(archive),
        "publisher_robots_evidence": publisher_policy, "archive_robots_evidence": archive_policy,
        "citation": spec["citation"], "publisher_reported_products": spec.get("publisher_reported_products"),
        "access_basis": "The official research publisher explicitly links this archive for download; retain citation. This does not authorize crawling Amazon.",
        "historical_record_acquisition_time": None,
        "time_note": "downloaded_at is our file download time, not the original historical product page acquisition time.",
        "network_requests": client.request_count}
    save_json(registration, result)
    return result


def open_metadata_db(config):
    db = sqlite3.connect((Path(config["work_dir"]) / "metadata.sqlite3").as_uri(), uri=True)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
      CREATE TABLE IF NOT EXISTS state(source_id TEXT PRIMARY KEY,payload TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS records(
        asin TEXT NOT NULL,source_id TEXT NOT NULL,archive_sha256 TEXT NOT NULL,
        record_sequence INTEGER NOT NULL,byte_start INTEGER NOT NULL,byte_end INTEGER NOT NULL,
        raw_record_sha256 TEXT NOT NULL,raw_record BLOB NOT NULL,source_downloaded_at TEXT NOT NULL,
        PRIMARY KEY(source_id,record_sequence));
      CREATE INDEX IF NOT EXISTS metadata_asin ON records(asin);
      CREATE TABLE IF NOT EXISTS issues(source_id TEXT,record_sequence INTEGER,byte_start INTEGER,
        reason TEXT,raw_record BLOB,PRIMARY KEY(source_id,record_sequence));
      CREATE TABLE IF NOT EXISTS identity_issues(source_id TEXT,record_sequence INTEGER,
        field_name TEXT,raw_value TEXT,issue TEXT NOT NULL,
        PRIMARY KEY(source_id,record_sequence,field_name));
    """)
    target = Path(config["work_dir"]) / "acquisition.sqlite3"
    try:
        db.execute("ATTACH DATABASE ? AS target", (target.as_uri() + "?mode=ro",))
    except Exception:
        db.close()
        raise
    return db


def import_archive(db, registration, spec, target_fingerprint, stop_after_records=None):
    source_id = registration["source_id"]
    row = db.execute("SELECT payload FROM state WHERE source_id=?", (source_id,)).fetchone()
    state = json.loads(row[0]) if row else {"source_id": source_id,
        "archive_sha256": registration["archive_sha256"], "target_fingerprint": target_fingerprint,
        "next_offset": 0, "records_read": 0, "valid_records": 0, "invalid_records": 0,
        "matched_records": 0, "unmatched_records": 0, "complete": False,
        "source_registration": registration}
    if state["archive_sha256"] != registration["archive_sha256"] or state["target_fingerprint"] != target_fingerprint:
        raise ValueError("Metadata source/target version changed; use a new versioned source or run")
    if hash_file(registration["archive_path"]) != registration["archive_sha256"]:
        raise ValueError("Metadata archive changed before import")
    if state["complete"]:
        return state
    pending, issues = [], []
    pending_bytes = processed = 0

    def commit():
        with db:
            db.executemany("INSERT INTO records VALUES (?,?,?,?,?,?,?,?,?)", pending)
            db.executemany("INSERT INTO issues VALUES (?,?,?,?,?)", issues)
            db.execute("INSERT OR REPLACE INTO state VALUES (?,?)", (source_id, json.dumps(state)))
        pending.clear()
        issues.clear()

    with gzip.open(registration["archive_path"], "rb") as stream:
        stream.seek(state["next_offset"])
        while True:
            start = stream.tell()
            raw = stream.readline(spec["max_record_bytes"] + 1)
            if not raw:
                break
            if len(raw) > spec["max_record_bytes"] or stream.tell() > spec["max_decompressed_bytes"]:
                raise ValueError("Metadata decompressed record/file exceeds size limit")
            sequence = state["records_read"] + 1
            reason = None
            try:
                item = json.loads(raw)
                asin = item.get("asin") if isinstance(item, dict) else None
                if not isinstance(asin, str) or not re.fullmatch(r"[A-Z0-9]{10}", asin):
                    reason = "missing_or_invalid_asin"
            except (ValueError, UnicodeError):
                reason = "invalid_json_or_encoding"
            state["records_read"] += 1
            if reason:
                state["invalid_records"] += 1
                issues.append((source_id, sequence, start, reason, raw))
            else:
                state["valid_records"] += 1
                exists = db.execute("SELECT 1 FROM target.products WHERE asin=?", (asin,)).fetchone()
                if exists:
                    state["matched_records"] += 1
                    pending.append((asin, source_id, registration["archive_sha256"], sequence, start,
                        stream.tell(), hashlib.sha256(raw).hexdigest(), raw, registration["downloaded_at"]))
                else:
                    state["unmatched_records"] += 1
            state["next_offset"] = stream.tell()
            pending_bytes += len(raw)
            processed += 1
            if state["records_read"] % 1000 == 0 or pending_bytes >= 8 * 1024**2:
                commit()
                pending_bytes = 0
            if state["records_read"] % 25000 == 0:
                logging.info("Public metadata records read=%s matched=%s", state["records_read"], state["matched_records"])
            if stop_after_records and processed >= stop_after_records:
                commit()
                raise KeyboardInterrupt("Intentional metadata import interruption")
    # Reaching gzip EOF also verifies its CRC; incomplete streams never become complete.
    state["complete"] = True
    commit()
    return state


def audit_identities(db):
    """Preserve conflicting source identifiers; exact top-level matches are not enough."""
    issues = []
    for row in db.execute("SELECT source_id,record_sequence,asin,raw_record FROM records"):
        details = json.loads(row["raw_record"]).get("details")
        if not isinstance(details, dict):
            continue
        for key, value in details.items():
            if key.strip().rstrip(":").strip().lower() == "asin" and (not isinstance(value, str) or value.strip() != row["asin"]):
                issues.append((row["source_id"], row["record_sequence"], f"details.{key}",
                    json.dumps(value, ensure_ascii=False), "asin_identity_conflict"))
    with db:
        db.executemany("INSERT OR IGNORE INTO identity_issues VALUES (?,?,?,?,?)", issues)


def export_metadata(db, config):
    audit_identities(db)
    path = Path(config["work_dir"]) / "third_party_product_records.parquet"
    temporary = path.with_suffix(".parquet.tmp")
    cursor = db.execute("SELECT " + ",".join(SCHEMA.names) + " FROM records ORDER BY source_id,record_sequence")
    with pq.ParquetWriter(temporary, SCHEMA, compression="zstd") as writer:
        while rows := cursor.fetchmany(500):
            writer.write_table(pa.Table.from_pylist([dict(row) for row in rows], schema=SCHEMA))
    os.replace(temporary, path)
    states = [json.loads(row[0]) for row in db.execute("SELECT payload FROM state ORDER BY source_id")]
    total = db.execute("SELECT COUNT(*) FROM target.products").fetchone()[0]
    matched = db.execute("SELECT COUNT(DISTINCT asin) FROM records").fetchone()[0]
    identity_issues = [dict(row) for row in db.execute("""SELECT r.asin,r.source_id,r.record_sequence,
        r.archive_sha256,r.byte_start,r.byte_end,q.field_name,q.raw_value,q.issue
        FROM identity_issues q JOIN records r USING(source_id,record_sequence)
        ORDER BY r.source_id,r.record_sequence,q.field_name""")]
    conflict_records = len({(row["source_id"], row["record_sequence"]) for row in identity_issues})
    conflict_asins = len({row["asin"] for row in identity_issues})
    save_json(Path(config["report_dir"]) / "metadata_quality_issues.json", {
        "rule_id": "task2-metadata-asin-consistency-v1", "identity_conflict_records": conflict_records,
        "identity_conflict_asins": conflict_asins, "issues": identity_issues,
        "handling": "Raw candidates retained; affected ASINs require confirmation and must not be automatically classified as movies."})
    stats = {"observed_at": utc_now(), "source_type": "third_party_product_metadata",
        "total_target_asins": total, "matched_asins": matched, "unmatched_target_asins": total - matched,
        "matched_records": db.execute("SELECT COUNT(*) FROM records").fetchone()[0],
        "identity_conflict_records": conflict_records, "identity_conflict_asins": conflict_asins,
        "asins_without_identity_conflicts": matched - conflict_asins,
        "coverage_all_target_products": matched / total if total else None,
        "source_states": states, "complete_sources": sum(state["complete"] for state in states),
        "raw_issue_records": db.execute("SELECT COUNT(*) FROM issues").fetchone()[0],
        "output": {"path": str(path.resolve()), "file_size_bytes": path.stat().st_size, "sha256": hash_file(path)},
        "checkpoint_path": str((Path(config["work_dir"]) / "metadata.sqlite3").resolve()),
        "counts_as_successful_product_pages": False,
        "note": "Exact ASIN correspondence to public research metadata. These are NOT raw/authorized HTML product snapshots, verified movie works, or current product data."}
    save_json(Path(config["report_dir"]) / "metadata_statistics.json", stats)
    return stats


def verify_metadata(config):
    stats = json.loads((Path(config["report_dir"]) / "metadata_statistics.json").read_text(encoding="utf-8"))
    path = Path(stats["output"]["path"])
    if hash_file(path) != stats["output"]["sha256"]:
        raise ValueError("Metadata Parquet hash mismatch")
    with closing(sqlite3.connect((Path(config["work_dir"]) / "metadata.sqlite3").as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute("ATTACH DATABASE ? AS target", ((Path(config["work_dir"]) / "acquisition.sqlite3").as_uri() + "?mode=ro",))
        invalid_asins = db.execute("SELECT COUNT(*) FROM (SELECT asin FROM records EXCEPT SELECT asin FROM target.products)").fetchone()[0]
        matched_asins = db.execute("SELECT COUNT(DISTINCT asin) FROM records").fetchone()[0]
        if invalid_asins or matched_asins != stats["matched_asins"]:
            raise ValueError("Metadata ASIN membership/count mismatch")
        target_fingerprint = json.loads(db.execute("SELECT value FROM target.meta WHERE key='source_fingerprint'").fetchone()[0])
        cursor = db.execute("SELECT " + ",".join(SCHEMA.names) + " FROM records ORDER BY source_id,record_sequence")
        rows = 0
        expected_issues = set()
        for batch in pq.ParquetFile(path).iter_batches(batch_size=500):
            records = batch.to_pylist()
            if records != [dict(row) for row in cursor.fetchmany(len(records))]:
                raise ValueError("Exported metadata differs from checkpoint")
            for row in records:
                item = json.loads(row["raw_record"])
                if hashlib.sha256(row["raw_record"]).hexdigest() != row["raw_record_sha256"] or item.get("asin") != row["asin"]:
                    raise ValueError("Metadata raw-record identity/hash mismatch")
                details = item.get("details")
                if isinstance(details, dict):
                    for key, value in details.items():
                        if key.strip().rstrip(":").strip().lower() == "asin" and (not isinstance(value, str) or value.strip() != row["asin"]):
                            expected_issues.add((row["source_id"], row["record_sequence"], f"details.{key}", json.dumps(value, ensure_ascii=False), "asin_identity_conflict"))
                rows += 1
        if cursor.fetchone() is not None or rows != stats["matched_records"]:
            raise ValueError("Metadata count mismatch")
        raw_locators = 0
        for state in stats["source_states"]:
            if state["target_fingerprint"] != target_fingerprint:
                raise ValueError("Metadata target input binding changed")
            registration = state["source_registration"]
            if hash_file(registration["archive_path"]) != registration["archive_sha256"]:
                raise ValueError("Metadata source archive changed")
            if hash_file(registration["publisher_snapshot"]) != registration["publisher_snapshot_sha256"]:
                raise ValueError("Publisher download-basis snapshot changed")
            expected = iter(db.execute("SELECT record_sequence,byte_start,byte_end,raw_record FROM records WHERE source_id=? ORDER BY record_sequence", (state["source_id"],)))
            wanted = next(expected, None)
            with gzip.open(registration["archive_path"], "rb") as stream:
                sequence = 0
                while True:
                    start = stream.tell()
                    raw = stream.readline()
                    if not raw:
                        break
                    sequence += 1
                    if wanted is not None and sequence == wanted[0]:
                        if (start, stream.tell(), raw) != (wanted[1], wanted[2], wanted[3]):
                            raise ValueError("Metadata record locator does not match the source archive")
                        raw_locators += 1
                        wanted = next(expected, None)
            if wanted is not None or state["complete"] and sequence != state["records_read"]:
                raise ValueError("Metadata source sequence/count mismatch")
        if raw_locators != rows:
            raise ValueError("Not all metadata raw locators verified")
        actual_issues = {tuple(row) for row in db.execute("SELECT source_id,record_sequence,field_name,raw_value,issue FROM identity_issues")}
        if actual_issues != expected_issues:
            raise ValueError("Metadata identity issues are missing or unsupported by raw evidence")
    return {"verified_at": utc_now(), "status": "passed", "matched_asins": stats["matched_asins"],
        "matched_records": rows, "all_matched_raw_locators_checked": raw_locators,
        "all_exported_fields_match_checkpoint": True, "source_archive_hashes_verified": True,
        "identity_conflict_records": stats["identity_conflict_records"],
        "identity_conflicts_preserved_and_flagged": True,
        "counts_as_successful_product_pages": False}


def acquire(config, source_config):
    spec = read_source_spec(source_config)
    with closing(sqlite3.connect((Path(config["work_dir"]) / "acquisition.sqlite3").as_uri() + "?mode=ro", uri=True)) as target:
        require_prepared(target, config)
        fingerprint = meta(target, "source_fingerprint")
    registration = download_source(config, spec)
    with closing(open_metadata_db(config)) as db:
        import_archive(db, registration, spec, fingerprint)
        stats = export_metadata(db, config)
    verification = verify_metadata(config)
    save_json(Path(config["report_dir"]) / "metadata_verification.json", verification)
    logging.info("Third-party product metadata matched_asins=%s/%s; this is not product-page acquisition", stats["matched_asins"], stats["total_target_asins"])
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config/task2.example.json")
    parser.add_argument("--source-config", type=Path, default=ROOT / "config/task2.metadata.example.json")
    args = parser.parse_args()
    config = load_config(args.config)
    Path(config["report_dir"]).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(Path(config["report_dir"]) / "metadata_execution.log", encoding="utf-8")], force=True)
    with run_lock(config["work_dir"]):
        result = acquire(config, args.source_config)
    print(json.dumps({key: value for key, value in result.items() if key != "source_states"}, indent=2))


if __name__ == "__main__":
    main()
