"""Stream SNAP reviews into restartable, auditable Parquet batches."""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import re
import sqlite3
import sys
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import psutil
import pyarrow as pa
import pyarrow.parquet as pq

from register_source import hash_file, utc_now

PARSER_VERSION = "1.2.0"
RULE_VERSION = "task1-preserve-raw-v3"
ROOT = Path(__file__).resolve().parents[2]
FIELDS = {
    "product/productId": "product_id", "review/userId": "reviewer_id",
    "review/profileName": "profile_name", "review/helpfulness": "helpfulness_raw",
    "review/score": "score_raw", "review/time": "review_time_raw",
    "review/summary": "summary", "review/text": "review_text",
}
OFFICIAL = {"reviews": 7911684, "products": 253059, "users": 889176}
INT64_MIN, INT64_MAX = -(2 ** 63), 2 ** 63 - 1
SCHEMA = pa.schema([
    ("source_review_id", pa.string()), ("source_file_sha256", pa.string()),
    ("raw_record_sha256", pa.string()), ("record_sequence", pa.int64()),
    ("byte_start", pa.int64()), ("byte_end", pa.int64()),
    ("start_line", pa.int64()), ("end_line", pa.int64()),
    *[(name, pa.string()) for name in FIELDS.values()],
    ("helpful_votes", pa.int64()), ("total_votes", pa.int64()),
    ("helpfulness_ratio", pa.float64()), ("score", pa.float64()),
    ("review_time_unix", pa.int64()), ("review_time_utc", pa.string()),
    ("quality_issues", pa.list_(pa.string())),
    ("encoding_issue_fields", pa.list_(pa.string())),
    ("decoding_status", pa.string()), ("decoding_policy", pa.string()),
    ("quarantine_reasons", pa.list_(pa.string())),
    ("raw_record_base64", pa.string()),
    ("run_id", pa.string()), ("parser_version", pa.string()),
    ("rule_version", pa.string()),
])


def iter_records(stream, offset=0, next_line=1):
    """Offsets are zero-based byte positions, end exclusive; lines are one-based."""
    stream.seek(offset)
    lines = []
    pending_blanks = []
    start = offset
    start_line = next_line
    content_end, end_line = offset, next_line - 1
    while raw_line := stream.readline():
        line_number = next_line
        next_line += 1
        offset += len(raw_line)
        if raw_line.rstrip(b"\r\n") == b"":
            if lines:
                pending_blanks.append(raw_line)
            continue
        # Empty lines inside profileName/text are data. A separator is confirmed
        # by the following product header, not by the empty line alone.
        if lines and pending_blanks and raw_line.startswith(b"product/productId:"):
            yield b"".join(lines), start, content_end, start_line, end_line, offset - len(raw_line), line_number
            lines = []
            pending_blanks = []
        elif lines and pending_blanks:
            lines.extend(pending_blanks)
            pending_blanks = []
        if not lines:
            start, start_line = offset - len(raw_line), line_number
        lines.append(raw_line)
        content_end, end_line = offset, line_number
    if lines:
        yield b"".join(lines), start, content_end, start_line, end_line, offset, next_line


def parse_record(raw, sequence, source_hash, locator, run_id, encoding_policy="unresolved"):
    import base64
    if encoding_policy not in ("unresolved", "latin1_candidate"):
        raise ValueError("Unknown encoding policy")
    issues, reasons, encoding_fields = [], [], []
    row = {name: None for name in SCHEMA.names}
    row.update(source_review_id=hashlib.sha256(f"{source_hash}:{sequence}".encode("ascii")).hexdigest(),
               source_file_sha256=source_hash, raw_record_sha256=hashlib.sha256(raw).hexdigest(),
               record_sequence=sequence, byte_start=locator[0], byte_end=locator[1],
               start_line=locator[2], end_line=locator[3], run_id=run_id,
               parser_version=PARSER_VERSION, rule_version=RULE_VERSION,
               decoding_policy=encoding_policy)
    seen = set()
    field_bytes = {}
    current_prefix = None
    previous_ending = b""
    multiline_fields = set()
    # split only on physical LF. Unicode line separators inside text are data.
    physical_lines = raw.split(b"\n")
    if physical_lines and physical_lines[-1] == b"":
        physical_lines.pop()
    for raw_line in physical_lines:
        ending = b"\r\n" if raw_line.endswith(b"\r") else b"\n"
        line = raw_line[:-1] if raw_line.endswith(b"\r") else raw_line
        prefix_bytes, sep, value = line.partition(b":")
        try:
            prefix = prefix_bytes.decode("ascii")
        except UnicodeDecodeError:
            prefix = ""
        if not sep or prefix not in FIELDS:
            if current_prefix in ("review/profileName", "review/summary", "review/text"):
                field_bytes[current_prefix] += previous_ending + line
                multiline_fields.add(FIELDS[current_prefix])
            else:
                reasons.append("unrecognized_field_or_line")
            previous_ending = ending
            continue
        if prefix in seen:
            reasons.append(f"duplicate_field:{prefix}")
            current_prefix = None
            previous_ending = ending
            continue
        seen.add(prefix)
        # Remove exactly the separator's one space, preserve all remaining text.
        value = value[1:] if value.startswith(b" ") else value
        field_bytes[prefix] = value
        current_prefix = prefix
        previous_ending = ending
    if multiline_fields:
        issues.append("multiline_text_field")
    for prefix, value in field_bytes.items():
        try:
            decoded = value.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            issues.append("invalid_utf8")
            encoding_fields.append(FIELDS[prefix])
            decoded = value.decode("latin-1") if encoding_policy == "latin1_candidate" else None
        row[FIELDS[prefix]] = decoded
    for prefix in FIELDS:
        if prefix not in seen:
            reasons.append(f"missing_field:{prefix}")
    if not row["product_id"] or not row["product_id"].strip():
        reasons.append("missing_asin")
    if not row["reviewer_id"] or not row["reviewer_id"].strip():
        issues.append("missing_user_id")
    helpfulness = row["helpfulness_raw"] or ""
    match = re.fullmatch(r"([+-]?\d+)/([+-]?\d+)", helpfulness)
    if match:
        try:
            helpful, total = map(int, match.groups())
            if not all(INT64_MIN <= x <= INT64_MAX for x in (helpful, total)):
                raise ValueError("int64 overflow")
            row["helpful_votes"], row["total_votes"] = helpful, total
            if helpful < 0 or total < 0:
                issues.append("negative_helpfulness_votes")
            if helpful > total:
                issues.append("helpful_votes_exceed_total")
            if total > 0 and 0 <= helpful <= total:
                row["helpfulness_ratio"] = helpful / total
        except ValueError:
            issues.append("invalid_helpfulness_format_or_range")
    else:
        issues.append("invalid_helpfulness_format_or_range")
    try:
        score = float(row["score_raw"] or "")
        if not math.isfinite(score) or not 1 <= score <= 5:
            raise ValueError("out of range")
        row["score"] = score
    except (ValueError, OverflowError):
        issues.append("invalid_score")
    try:
        timestamp = int(row["review_time_raw"] or "")
        if not 0 <= timestamp <= INT64_MAX:
            raise ValueError("unsupported timestamp")
        row["review_time_unix"] = timestamp
        date = datetime.fromtimestamp(timestamp, timezone.utc)
        row["review_time_utc"] = date.isoformat()
        if not datetime(1997, 8, 1, tzinfo=timezone.utc) <= date < datetime(2012, 11, 1, tzinfo=timezone.utc):
            issues.append("outside_published_month_range")
    except (ValueError, OverflowError, OSError):
        issues.append("invalid_timestamp")
    row["quality_issues"] = sorted(set(issues))
    row["encoding_issue_fields"] = encoding_fields
    row["decoding_status"] = encoding_policy if encoding_fields else "utf8_valid"
    row["quarantine_reasons"] = sorted(set(reasons))
    if reasons or "invalid_utf8" in issues:
        row["raw_record_base64"] = base64.b64encode(raw).decode("ascii")
    return row


def save_json(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp, path)


def memory_peak():
    info = psutil.Process().memory_info()
    return max(info.rss, getattr(info, "peak_wset", 0))


def prepare_db(path):
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript("""
      CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS asins (id TEXT PRIMARY KEY, raw_records INTEGER NOT NULL, valid_records INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, raw_records INTEGER NOT NULL, valid_records INTEGER NOT NULL);
      CREATE TABLE IF NOT EXISTS batches (batch_id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
    """)
    return db


def persist_state(db, state):
    db.execute("INSERT INTO state VALUES (1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload",
               (json.dumps(state, ensure_ascii=False),))


def flush_batch(db, state, rows, run_dir):
    batch_id = state["batch_count"] + 1
    valid = [r for r in rows if not r["quarantine_reasons"]]
    quarantine = [r for r in rows if r["quarantine_reasons"]]
    files = []
    for folder, subset in (("reviews", valid), ("quarantine", quarantine)):
        if not subset:
            continue
        target = run_dir / folder / f"part-{batch_id:06d}.parquet"
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_suffix(".parquet.tmp")
        pq.write_table(pa.Table.from_pylist(subset, schema=SCHEMA), temp, compression="zstd")
        # A crash may leave an uncommitted part. Preserve it before retrying this batch.
        if target.exists():
            recovery = run_dir / "uncommitted" / f"{uuid.uuid4().hex}-{target.name}"
            recovery.parent.mkdir(parents=True, exist_ok=True)
            target.rename(recovery)
        os.replace(temp, target)
        files.append({"path": str(target.relative_to(run_dir)), "rows": len(subset), "sha256": hash_file(target)})
    issues, reasons = Counter(state["quality_issue_counts"]), Counter(state["quarantine_reason_counts"])
    for row in rows:
        issues.update(row["quality_issues"])
        reasons.update(row["quarantine_reasons"])
    for table, field in (("asins", "product_id"), ("users", "reviewer_id")):
        raw_counts = Counter(r[field] for r in rows if r[field] and r[field].strip())
        valid_counts = Counter(r[field] for r in valid if r[field] and r[field].strip())
        db.executemany(f"INSERT INTO {table} VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET raw_records=raw_records+excluded.raw_records, valid_records=valid_records+excluded.valid_records",
                       [(key, count, valid_counts[key]) for key, count in raw_counts.items()])
    state["records_read"] += len(rows)
    state["valid_records"] += len(valid)
    state["quarantined_records"] += len(quarantine)
    state["records_with_quality_issues"] += sum(bool(r["quality_issues"]) for r in rows)
    state["empty_user_records"] += sum(not r["reviewer_id"] or not r["reviewer_id"].strip() for r in rows)
    state["quality_issue_counts"], state["quarantine_reason_counts"] = dict(issues), dict(reasons)
    state["batch_count"] = batch_id
    state["peak_memory_bytes"] = max(state["peak_memory_bytes"], memory_peak())
    logical_hash = hashlib.sha256()
    for row in rows:
        logical_hash.update((row["source_review_id"] + row["raw_record_sha256"]).encode("ascii"))
    batch = {"batch_id": batch_id, "records": len(rows), "valid": len(valid),
             "quarantined": len(quarantine), "logical_sha256": logical_hash.hexdigest(),
             "files": files, "next_byte": state["next_byte"], "next_line": state["next_line"]}
    db.execute("INSERT INTO batches VALUES (?,?)", (batch_id, json.dumps(batch)))
    persist_state(db, state)
    db.commit()
    logging.info("batch=%s read=%s valid=%s quarantined=%s offset=%s", batch_id,
                 state["records_read"], state["valid_records"], state["quarantined_records"], state["next_byte"])


def verify_batches(db, run_dir):
    for (payload,) in db.execute("SELECT payload FROM batches ORDER BY batch_id"):
        batch = json.loads(payload)
        for file in batch["files"]:
            path = run_dir / file["path"]
            if not path.exists() or hash_file(path) != file["sha256"]:
                raise ValueError(f"Committed output missing or changed: {path}")
            if pq.read_metadata(path).num_rows != file["rows"]:
                raise ValueError(f"Committed row count mismatch: {path}")


def report(db, state, run_dir, path):
    stats = dict(state)
    full = state["status"] == "complete" and state["max_records"] is None
    stats["is_full_dataset"] = full
    stats["counts_reconcile"] = state["records_read"] == state["valid_records"] + state["quarantined_records"]
    for table, name in (("asins", "products"), ("users", "users")):
        stats[f"distinct_{name}_all_parsed"] = db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        stats[f"distinct_{name}_valid_records"] = db.execute(f"SELECT COUNT(*) FROM {table} WHERE valid_records>0").fetchone()[0]
    stats["official_reference_from_assignment"] = OFFICIAL
    stats["difference_from_official"] = ({"reviews": state["records_read"] - OFFICIAL["reviews"],
        "products": stats["distinct_products_all_parsed"] - OFFICIAL["products"],
        "users": stats["distinct_users_all_parsed"] - OFFICIAL["users"]} if full else None)
    digest = hashlib.sha256()
    for (payload,) in db.execute("SELECT payload FROM batches ORDER BY batch_id"):
        digest.update(json.loads(payload)["logical_sha256"].encode("ascii"))
    stats["ordered_records_fingerprint"] = digest.hexdigest()
    stats["fingerprint_note"] = "Compare only with the same source hash, parser/rules and batch size. Covers ordered IDs and raw record hashes."
    stats["generated_size_bytes"] = sum(p.stat().st_size for p in run_dir.rglob("*") if p.is_file())
    stats["count_semantics"] = "All parsed distinct counts include recoverable first values from quarantined rows; valid counts exclude quarantined rows. Users exclude NULL/empty/whitespace-only IDs. No deduplication."
    stats["verification_scope"] = "Counts and checkpoints; entity loading and full-project acceptance are separate tasks."
    save_json(path, stats)
    return stats


def run(args):
    if args.batch_size <= 0 or (args.max_records is not None and args.max_records <= 0):
        raise ValueError("Batch size and sample limit must be positive")
    if args.stop_after_batches is not None and args.stop_after_batches <= 0:
        raise ValueError("Stop-after-batches must be positive")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.run_name):
        raise ValueError("Run name may contain only letters, digits, underscores and hyphens")
    manifest = json.loads(args.manifest.read_text(encoding="utf-8-sig"))
    source = Path(manifest["source_file"]["path"])
    run_dir = args.output_root.resolve() / args.run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    log_dir = args.report_root.resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(log_dir / f"{args.run_name}.log", encoding="utf-8")], force=True)
    # Hold an OS lock for the entire run; Windows releases the lock after a crash.
    lock_stream = (run_dir / "run.lock").open("a+b")
    if os.name == "nt":
        import msvcrt
        lock_stream.seek(0)
        if not lock_stream.read(1):
            lock_stream.write(b"0")
            lock_stream.flush()
        lock_stream.seek(0)
        msvcrt.locking(lock_stream.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        fcntl.flock(lock_stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    db = None
    try:
        logging.info("Verifying source SHA-256 before processing")
        if source.stat().st_size != manifest["source_file"]["file_size_bytes"] or hash_file(source) != manifest["source_file"]["sha256"]:
            raise ValueError("Source file does not match its manifest")
        db = prepare_db(run_dir / "checkpoint.sqlite3")
        config = {"source_sha256": manifest["source_file"]["sha256"],
                  "parser_version": PARSER_VERSION, "rule_version": RULE_VERSION,
                  "batch_size": args.batch_size, "max_records": args.max_records,
                  "encoding_policy": args.encoding_policy}
        existing = db.execute("SELECT payload FROM state WHERE id=1").fetchone()
        if existing:
            if not args.resume:
                raise ValueError("Run exists. Use --resume or a new --run-name")
            state = json.loads(existing[0])
            if any(state.get(k) != v for k, v in config.items()):
                raise ValueError("Resume configuration differs from original run")
            verify_batches(db, run_dir)
            if state["status"] == "complete":
                logging.info("Already complete; verified committed files, no duplicate output created")
                return report(db, state, run_dir, log_dir / f"{args.run_name}_statistics.json")
        else:
            state = dict(config, run_id=uuid.uuid4().hex, started_at=utc_now(), status="running",
                         next_byte=0, next_line=1, records_read=0, valid_records=0,
                         quarantined_records=0, batch_count=0, records_with_quality_issues=0,
                         empty_user_records=0, quality_issue_counts={}, quarantine_reason_counts={},
                         peak_memory_bytes=memory_peak(), processing_seconds=0.0, sessions=[])
            state["environment"] = {"python": sys.version, "pyarrow": pa.__version__,
                                    "psutil": psutil.__version__, "platform": sys.platform}
            save_json(run_dir / "source_manifest.json", manifest)
            persist_state(db, state)
            db.commit()
        started = time.perf_counter()
        previous_processing_seconds = state["processing_seconds"]
        state["status"] = "running"
        session_started_at = utc_now()
        initial_batches = state["batch_count"]
        rows = []
        interrupted = False
        try:
            with source.open("rb") as stream:
                for record in iter_records(stream, state["next_byte"], state["next_line"]):
                    raw, start, end, line_start, line_end, next_byte, next_line = record
                    if args.max_records is not None and state["records_read"] + len(rows) >= args.max_records:
                        break
                    row = parse_record(raw, state["records_read"] + len(rows) + 1,
                                       config["source_sha256"], (start, end, line_start, line_end), state["run_id"],
                                       args.encoding_policy)
                    rows.append(row)
                    state["next_byte"], state["next_line"] = next_byte, next_line
                    if len(rows) >= args.batch_size:
                        state["processing_seconds"] = previous_processing_seconds + time.perf_counter() - started
                        flush_batch(db, state, rows, run_dir)
                        rows = []
                        if args.stop_after_batches and state["batch_count"] - initial_batches >= args.stop_after_batches:
                            interrupted = True
                            break
                if rows:
                    state["processing_seconds"] = previous_processing_seconds + time.perf_counter() - started
                    flush_batch(db, state, rows, run_dir)
        except KeyboardInterrupt:
            # The persisted checkpoint is the last complete batch. Reload it so an
            # interrupt during a write cannot publish an in-memory, uncommitted offset.
            db.rollback()
            state = json.loads(db.execute("SELECT payload FROM state WHERE id=1").fetchone()[0])
            interrupted = True
        except Exception:
            db.rollback()
            state = json.loads(db.execute("SELECT payload FROM state WHERE id=1").fetchone()[0])
            state["status"] = "failed"
            state["processing_seconds"] = previous_processing_seconds + time.perf_counter() - started
            state["sessions"].append({"started_at": session_started_at, "ended_at": utc_now(), "failed": True})
            persist_state(db, state)
            db.commit()
            report(db, state, run_dir, log_dir / f"{args.run_name}_statistics.json")
            logging.exception("Processing failed; resume will use the last committed batch")
            raise
        elapsed = time.perf_counter() - started
        state["processing_seconds"] = previous_processing_seconds + elapsed
        state["peak_memory_bytes"] = max(state["peak_memory_bytes"], memory_peak())
        state["sessions"].append({"started_at": session_started_at, "ended_at": utc_now(),
                                  "processing_seconds": elapsed, "interrupted": interrupted})
        state["status"] = "interrupted" if interrupted else "complete"
        state["finished_at"] = None if interrupted else utc_now()
        persist_state(db, state)
        db.commit()
        result = report(db, state, run_dir, log_dir / f"{args.run_name}_statistics.json")
        logging.info("status=%s full_dataset=%s read=%s products=%s users=%s", result["status"],
                     result["is_full_dataset"], result["records_read"], result["distinct_products_all_parsed"], result["distinct_users_all_parsed"])
        return result
    finally:
        if db is not None:
            db.close()
        lock_stream.close()
        for handler in logging.getLogger().handlers[:]:
            handler.close()
            logging.getLogger().removeHandler(handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--run-name", required=True)
    parser.add_argument("--encoding-policy", choices=("unresolved", "latin1_candidate"), required=True)
    parser.add_argument("--batch-size", type=int, default=25000)
    parser.add_argument("--max-records", type=int, help="Sample only; never counted as full-dataset results")
    parser.add_argument("--stop-after-batches", type=int, help="Intentional interruption at a batch boundary")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--output-root", type=Path, default=ROOT / "data/staging/task1")
    parser.add_argument("--report-root", type=Path, default=ROOT / "reports/task1")
    args = parser.parse_args()
    result = run(args)
    if result["status"] == "interrupted":
        raise SystemExit(130)


if __name__ == "__main__":
    main()
