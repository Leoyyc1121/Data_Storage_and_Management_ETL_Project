"""Verify committed Task 1 parts and compare repeat/recovery runs."""
from __future__ import annotations

import argparse
from contextlib import closing
import hashlib
import json
import sqlite3
import sys
from pathlib import Path

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "extract"))
from parse_reviews import save_json
from register_source import hash_file, utc_now


def snapshot(run_dir):
    uri = (Path(run_dir).resolve() / "checkpoint.sqlite3").as_uri() + "?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as db:
        state = json.loads(db.execute("SELECT payload FROM state WHERE id=1").fetchone()[0])
        batches = [json.loads(row[0]) for row in db.execute("SELECT payload FROM batches ORDER BY batch_id")]
    return state, batches


def verify(run_dir):
    run_dir = Path(run_dir).resolve()
    state, batches = snapshot(run_dir)
    manifest = json.loads((run_dir / "source_manifest.json").read_text(encoding="utf-8"))
    source = Path(manifest["source_file"]["path"])
    if hash_file(source) != state["source_sha256"]:
        raise ValueError("Source checksum mismatch")
    source_size = source.stat().st_size
    counts = {"records": 0, "valid": 0, "quarantined": 0, "raw_locators_checked": 0}
    seq = 1
    fingerprint = hashlib.sha256()
    previous_end = 0
    with source.open("rb") as raw_source:
        for batch in batches:
            entries = []
            for file in batch["files"]:
                path = run_dir / file["path"]
                if hash_file(path) != file["sha256"]:
                    raise ValueError(f"Output checksum mismatch: {path}")
                table = pq.read_table(path, columns=["record_sequence", "source_review_id", "raw_record_sha256",
                                                     "byte_start", "byte_end", "quarantine_reasons"])
                if table.num_rows != file["rows"]:
                    raise ValueError(f"Parquet row count mismatch: {path}")
                entries.extend(table.to_pylist())
            entries.sort(key=lambda row: row["record_sequence"])
            if len(entries) != batch["records"]:
                raise ValueError("Batch count mismatch")
            digest = hashlib.sha256()
            for i, row in enumerate(entries):
                if row["record_sequence"] != seq:
                    raise ValueError(f"Missing or duplicate sequence near {seq}")
                expected_id = hashlib.sha256(f"{state['source_sha256']}:{seq}".encode("ascii")).hexdigest()
                if row["source_review_id"] != expected_id:
                    raise ValueError(f"Unstable review ID at {seq}")
                if not previous_end <= row["byte_start"] < row["byte_end"] <= source_size:
                    raise ValueError(f"Invalid raw byte interval at {seq}")
                previous_end = row["byte_end"]
                digest.update((row["source_review_id"] + row["raw_record_sha256"]).encode("ascii"))
                counts["quarantined" if row["quarantine_reasons"] else "valid"] += 1
                counts["records"] += 1
                seq += 1
                # Verify exact source bytes for each batch's first, middle and last record.
                if i in (0, len(entries) // 2, len(entries) - 1):
                    raw_source.seek(row["byte_start"])
                    raw = raw_source.read(row["byte_end"] - row["byte_start"])
                    if hashlib.sha256(raw).hexdigest() != row["raw_record_sha256"]:
                        raise ValueError(f"Raw locator mismatch at {row['record_sequence']}")
                    counts["raw_locators_checked"] += 1
            if digest.hexdigest() != batch["logical_sha256"]:
                raise ValueError(f"Logical fingerprint mismatch: batch {batch['batch_id']}")
            fingerprint.update(digest.hexdigest().encode("ascii"))
            if batch["batch_id"] % 25 == 0:
                print(f"Verified {batch['batch_id']} batches / {counts['records']} records", flush=True)
    if (counts["records"], counts["valid"], counts["quarantined"]) != (
            state["records_read"], state["valid_records"], state["quarantined_records"]):
        raise ValueError("Checkpoint and actual Parquet counts differ")
    return {"verified_at": utc_now(), "run_dir": str(run_dir), "status": "passed",
            "run_status": state["status"], "scope": "full input" if state["max_records"] is None else "sample",
            **counts, "ordered_records_fingerprint": fingerprint.hexdigest(),
            "all_output_file_checksums_verified": True, "all_record_sequences_and_ids_verified": True,
            "raw_locator_scope": "First, middle and last row of every batch; not every raw record"}


def compare(left_dir, right_dir):
    left_dir, right_dir = Path(left_dir).resolve(), Path(right_dir).resolve()
    left, lb = snapshot(left_dir)
    right, rb = snapshot(right_dir)
    for key in ("source_sha256", "parser_version", "rule_version", "encoding_policy", "batch_size", "max_records", "status"):
        if left[key] != right[key]:
            raise ValueError(f"Run configuration differs: {key}")
    if left["status"] != "complete" or len(lb) != len(rb):
        raise ValueError("Both runs must be complete with equal batch counts")
    compared = 0
    for lbatch, rbatch in zip(lb, rb):
        if lbatch["logical_sha256"] != rbatch["logical_sha256"]:
            raise ValueError("Ordered input records differ")
        lf = {f["path"]: f for f in lbatch["files"]}
        rf = {f["path"]: f for f in rbatch["files"]}
        if lf.keys() != rf.keys():
            raise ValueError("Output partition layout differs")
        for key in lf:
            ltable = pq.read_table(left_dir / key).drop(["run_id"])
            rtable = pq.read_table(right_dir / key).drop(["run_id"])
            if not ltable.equals(rtable):
                raise ValueError(f"Parsed fields differ: {key}")
            compared += ltable.num_rows
    return {"verified_at": utc_now(), "status": "passed", "left": str(left_dir), "right": str(right_dir),
            "scope": "full input" if left["max_records"] is None else "sample",
            "records_compared": compared, "all_fields_except_run_id_equal": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--compare-with", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = compare(args.run_dir, args.compare_with) if args.compare_with else verify(args.run_dir)
    save_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
