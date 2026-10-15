"""Validate the full acquisition manifest against Task 1 and retained evidence."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

import pyarrow.parquet as pq
from protego import Protego

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "extract"))
from acquire_products import SCHEMA, STATUSES, load_config, origin, save_json, timestamp
from register_source import hash_file, utc_now


def verify(config):
    stats = json.loads((Path(config["report_dir"]) / "full_statistics.json").read_text(encoding="utf-8"))
    manifest = Path(config["work_dir"]) / "product_manifest.parquet"
    if hash_file(manifest) != stats["product_manifest"]["sha256"]:
        raise ValueError("Manifest SHA-256 differs from statistics")
    db_path = Path(config["work_dir"]) / "acquisition.sqlite3"
    db = sqlite3.connect(db_path.as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    try:
        if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("Acquisition checkpoint integrity check failed")
        db.execute("ATTACH DATABASE ? AS source", ((Path(config["source_run"]) / "checkpoint.sqlite3").as_uri() + "?mode=ro",))
        source_state = json.loads(db.execute("SELECT payload FROM source.state").fetchone()[0])
        missing = db.execute("SELECT COUNT(*) FROM (SELECT id FROM source.asins EXCEPT SELECT asin FROM products)").fetchone()[0]
        extra = db.execute("SELECT COUNT(*) FROM (SELECT asin FROM products EXCEPT SELECT id FROM source.asins)").fetchone()[0]
        bad_counts = db.execute("SELECT COUNT(*) FROM products p JOIN source.asins s ON p.asin=s.id WHERE p.review_count!=s.raw_records").fetchone()[0]
        if missing or extra or bad_counts:
            raise ValueError(f"ASIN input reconciliation failed: missing={missing}, extra={extra}, bad_review_counts={bad_counts}")
        attempt_mismatch = db.execute("""SELECT COUNT(*) FROM products p LEFT JOIN
            (SELECT asin,COUNT(*) AS n FROM attempts GROUP BY asin) a ON p.asin=a.asin
            WHERE p.attempts!=COALESCE(a.n,0)""").fetchone()[0]
        if attempt_mismatch:
            raise ValueError("Product attempt totals differ from attempt journal")
        cursor = db.execute("SELECT " + ",".join(SCHEMA.names) + " FROM products ORDER BY asin")
        seen_evidence, seen_snapshots = {}, {}
        counts = Counter()
        rows = policy_checks = pending_policy_checks = success_checks = 0
        previous = None
        logical_hash = hashlib.sha256()
        for batch in pq.ParquetFile(manifest).iter_batches(batch_size=5000):
            records = batch.to_pylist()
            checkpoint_rows = [dict(row) for row in cursor.fetchmany(len(records))]
            if records != checkpoint_rows:
                raise ValueError("Exported rows differ from the checkpoint")
            for row in records:
                asin = row["asin"]
                if previous is not None and asin <= previous:
                    raise ValueError("ASIN primary key/order violation")
                previous = asin
                if row["status"] not in STATUSES or row["attempts"] < 0 or row["review_count"] <= 0:
                    raise ValueError("Invalid manifest status or count")
                if not row["original_url"] or not row["source_sha256"] or not row["source_run_id"]:
                    raise ValueError("Missing input lineage")
                if row["source_sha256"] != source_state["source_sha256"] or row["source_run_id"] != source_state["run_id"] or not row["first_review_id"]:
                    raise ValueError("Product input lineage differs from the actual Task 1 run")
                counts[row["status"]] += 1
                rows += 1
                logical_hash.update((json.dumps(row, sort_keys=True, ensure_ascii=True, separators=(",", ":")) + "\n").encode())
                evidence = None
                if row["evidence_path"]:
                    path = row["evidence_path"]
                    if path not in seen_evidence:
                        seen_evidence[path] = json.loads(Path(path).read_text(encoding="utf-8"))
                    evidence = seen_evidence[path]
                if row["status"] == "restricted_access" and row["attempts"] == 0 and not evidence:
                    raise ValueError("An unrequested restricted product must reference actual policy evidence")
                if row["snapshot_path"]:
                    path = row["snapshot_path"]
                    if path not in seen_snapshots:
                        seen_snapshots[path] = hash_file(path)
                    if seen_snapshots[path] != row["content_hash"]:
                        raise ValueError("Response snapshot checksum mismatch")
                if row["acquired_at"]:
                    timestamp(row["acquired_at"])
                if row["status"] == "success":
                    if not all((row["http_status"] == 200, row["verified_asin"] == asin,
                            row["snapshot_path"], row["content_hash"], row["acquired_at"], evidence,
                            row["acquisition_mode"] in {"live", "authorized_snapshot"})):
                        raise ValueError("Success lacks verified identity/raw evidence")
                    if evidence.get("asin") != asin or evidence.get("status") != "success":
                        raise ValueError("Success evidence does not match product")
                    success_checks += 1
                if row["status"] in {"restricted_access", "not_collected"} and row["attempts"] == 0 and evidence:
                    if not evidence or any(row[key] is not None for key in
                            ("final_url", "http_status", "acquired_at", "snapshot_path", "content_hash")):
                        raise ValueError("Unrequested product has invented HTTP fields or lacks policy evidence")
                    policy_path = evidence.get("snapshot_path")
                    if not policy_path or hash_file_cached(policy_path, seen_snapshots) != evidence.get("content_hash"):
                        raise ValueError("Policy evidence checksum mismatch")
                    if row["reason"] == "robots_disallow":
                        if "parser" not in evidence:
                            evidence["parser"] = Protego.parse(Path(policy_path).read_text(encoding="utf-8-sig"))
                        if evidence["parser"].can_fetch(row["original_url"], config["robots_agent"]):
                            raise ValueError("Policy does not actually disallow the product URL")
                    if row["status"] == "restricted_access":
                        policy_checks += 1
                    else:
                        pending_policy_checks += 1
        if cursor.fetchone() is not None:
            raise ValueError("Export omitted database rows")
        if rows != stats["distinct_asins"] or logical_hash.hexdigest() != stats["product_manifest"]["ordered_rows_sha256"]:
            raise ValueError("Manifest row total/fingerprint mismatch")
        if any(counts[status] != stats["statuses"][status] for status in STATUSES):
            raise ValueError("Status reconciliation failed")
        total_attempts = db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]
        if total_attempts != stats["attempts_total"]:
            raise ValueError("Attempt statistics mismatch")
        result = {"verified_at": utc_now(), "status": "passed", "products": rows,
            "missing_asins": missing, "extra_asins": extra, "per_asin_review_count_mismatches": bad_counts,
            "all_exported_fields_match_checkpoint": True, "all_asins_unique": True,
            "status_counts_reconcile": True, "attempt_journal_reconciles": True,
            "policy_denied_urls_checked": policy_checks, "successful_page_evidence_checked": success_checks,
            "policy_pending_urls_checked": pending_policy_checks,
            "unique_evidence_files_read": len(seen_evidence), "unique_snapshots_hashed": len(seen_snapshots),
            "product_attempts": total_attempts, "manifest_sha256": stats["product_manifest"]["sha256"],
            "ordered_rows_sha256": logical_hash.hexdigest(),
            "scope": "Complete Task 2 manifest, exact Task 1 ASIN membership and per-ASIN counts, all exported fields and referenced final raw evidence. Zero successful pages is not full page acquisition."}
        if (Path(config["report_dir"]) / "metadata_statistics.json").exists():
            from acquire_product_metadata import verify_metadata
            result["third_party_metadata"] = verify_metadata(config)
        return result
    finally:
        db.close()


def hash_file_cached(path, cache):
    if path not in cache:
        cache[path] = hash_file(path)
    return cache[path]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path(__file__).resolve().parents[2] / "config/task2.example.json")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    config = load_config(args.config)
    result = verify(config)
    save_json(args.output or Path(config["report_dir"]) / "full_verification.json", result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
