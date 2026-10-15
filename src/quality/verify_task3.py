"""Full Task3 integrity/evidence replay; not independent classification accuracy."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sqlite3
import sys

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/transform"))
from classify_products import (CANDIDATE_SCHEMA, ENGINE_VERSION, LABELS, PRODUCT_SCHEMA, classify_record, consensus,
    digest_json, extract_fields, field_coverage, input_registration, json_text, load_config, load_rules, source_record_id)
from acquire_products import save_json
from register_source import hash_file, utc_now


def check_export(db, table, schema, info, order):
    if hash_file(info["path"]) != info["sha256"]:
        raise ValueError(f"{table} output hash mismatch")
    if Path(info["path"]).stat().st_size != info["file_size_bytes"] or pq.ParquetFile(info["path"]).schema_arrow != schema:
        raise ValueError(f"{table} file size/schema mismatch")
    cursor = db.execute(f"SELECT {','.join(schema.names)} FROM {table} ORDER BY {order}")
    count = 0
    ordered_hash = hashlib.sha256()
    for batch in pq.ParquetFile(info["path"]).iter_batches(batch_size=2000):
        rows = batch.to_pylist()
        if rows != [dict(row) for row in cursor.fetchmany(len(rows))]:
            raise ValueError(f"{table} exported fields differ from checkpoint")
        for row in rows:
            ordered_hash.update((json_text(row) + "\n").encode())
        count += len(rows)
    if cursor.fetchone() is not None or count != info["rows"] or ordered_hash.hexdigest() != info["ordered_rows_sha256"]:
        raise ValueError(f"{table} count/ordered-hash mismatch")
    return count


def verify(config):
    task2, registration = input_registration(config)
    rules = load_rules(config["rules_file"])
    stats = json.loads((Path(config["report_dir"]) / "full_statistics.json").read_text(encoding="utf-8"))
    if stats["status"] != "complete" or stats["input_registration"] != registration:
        raise ValueError("Cannot verify incomplete or changed-source decisions")
    with closing(sqlite3.connect((Path(config["work_dir"]) / "classification.sqlite3").as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        if db.execute("PRAGMA quick_check").fetchone()[0] != "ok" or db.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("Checkpoint integrity/foreign-key error")
        if json.loads(db.execute("SELECT value FROM state WHERE key='input'").fetchone()[0]) != registration:
            raise ValueError("Checkpoint binding changed")
        if not db.execute("SELECT 1 FROM state WHERE key='complete' AND value='true'").fetchone():
            raise ValueError("Checkpoint not complete")
        for name, filename in (("acquisition", "acquisition.sqlite3"), ("metadata", "metadata.sqlite3")):
            db.execute(f"ATTACH DATABASE ? AS {name}", ((Path(task2["work_dir"]) / filename).as_uri() + "?mode=ro",))
        missing = db.execute("SELECT COUNT(*) FROM (SELECT asin FROM acquisition.products EXCEPT SELECT asin FROM products)").fetchone()[0]
        extra = db.execute("SELECT COUNT(*) FROM (SELECT asin FROM products EXCEPT SELECT asin FROM acquisition.products)").fetchone()[0]
        bad_refs = db.execute("""SELECT COUNT(*) FROM products p JOIN acquisition.products a USING(asin) WHERE
            p.review_count!=a.review_count OR p.first_review_id!=a.first_review_id OR p.acquisition_status!=a.status
            OR p.source_review_sha256!=a.source_sha256 OR p.source_review_run_id!=a.source_run_id""").fetchone()[0]
        if missing or extra or bad_refs:
            raise ValueError("ASIN membership or review/acquisition references changed")
        products = check_export(db, "products", PRODUCT_SCHEMA, stats["outputs"]["products"], "asin")
        candidates = check_export(db, "candidates", CANDIDATE_SCHEMA, stats["outputs"]["candidates"], "source_record_id")
        source_total = db.execute("SELECT COUNT(*) FROM metadata.records").fetchone()[0]
        if source_total != candidates:
            raise ValueError("Source candidates skipped or duplicated")
        raw_verified = 0
        for row in db.execute("SELECT * FROM candidates ORDER BY source_record_id"):
            raw = db.execute("SELECT * FROM metadata.records WHERE source_id=? AND record_sequence=?", (row["source_id"], row["record_sequence"])).fetchone()
            if raw is None:
                raise ValueError("Classification evidence has no raw source")
            for key in ("asin", "archive_sha256", "byte_start", "byte_end", "raw_record_sha256", "source_downloaded_at"):
                if row[key] != raw[key]:
                    raise ValueError("Classification raw locator or identity changed")
            if source_record_id(raw) != row["source_record_id"] or hashlib.sha256(raw["raw_record"]).hexdigest() != row["raw_record_sha256"]:
                raise ValueError("Source record ID/hash mismatch")
            item = json.loads(raw["raw_record"])
            if item["asin"] != row["asin"]:
                raise ValueError("Evidence ASIN mismatch")
            decision = classify_record(item, rules)
            expected = dict(decision, rule_ids=sorted({e["rule_id"] for e in decision["evidence"]}), fields=extract_fields(item, rules))
            for key in ("label", "reason", "confidence", "evidence", "rule_ids", "fields"):
                actual = json.loads(row[key]) if key in {"evidence", "rule_ids", "fields"} else row[key]
                if actual != expected[key]:
                    raise ValueError(f"Decision/field replay differs: {key}")
            if row["rule_version"] != rules["version"] or not db.execute("SELECT 1 FROM runs WHERE run_id=?", (row["decision_run_id"],)).fetchone():
                raise ValueError("Candidate version/run lineage missing")
            raw_verified += 1
        labels, reasons = Counter(), Counter()
        no_metadata = identity_unresolved = reviews = 0
        fingerprint = digest_json(registration)
        for row in db.execute("SELECT * FROM products ORDER BY asin"):
            evidence = [dict(c, fields=json.loads(c["fields"])) for c in db.execute("SELECT * FROM candidates WHERE asin=? ORDER BY source_id,record_sequence", (row["asin"],))]
            expected = consensus(evidence)
            if (row["label"], row["reason"], row["confidence"]) != expected or row["candidate_count"] != len(evidence):
                raise ValueError("Product decision/candidate count is unsupported")
            if json.loads(row["evidence_ids"]) != [c["source_record_id"] for c in evidence]:
                raise ValueError("Missing or unrelated product evidence references")
            if row["rule_hash"] != registration["rule_file_sha256"] or row["input_fingerprint"] != fingerprint or row["engine_version"] != ENGINE_VERSION or row["rule_version"] != rules["version"]:
                raise ValueError("Product source/rule version binding mismatch")
            if not db.execute("SELECT 1 FROM runs WHERE run_id=?", (row["decision_run_id"],)).fetchone():
                raise ValueError("Product run lineage missing")
            if row["label"] != "awaiting_confirmation" and not evidence:
                raise ValueError("Resolved product has no evidence")
            labels[row["label"]] += 1
            reasons[row["reason"]] += 1
            reviews += row["review_count"]
            no_metadata += not evidence
            identity_unresolved += row["reason"] == "inner_asin_conflict"
        if {label: labels[label] for label in LABELS} != stats["classification_counts"] or dict(reasons) != stats["reason_counts"] or sum(labels.values()) != registration["products"]:
            raise ValueError("Classification statistics do not reconcile")
        if reviews != registration["reviews"] or reviews != stats["review_references_retained"]:
            raise ValueError("Reviews omitted or copied")
        if stats["products"] != products or stats["total_target_products"] != products or stats["classification_candidate_records"] != candidates:
            raise ValueError("Reported output counts differ from full data")
        if stats["identified_movie_products"] != labels["movie"] or stats["identification_coverage_all_products"] != labels["movie"]/products or stats["unresolved_rate_all_products"] != labels["awaiting_confirmation"]/products:
            raise ValueError("Identification/unresolved coverage denominator differs")
        if field_coverage(db) != stats["field_coverage"]:
            raise ValueError("Field coverage counts differ")
        acquired = json.loads((Path(task2["report_dir"]) / "full_statistics.json").read_text(encoding="utf-8"))
        acquisition = stats["acquisition"]
        if acquisition["statuses"] != acquired["statuses"] or acquisition["denominator_all_products"] != products or acquisition["page_success_rate"] != acquired["success_rate_all_products"]:
            raise ValueError("Acquisition denominator/distribution altered")
        metadata_stats = json.loads((Path(task2["report_dir"]) / "metadata_statistics.json").read_text(encoding="utf-8"))
        if acquisition["live_success"] != acquired["successful_live"] or acquisition["authorized_html_snapshot_success"] != acquired["successful_authorized_snapshots"] or acquisition["third_party_is_page_success"] is not False:
            raise ValueError("Live/offline/third-party measures mixed")
        if acquisition["third_party_matched_products"] != metadata_stats["matched_asins"] or acquisition["third_party_coverage"] != metadata_stats["coverage_all_target_products"]:
            raise ValueError("Third-party coverage differs from actual evidence")
        if no_metadata != metadata_stats["unmatched_target_asins"] or identity_unresolved != metadata_stats["identity_conflict_asins"]:
            raise ValueError("Missing/identity-conflicting products were not preserved unresolved")
    return {"verified_at": utc_now(), "status": "passed", "products": products, "candidate_records": candidates,
        "missing_asins": missing, "extra_asins": extra, "review_reference_mismatches": bad_refs,
        "review_references_retained": reviews, "raw_candidate_evidence_replayed": raw_verified,
        "all_classification_and_field_candidates_match_raw_replay": True,
        "all_product_decisions_replayed": products, "no_metadata_products_retained_unresolved": no_metadata,
        "identity_conflicting_products_unresolved": identity_unresolved,
        "classification_counts_reconcile": True, "all_exported_fields_match_checkpoint": True,
        "acquisition_denominator_is_all_products": True,
        "scope": "Full integrity, provenance, deterministic decision/field replay and count verification. NOT independent manually labeled accuracy, entity matching or target loading evaluation."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config/task3.example.json")
    args = parser.parse_args()
    config = load_config(args.config)
    result = verify(config)
    save_json(Path(config["report_dir"]) / "full_verification.json", result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
