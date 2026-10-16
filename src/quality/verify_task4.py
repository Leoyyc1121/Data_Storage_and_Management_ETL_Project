"""Full Task4 source assertions, candidate decisions, cluster and mapping replay."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
import hashlib
import itertools
import json
from pathlib import Path
import sqlite3
import sys
import uuid

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/"src/transform"))
from resolve_movies import (DECISIONS, EXPORTS, NAMESPACE, compare, complete_link_clusters, digest_json, generate_candidates,
    input_registration, json_text, load_config, load_rules, make_profile, read_profiles, save_json, state)
from register_source import hash_file, utc_now


def verify(config):
    task3, registration = input_registration(config, verify_input=False)
    rules = load_rules(config["rules_file"])
    stats = json.loads((Path(config["report_dir"])/"full_statistics.json").read_text(encoding="utf-8"))
    if stats["input_registration"] != registration:
        raise ValueError("Source/rule binding changed")
    with closing(sqlite3.connect((Path(config["work_dir"])/"resolution.sqlite3").as_uri()+"?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        if db.execute("PRAGMA quick_check").fetchone()[0] != "ok" or db.execute("PRAGMA foreign_key_check").fetchone():
            raise ValueError("Checkpoint integrity or FK violation")
        if state(db, "input") != registration or not all(state(db, key) for key in ("profiles_complete", "pairs_complete", "clusters_complete", "evaluation_complete")):
            raise ValueError("Incomplete or changed checkpoint")
        from acquire_products import load_config as load_task2
        task2 = load_task2(task3["task2_config"])
        db.execute("ATTACH DATABASE ? AS task3", ((Path(task3["work_dir"])/"classification.sqlite3").as_uri()+"?mode=ro",))
        db.execute("ATTACH DATABASE ? AS metadata", ((Path(task2["work_dir"])/"metadata.sqlite3").as_uri()+"?mode=ro",))
        for query in ("SELECT asin FROM task3.products EXCEPT SELECT asin FROM products", "SELECT asin FROM products EXCEPT SELECT asin FROM task3.products", "SELECT asin FROM products EXCEPT SELECT asin FROM product_status"):
            if db.execute(query).fetchone():
                raise ValueError("ASIN lost, added or missing mapping status")
        source_records = 0
        for row in db.execute("SELECT * FROM products ORDER BY asin"):
            source = db.execute("SELECT * FROM task3.products WHERE asin=?", (row["asin"],)).fetchone()
            records = [dict(r) for r in db.execute("SELECT * FROM metadata.records WHERE asin=? ORDER BY source_id,record_sequence", (row["asin"],))]
            expected = make_profile(source, records, rules)
            if json.loads(row["profile"]) != expected or digest_json(expected) != row["profile_hash"] or row["metadata_records"] != len(records) or row["review_count"] != source["review_count"] or row["classification"] != source["label"]:
                raise ValueError("Profile/credit/year assertion does not match retained raw source")
            source_records += len(records)
        profiles = read_profiles(db)
        generated, skipped = generate_candidates(profiles, rules)
        actual_keys = [(r["pair_id"], r["left_asin"], r["right_asin"]) for r in db.execute("SELECT * FROM pairs ORDER BY pair_id")]
        if generated != actual_keys or skipped != stats["nonexact_high_frequency_prefixes_skipped"]:
            raise ValueError("Candidate generation skipped/added pairs")
        pairs = []
        for row in db.execute("SELECT * FROM pairs ORDER BY pair_id"):
            expected = compare(profiles[row["left_asin"]], profiles[row["right_asin"]], rules)
            if json.loads(row["evidence"]) != expected or row["proposed_decision"] != expected["proposed_decision"] or row["rule_version"] != rules["version"]:
                raise ValueError("Pair evidence/decision replay differs")
            pairs.append((row["pair_id"], row["left_asin"], row["right_asin"], expected))
        groups, checks, events, blocked = complete_link_clusters(profiles, pairs, rules)
        actual_checks = [tuple(row) for row in db.execute("SELECT merge_pair_id,left_asin,right_asin,decision,evidence FROM cluster_checks ORDER BY merge_pair_id,left_asin,right_asin")]
        expected_checks = sorted((pid,a,b,d["proposed_decision"],json_text(d)) for pid,a,b,d in checks)
        if actual_checks != expected_checks:
            raise ValueError("Cross-cluster consistency evidence missing/altered")
        for pid, _, _, decision in pairs:
            row = db.execute("SELECT final_decision,rule_id FROM pairs WHERE pair_id=?", (pid,)).fetchone()
            expected = ("manual_review", blocked[pid]) if pid in blocked else (decision["final_decision"], decision["rule_id"])
            if tuple(row) != expected:
                raise ValueError("Final pair decision ignores cluster conflict")
        expected_relations, expected_works = set(), set()
        within_checks = 0
        for group in groups:
            anchors = sorted(rec["source_record_id"] for asin in group if profiles[asin]["classification"] == "movie" for rec in profiles[asin]["records"])
            mid = "mw_" + uuid.uuid5(NAMESPACE, "creative-work-seed:"+anchors[0]).hex
            expected_works.add(mid)
            work = db.execute("SELECT * FROM works WHERE movie_id=?", (mid,)).fetchone()
            if work is None or work["anchor_source_record_id"] != anchors[0] or work["status"] != ("multifield_group" if len(group)>1 else "provisional_singleton"):
                raise ValueError("Work identity or status differs")
            source = next(rec for asin in group for rec in profiles[asin]["records"] if rec["source_record_id"] == anchors[0])
            if work["display_title"] != source["title_raw"] or work["display_title_source"] != anchors[0]:
                raise ValueError("Unsourced provisional display title")
            for a,b in itertools.combinations(group,2):
                if compare(profiles[a], profiles[b], rules)["proposed_decision"] != "auto_merge":
                    raise ValueError("Transitive incorrect merge found inside actual work group")
                within_checks += 1
            expected_relations.update((asin, mid, "single_work") for asin in group)
        actual_relations = {tuple(row) for row in db.execute("SELECT asin,movie_id,relationship FROM product_movie")}
        if expected_relations != actual_relations or expected_works != {r[0] for r in db.execute("SELECT movie_id FROM works")}:
            raise ValueError("Product-work mapping differs from guarded clusters")
        for row in db.execute("SELECT pm.*,p.profile FROM product_movie pm JOIN products p USING(asin)"):
            evidence = json.loads(row["evidence"])
            p = json.loads(row["profile"])
            if evidence["source_record_ids"] != [rec["source_record_id"] for rec in p["records"]]:
                raise ValueError("Mapping raw source evidence lost")
        if {row["event_id"]: json.loads(row["event"]) for row in db.execute("SELECT event_id,event FROM mapping_history")} != {digest_json(event): event for event in events}:
            raise ValueError("Mapping history missing")
        for row in db.execute("SELECT * FROM product_status ORDER BY asin"):
            product = db.execute("SELECT classification FROM products WHERE asin=?", (row["asin"],)).fetchone()[0]
            work = db.execute("SELECT w.status FROM works w JOIN product_movie pm USING(movie_id) WHERE pm.asin=?", (row["asin"],)).fetchone()
            expected = "merged" if work and work[0] == "multifield_group" else "provisional_work" if work else "compilation_unresolved" if product == "compilation" else "pending_classification" if product == "awaiting_confirmation" else "excluded_content"
            if expected != row["mapping_status"]:
                raise ValueError("Product mapping status inconsistent")
        heldout = {r[0] for r in db.execute("SELECT pair_id FROM evaluation_pairs")}
        if heldout & set(registration["development_pair_ids"]):
            raise ValueError("Evaluation queue overlaps declared development pairs")
        queue = json.loads(Path(stats["evaluation"]["queue_path"]).read_text(encoding="utf-8"))
        if hash_file(stats["evaluation"]["queue_path"]) != stats["evaluation"]["queue_sha256"] or {r["pair_id"] for r in queue["pairs"]} != heldout or any(r["label"] is not None for r in queue["pairs"]):
            raise ValueError("Unlabeled queue altered; human labels must be saved separately")
        for packet in queue["pairs"]:
            pair = db.execute("SELECT left_asin,right_asin FROM pairs WHERE pair_id=?", (packet["pair_id"],)).fetchone()
            for side, asin in zip(("left", "right"), pair):
                expected_packet = {key: profiles[asin][key] for key in ("asin", "classification", "raw_titles", "values", "variants", "records")}
                if packet[side] != expected_packet:
                    raise ValueError("Human evaluation raw evidence packet does not match source")
            if packet["partition"] != "independent_human_evaluation" or any(packet[key] is not None for key in ("labeled_by", "labeler_type", "labeled_at", "rationale")) or packet["labeling_evidence"]:
                raise ValueError("Generated evaluation queue must not fabricate human labeling")
        for table in ("products", "pairs", "works", "product_movie", "product_status", "mapping_history"):
            if db.execute(f"SELECT 1 FROM {table} t LEFT JOIN runs r ON t.run_id=r.run_id WHERE r.run_id IS NULL LIMIT 1").fetchone():
                raise ValueError("Missing decision/source run lineage")
        review_count = db.execute("SELECT SUM(review_count) FROM products").fetchone()[0]
        if review_count != registration["reviews"] or review_count != stats["reviews_retained"]:
            raise ValueError("Reviews lost/duplicated")
        for key, (_, sql) in EXPORTS.items():
            info = stats["outputs"][key]
            if hash_file(info["path"]) != info["sha256"]:
                raise ValueError("Export hash mismatch")
            cursor = db.execute(sql)
            count, logical = 0, hashlib.sha256()
            for batch in pq.ParquetFile(info["path"]).iter_batches(batch_size=2000):
                rows = batch.to_pylist()
                if rows != [dict(r) for r in cursor.fetchmany(len(rows))]:
                    raise ValueError("Export differs from checkpoint")
                for row in rows:
                    logical.update((json_text(row)+"\n").encode())
                count += len(rows)
            if cursor.fetchone() or count != info["rows"] or logical.hexdigest() != info["ordered_rows_sha256"]:
                raise ValueError("Export count/logical hash mismatch")
        counts = Counter(row[0] for row in db.execute("SELECT final_decision FROM pairs"))
        if {name:counts[name] for name in DECISIONS} != stats["decisions"] or len(pairs) != stats["candidate_pairs"] or len(expected_works) != stats["after_work_records"]:
            raise ValueError("Final statistics disagree with actual decisions")
        statuses = dict(db.execute("SELECT mapping_status,COUNT(*) FROM product_status GROUP BY mapping_status"))
        if sum(statuses.values()) != registration["products"] or statuses != stats["product_mapping_statuses"] or stats["products"] != registration["products"]:
            raise ValueError("Full product/status denominator mismatch")
        if stats["evaluation"]["manually_labeled_pairs"] != 0 or any(stats["evaluation"][key] is not None for key in ("precision", "recall", "f1")):
            raise ValueError("Generated unlabeled statistics cannot claim measured accuracy")
    return dict(verified_at=utc_now(),status="passed_integrity_not_human_accuracy",products=registration["products"],reviews_retained=review_count,
        all_raw_source_candidate_profiles_replayed=source_records,candidate_pairs_replayed=len(pairs),work_records=len(expected_works),
        all_within_group_pairs_checked=within_checks,cluster_cross_pair_checks=len(checks),transitive_edges_blocked=len(blocked),
        all_product_statuses_and_mappings_verified=True,all_exported_fields_match_checkpoint=True,
        evaluation_reserved_pairs=len(heldout),evaluation_development_overlap=0,independent_human_evaluation_completed=False,
        scope="Full deterministic raw assertion, decision, cluster, mapping and export verification; not manual gold labeling, final work-field completeness or target loading acceptance.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config",type=Path,default=ROOT/"config/task4.example.json")
    args=parser.parse_args()
    config=load_config(args.config)
    result=verify(config)
    save_json(Path(config["report_dir"])/"full_verification.json",result)
    print(json.dumps(result,indent=2))


if __name__ == "__main__":
    main()
