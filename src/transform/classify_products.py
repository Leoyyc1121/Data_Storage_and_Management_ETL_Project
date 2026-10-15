"""Task 3: evidence-based product classification, not movie entity resolution.

All ASINs remain present, including those without metadata. This offline stage
uses retained third-party records; it does not download or fabricate page facts.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
import hashlib
from html import unescape
import json
import logging
from pathlib import Path
import re
import sqlite3
import sys
import time
import unicodedata
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src/extract"))
from acquire_products import digest_json, load_config as load_task2, run_lock, save_json
from register_source import hash_file, utc_now

ENGINE_VERSION = "1.1.0"
LABELS = ("movie", "non_movie", "compilation", "television_series", "concert", "other_content", "awaiting_confirmation")
FIELDS = ("title", "original_title", "genres", "directors", "starring", "cast", "release_date", "release_year",
          "medium", "language", "subtitles", "region_code", "cut", "distributor", "product_release_date")
PRODUCT_SCHEMA = pa.schema([(name, pa.int64() if name in {"review_count", "candidate_count"} else pa.string()) for name in
    ("asin", "label", "reason", "confidence", "rule_version", "engine_version", "rule_hash", "input_fingerprint",
     "acquisition_status", "review_count", "first_review_id", "candidate_count", "evidence_ids", "source_review_sha256", "source_review_run_id", "decision_run_id")])
CANDIDATE_SCHEMA = pa.schema([(name, pa.int64() if name in {"record_sequence", "byte_start", "byte_end"} else pa.string()) for name in
    ("source_record_id", "asin", "source_id", "archive_sha256", "record_sequence", "byte_start", "byte_end", "raw_record_sha256",
     "source_downloaded_at", "label", "reason", "confidence", "rule_ids", "evidence", "fields", "rule_version", "decision_run_id")])


def text(value):
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", " ", unescape(re.sub(r"<[^>]+>", " ", unicodedata.normalize("NFKC", value)))).strip()


def json_text(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def load_config(path):
    config = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    for key in ("task2_config", "rules_file", "work_dir", "report_dir"):
        config[key] = str((ROOT / config[key]).resolve())
    if isinstance(config["batch_size"], bool) or not isinstance(config["batch_size"], int) or not 1 <= config["batch_size"] <= 10000:
        raise ValueError("batch_size must be an integer from 1 to 10000")
    source = load_task2(config["task2_config"])
    if Path(config["work_dir"]) in {ROOT, Path(source["work_dir"]), Path(source["source_run"])}:
        raise ValueError("Task 3 must not overwrite project/Task 1/Task 2")
    return config


def load_rules(path):
    rules = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if tuple(rules["labels"]) != LABELS or not rules.get("version"):
        raise ValueError("Invalid rules vocabulary/version")
    expected = {"dvd_or_bluray_title_is_movie_evidence": False, "generic_movies_tv_category_is_movie_evidence": False,
        "genre_alone_is_movie_evidence": False, "boxed_sets_alone_is_compilation_evidence": False,
        "all_candidates_must_agree": True, "conflicting_nonempty_titles_are_unresolved": True, "inner_asin_conflict_is_unresolved": True}
    if rules["policy"] != expected:
        raise ValueError("Policy changes require a new implementation version, not an ignored configuration switch")
    for pattern in rules["patterns"].values():
        re.compile(pattern, re.I)
    return rules


def source_record_id(row):
    return "meta:" + digest_json([row["source_id"], row["archive_sha256"], row["record_sequence"]])


def classify_record(item, rules):
    title = text(item.get("title"))
    categories = [(f"category[{i}]", text(value)) for i, value in enumerate(item.get("category", [])) if isinstance(value, str)]
    cat = {value for _, value in categories}
    raw_desc = item.get("description", [])
    descriptions = [("description" if isinstance(raw_desc, str) else f"description[{i}]", text(value))
                    for i, value in enumerate([raw_desc] if isinstance(raw_desc, str) else raw_desc or []) if isinstance(value, str)]
    details = item.get("details")
    if isinstance(details, dict):
        conflicts = [{"rule_id": "identity_conflict", "field_locator": f"details.{key}", "raw_value": value}
                     for key, value in details.items() if key.strip().rstrip(":").strip().lower() == "asin"
                     and (not isinstance(value, str) or value.strip() != item.get("asin"))]
        if conflicts:
            return {"label": "awaiting_confirmation", "reason": "inner_asin_conflict", "confidence": "unresolved", "evidence": conflicts}
    evidence = []

    def category_evidence(names):
        return [{"field_locator": loc, "raw_value": value} for loc, value in categories if value in names]

    def matches(name, locations):
        result = []
        for loc, value in locations:
            match = re.search(rules["patterns"][name], value, re.I)
            if match:
                result.append({"field_locator": loc, "matched_text": match.group(),
                               "excerpt": value[max(0, match.start()-80):match.end()+160]})
        return result

    def add(label, rule_id, facts):
        evidence.append({"label": label, "rule_id": rule_id, "facts": facts})

    title_loc = [("title", title)]
    movie_cat = category_evidence(set(rules["movie_categories"]))
    tv_cat = category_evidence(set(rules["television_categories"]))
    tv_title = matches("television_title", title_loc)
    tv_desc = matches("television_description", descriptions)
    program_title = matches("television_program_title", title_loc)
    boxes = category_evidence({"Boxed Sets"})
    music = category_evidence({"Music Videos & Concerts"})
    fitness = category_evidence({"Exercise & Fitness"})
    genres = category_evidence(set(rules["genre_mapping"]))
    if title and (tv_cat or tv_title and (tv_desc or boxes) or program_title and tv_desc):
        add("television_series", "tv_corroborated", tv_cat + tv_title + tv_desc + boxes + program_title)
    comp_title = matches("compilation_title", title_loc)
    comp_desc = matches("compilation_description", descriptions)
    if title and (comp_title and (boxes or movie_cat) or comp_desc and (boxes or movie_cat or genres)):
        add("compilation", "multiple_films_corroborated", comp_title + comp_desc + boxes + movie_cat)
    concert = matches("concert_title", title_loc) + matches("concert_description", descriptions)
    if title and music and concert:
        add("concert", "concert_corroborated", music + concert)
    workout = matches("workout", title_loc + descriptions)
    if title and fitness and workout:
        add("non_movie", "fitness_instruction_corroborated", fitness + workout)
    instruction = matches("instruction", title_loc + descriptions)
    instruction_cat = category_evidence({"Special Interests", "Musicals & Performing Arts", "Exercise & Fitness"})
    if title and instruction and instruction_cat:
        add("non_movie", "instruction_corroborated", instruction_cat + instruction)
    # A historical mention or bonus video does not identify the main content.
    music_video = matches("music_video_title", title_loc) + matches("music_video_description", descriptions)
    if title and music and music_video and not concert:
        add("other_content", "music_video_corroborated", music + music_video)
    special_labels = {row["label"] for row in evidence}
    if len(special_labels) > 1:
        return {"label": "awaiting_confirmation", "reason": "conflicting_content_types", "confidence": "unresolved", "evidence": evidence}
    if special_labels:
        return {"label": next(iter(special_labels)), "reason": "corroborated_content_evidence", "confidence": "rule_supported", "evidence": evidence}
    movie_desc = matches("movie_description", descriptions)
    # Specific movie categories are affirmative source assertions, not general Movies & TV.
    # Known contrary program/season/collection hints prevent generic movie acceptance.
    contrary = tv_title or tv_desc or program_title or comp_title or comp_desc or concert or workout or instruction or music_video
    if title and not contrary and (movie_cat or genres and movie_desc):
        add("movie", "explicit_movie_category" if movie_cat else "genre_and_movie_description", movie_cat or genres + movie_desc)
        return {"label": "movie", "reason": "positive_movie_evidence", "confidence": "rule_supported", "evidence": evidence}
    return {"label": "awaiting_confirmation", "reason": "insufficient_or_ambiguous_evidence", "confidence": "unresolved",
            "evidence": [{"rule_id": "insufficient_evidence", "title": title or None,
                          "category": [value for _, value in categories], "has_description": bool(descriptions)}]}


def extract_fields(item, rules):
    """Candidate fields only. Missing credits/dates are never inferred from brand or reviews."""
    fields = {key: {"raw": None, "normalized": None, "field_locators": [], "missing_reason": "not_explicit_in_source"} for key in FIELDS}

    def put(name, raw, normalized, locators):
        if normalized not in (None, "", []):
            fields[name] = {"raw": raw, "normalized": normalized, "field_locators": locators, "missing_reason": None}

    put("title", item.get("title"), text(item.get("title")), ["title"])
    categories = item.get("category", []) or []
    genre_pairs = [(i, value) for i, value in enumerate(categories) if isinstance(value, str) and text(value) in rules["genre_mapping"]]
    put("genres", [value for _, value in genre_pairs], sorted({genre for _, value in genre_pairs for genre in rules["genre_mapping"][text(value)]}), [f"category[{i}]" for i, _ in genre_pairs])
    details = item.get("details") or {}
    if isinstance(details, dict):
        for field, names in {"language": {"language"}, "subtitles": {"subtitles"}, "region_code": {"region", "region code"}, "distributor": {"studio", "distributor"}}.items():
            pairs = [(key, value) for key, value in details.items() if key.strip().rstrip(":").strip().lower() in names and text(value)]
            put(field, [value for _, value in pairs], [text(value) for _, value in pairs], [f"details.{key}" for key, _ in pairs])
    title = text(item.get("title"))
    medium = sorted({"blu_ray" if re.fullmatch(r"blu[- ]?ray", value, re.I) else value.lower()
                     for value in re.findall(r"\b(?:DVD|Blu[- ]?ray|VHS)\b", title, re.I)})
    put("medium", item.get("title") if medium else None, medium, ["title"] if medium else [])
    for name in ("directors", "starring", "cast"):
        fields[name]["missing_reason"] = "no_explicit_role_credit_in_supported_source_schema"
    for name in ("release_date", "release_year"):
        fields[name]["missing_reason"] = "no_verified_movie_release_semantics_or_work_identity"
    fields["product_release_date"]["missing_reason"] = "no_validated_product_release_date_semantics"
    return fields


def consensus(candidates):
    if not candidates:
        return "awaiting_confirmation", "no_available_product_metadata", "unresolved"
    if any(c["reason"] == "inner_asin_conflict" for c in candidates):
        return "awaiting_confirmation", "inner_asin_conflict", "unresolved"
    titles = {c["fields"]["title"]["normalized"].casefold() for c in candidates if c["fields"]["title"]["normalized"]}
    if len(titles) > 1:
        return "awaiting_confirmation", "conflicting_candidate_titles", "unresolved"
    labels = {c["label"] for c in candidates}
    if len(labels) != 1:
        return "awaiting_confirmation", "conflicting_or_incomplete_candidates", "unresolved"
    label = next(iter(labels))
    return label, "candidate_consensus" if label != "awaiting_confirmation" else "insufficient_or_ambiguous_evidence", "rule_supported" if label != "awaiting_confirmation" else "unresolved"


def input_registration(config, verify_input=True):
    task2 = load_task2(config["task2_config"])
    if verify_input:
        sys.path.insert(0, str(ROOT / "src/quality"))
        from verify_task2 import verify
        verify(task2)
    stats = json.loads((Path(task2["report_dir"]) / "full_statistics.json").read_text(encoding="utf-8"))
    metadata = json.loads((Path(task2["report_dir"]) / "metadata_statistics.json").read_text(encoding="utf-8"))
    if stats["statuses"]["success"]:
        raise ValueError("This adapter supports third-party JSON, not HTML extraction. Add and verify an HTML adapter before processing newly successful pages.")
    if not metadata["source_states"] or not all(state["complete"] for state in metadata["source_states"]):
        raise ValueError("Full-source metadata import must finish first")
    for entry in (stats["product_manifest"], metadata["output"]):
        if hash_file(entry["path"]) != entry["sha256"]:
            raise ValueError("Input Parquet changed")
    value = {"task2_manifest_sha256": stats["product_manifest"]["sha256"], "metadata_sha256": metadata["output"]["sha256"],
             "task2_source_fingerprint": stats["source_fingerprint"], "rule_file_sha256": hash_file(config["rules_file"]),
             "engine_version": ENGINE_VERSION, "engine_source_sha256": hash_file(Path(__file__)),
             "products": stats["distinct_asins"], "reviews": stats["source_records"]}
    return task2, value


def open_db(config, registration):
    Path(config["work_dir"]).mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(Path(config["work_dir"]) / "classification.sqlite3", uri=True)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY,value TEXT NOT NULL); CREATE TABLE IF NOT EXISTS runs(run_id TEXT PRIMARY KEY,started_at TEXT,finished_at TEXT,status TEXT,inserted_products INTEGER,inserted_candidates INTEGER);")
    for table, schema, pk, extra in (("products", PRODUCT_SCHEMA, "asin", ""), ("candidates", CANDIDATE_SCHEMA, "source_record_id", ",FOREIGN KEY(asin) REFERENCES products(asin)")):
        columns = ",".join(f'"{field.name}" {"INTEGER" if pa.types.is_integer(field.type) else "TEXT"} NOT NULL' for field in schema)
        db.execute(f"CREATE TABLE IF NOT EXISTS {table}({columns},PRIMARY KEY({pk}){extra},CHECK(label IN ({','.join(repr(v) for v in LABELS)})))")
    db.execute("CREATE INDEX IF NOT EXISTS candidate_asin ON candidates(asin)")
    existing = db.execute("SELECT value FROM state WHERE key='input'").fetchone()
    if existing and json.loads(existing[0]) != registration:
        db.close()
        raise ValueError("Source/rule version changed. Preserve old decisions and use a new work_dir/report_dir.")
    with db:
        db.execute("INSERT OR IGNORE INTO state VALUES ('input',?)", (json_text(registration),))
    task2 = load_task2(config["task2_config"])
    try:
        for name, filename in (("acquisition", "acquisition.sqlite3"), ("metadata", "metadata.sqlite3")):
            db.execute(f"ATTACH DATABASE ? AS {name}", ((Path(task2["work_dir"]) / filename).as_uri() + "?mode=ro",))
    except Exception:
        db.close()
        raise
    return db


def transform(db, config, rules, registration, run_id, stop_after_batches=None):
    fingerprint = digest_json(registration)
    state = db.execute("SELECT value FROM state WHERE key='last_asin'").fetchone()
    last = state[0] if state else ""
    inserted = candidates_inserted = batches = 0
    while True:
        rows = db.execute("SELECT * FROM acquisition.products WHERE asin>? ORDER BY asin LIMIT ?", (last, config["batch_size"])).fetchall()
        if not rows:
            with db:
                db.execute("INSERT OR REPLACE INTO state VALUES ('complete','true')")
            break
        end = rows[-1]["asin"]
        by_asin = defaultdict(list)
        for raw in db.execute("SELECT * FROM metadata.records WHERE asin>? AND asin<=? ORDER BY asin,source_id,record_sequence", (last, end)):
            item = json.loads(raw["raw_record"])
            decision = classify_record(item, rules)
            by_asin[raw["asin"]].append(dict(raw, source_record_id=source_record_id(raw), fields=extract_fields(item, rules), **decision))
        with db:
            for product in rows:
                candidates = by_asin[product["asin"]]
                label, reason, confidence = consensus(candidates)
                output = dict(asin=product["asin"], label=label, reason=reason, confidence=confidence, rule_version=rules["version"], engine_version=ENGINE_VERSION,
                    rule_hash=registration["rule_file_sha256"], input_fingerprint=fingerprint, acquisition_status=product["status"], review_count=product["review_count"],
                    first_review_id=product["first_review_id"], candidate_count=len(candidates), evidence_ids=json_text([c["source_record_id"] for c in candidates]),
                    source_review_sha256=product["source_sha256"], source_review_run_id=product["source_run_id"], decision_run_id=run_id)
                db.execute(f"INSERT INTO products VALUES ({','.join('?' for _ in PRODUCT_SCHEMA)})", tuple(output[key] for key in PRODUCT_SCHEMA.names))
                for candidate in candidates:
                    candidate.update(rule_ids=json_text(sorted({e["rule_id"] for e in candidate["evidence"]})), evidence=json_text(candidate["evidence"]), fields=json_text(candidate["fields"]), rule_version=rules["version"], decision_run_id=run_id)
                    db.execute(f"INSERT INTO candidates VALUES ({','.join('?' for _ in CANDIDATE_SCHEMA)})", tuple(candidate[key] for key in CANDIDATE_SCHEMA.names))
                candidates_inserted += len(candidates)
            db.execute("INSERT OR REPLACE INTO state VALUES ('last_asin',?)", (end,))
            inserted += len(rows)
            db.execute("UPDATE runs SET inserted_products=?,inserted_candidates=? WHERE run_id=?", (inserted, candidates_inserted, run_id))
        last = end
        batches += 1
        if batches % 10 == 0 or batches == 1:
            logging.info("committed products=%s last_asin=%s candidates_this_run=%s", db.execute("SELECT COUNT(*) FROM products").fetchone()[0], last, candidates_inserted)
        if stop_after_batches and batches >= stop_after_batches:
            raise KeyboardInterrupt
    return inserted, candidates_inserted


def export_table(db, table, schema, path, order):
    temporary = path.with_suffix(".parquet.tmp")
    cursor = db.execute(f"SELECT {','.join(schema.names)} FROM {table} ORDER BY {order}")
    rows = 0
    ordered_hash = hashlib.sha256()
    with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
        while batch := cursor.fetchmany(2000):
            records = [dict(row) for row in batch]
            for row in records:
                ordered_hash.update((json_text(row) + "\n").encode())
            writer.write_table(pa.Table.from_pylist(records, schema=schema))
            rows += len(records)
    temporary.replace(path)
    return {"path": str(path.resolve()), "rows": rows, "file_size_bytes": path.stat().st_size, "sha256": hash_file(path), "ordered_rows_sha256": ordered_hash.hexdigest()}


def field_coverage(db):
    counts = {name: Counter() for name in FIELDS}
    products = db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    movies = db.execute("SELECT COUNT(*) FROM products WHERE label='movie'").fetchone()[0]
    seen = set()
    for row in db.execute("SELECT c.asin,p.label,c.fields FROM candidates c JOIN products p ON c.asin=p.asin ORDER BY c.asin,c.source_record_id"):
        fields = json.loads(row["fields"])
        for name in FIELDS:
            key = row["asin"], name
            if fields[name]["normalized"] is not None and key not in seen:
                seen.add(key)
                counts[name]["all_products"] += 1
                if row["label"] == "movie":
                    counts[name]["movie_products"] += 1
    return {"scope": "Candidate availability per product, NOT selected/verified final movie-work field coverage. Multiple candidates counted at most once per ASIN/field; conflicts remain candidates.",
        "all_product_denominator": products, "identified_movie_product_denominator": movies,
        "fields": {name: {"available_all_products": counts[name]["all_products"], "missing_all_products": products-counts[name]["all_products"],
            "coverage_all_products": counts[name]["all_products"]/products if products else None,
            "available_movie_products": counts[name]["movie_products"], "missing_movie_products": movies-counts[name]["movie_products"],
            "coverage_movie_products": counts[name]["movie_products"]/movies if movies else None,
            "inferred_final_values": 0, "verified_final_work_values": 0} for name in FIELDS},
        "movie_work_coverage": None, "movie_work_coverage_missing_reason": "Task4 work identity and Task5/6/7 role/date/selection steps not implemented"}


def export(db, config, rules, registration):
    outputs = {"products": export_table(db, "products", PRODUCT_SCHEMA, Path(config["work_dir"]) / "product_classification.parquet", "asin"),
        "candidates": export_table(db, "candidates", CANDIDATE_SCHEMA, Path(config["work_dir"]) / "classification_evidence.parquet", "source_record_id")}
    total = outputs["products"]["rows"]
    counts = {label: 0 for label in LABELS}
    counts.update(dict(db.execute("SELECT label,COUNT(*) FROM products GROUP BY label")))
    task2 = load_task2(config["task2_config"])
    acquired = json.loads((Path(task2["report_dir"]) / "full_statistics.json").read_text(encoding="utf-8"))
    meta_stats = json.loads((Path(task2["report_dir"]) / "metadata_statistics.json").read_text(encoding="utf-8"))
    complete = bool(db.execute("SELECT 1 FROM state WHERE key='complete' AND value='true'").fetchone())
    examples = {}
    for label in LABELS:
        examples[label] = [dict(row) for row in db.execute("SELECT asin,label,reason,evidence_ids FROM products WHERE label=? ORDER BY asin LIMIT 3", (label,))]
    stats = {"observed_at": utc_now(), "status": "complete" if complete else "interrupted", "engine_version": ENGINE_VERSION, "rule_version": rules["version"],
        "input_registration": registration, "input_fingerprint": digest_json(registration), "products": total, "total_target_products": registration["products"],
        "classification_counts": counts, "identified_movie_products": counts["movie"], "identification_coverage_all_products": counts["movie"]/registration["products"],
        "unresolved_rate_all_products": counts["awaiting_confirmation"]/registration["products"], "review_references_retained": db.execute("SELECT COALESCE(SUM(review_count),0) FROM products").fetchone()[0],
        "review_policy": "All Task1 review records and Task2 ASIN references retained. No movie_id, review deletion, review copying or entity merging in Task3.",
        "reason_counts": dict(db.execute("SELECT reason,COUNT(*) FROM products GROUP BY reason")), "classification_candidate_records": outputs["candidates"]["rows"],
        "acquisition": {"denominator_all_products": acquired["distinct_asins"], "statuses": acquired["statuses"], "live_success": acquired["successful_live"],
            "authorized_html_snapshot_success": acquired["successful_authorized_snapshots"], "page_success_rate": acquired["success_rate_all_products"],
            "third_party_matched_products": meta_stats["matched_asins"], "third_party_coverage": meta_stats["coverage_all_target_products"], "third_party_is_page_success": False},
        "field_coverage": field_coverage(db), "outputs": outputs, "examples": examples,
        "examples_note": "Deterministic actual-source examples for inspection, not independently manually labeled evaluation data or accuracy estimates.",
        "limits": ["Rules conservative; no manually measured classification accuracy.", "Movie label is a source-supported product classification, not a resolved movie work.", "Unknown and conflicting records retained; no final credits/dates invented."],
        "checkpoint_path": str((Path(config["work_dir"]) / "classification.sqlite3").resolve())}
    save_json(Path(config["report_dir"]) / "full_statistics.json", stats)
    return stats


def run(config, stop_after_batches=None):
    Path(config["report_dir"]).mkdir(parents=True, exist_ok=True)
    rules = load_rules(config["rules_file"])
    logging.info("Stage input verification: full Task2 manifest and raw metadata evidence")
    _, registration = input_registration(config)
    run_id, started = uuid.uuid4().hex, time.monotonic()
    with run_lock(config["work_dir"]), closing(open_db(config, registration)) as db:
        with db:
            db.execute("UPDATE runs SET status='interrupted',finished_at=? WHERE status='running'", (utc_now(),))
            db.execute("INSERT INTO runs VALUES (?,?,NULL,'running',0,0)", (run_id, utc_now()))
        status = "complete"
        try:
            logging.info("Stage classification: evidence rules=%s; full scope=%s", rules["version"], registration["products"])
            transform(db, config, rules, registration, run_id, stop_after_batches)
            logging.info("Stage export: full classification and source candidates")
            stats = export(db, config, rules, registration)
            sys.path.insert(0, str(ROOT / "src/quality"))
            from verify_task3 import verify
            logging.info("Stage verification: every ASIN, candidate, raw locator and field replay")
            save_json(Path(config["report_dir"]) / "full_verification.json", verify(config))
        except KeyboardInterrupt:
            status = "interrupted"
            stats = export(db, config, rules, registration)
            logging.warning("Interrupted; resume with the same config and rule version")
        except Exception:
            status = "failed"
            logging.exception("Task3 failed; committed decisions preserved, no successful validation claimed")
            raise
        finally:
            with db:
                db.execute("UPDATE runs SET status=?,finished_at=? WHERE run_id=?", (status, utc_now(), run_id))
            row = dict(db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone())
            save_json(Path(config["report_dir"]) / "last_execution.json", dict(row, elapsed_seconds=time.monotonic()-started,
                updated_products=0, deleted_reviews=0, merged_entities=0, input_fingerprint=digest_json(registration)))
    return status, stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT / "config/task3.example.json")
    parser.add_argument("--stop-after-batches", type=int)
    args = parser.parse_args()
    if args.stop_after_batches is not None and args.stop_after_batches <= 0:
        parser.error("stop-after-batches must be positive")
    config = load_config(args.config)
    Path(config["report_dir"]).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True,
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(Path(config["report_dir"]) / "execution.log", encoding="utf-8")])
    status, stats = run(config, args.stop_after_batches)
    print(json.dumps({"status": status, "products": stats["products"], "labels": stats["classification_counts"]}, indent=2))
    return 130 if status == "interrupted" else 0


if __name__ == "__main__":
    sys.exit(main())
