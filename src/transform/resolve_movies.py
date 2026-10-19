"""Task4: offline, versioned, multi-field movie-work resolution.

All original products/review references are retained. No generated factual values,
person disambiguation, final release-date selection or automatic human labels.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
from difflib import SequenceMatcher
from html import unescape
import hashlib
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
sys.path.insert(0, str(ROOT / "src/transform"))
from acquire_products import digest_json, load_config as load_task2, run_lock, save_json
from classify_products import load_config as load_task3, json_text, source_record_id, text
from register_source import hash_file, utc_now

ENGINE_VERSION = "1.3.0"
DECISIONS = ("auto_merge", "manual_review", "reject", "unresolved")
NAMESPACE = uuid.UUID("b81f0945-de98-5236-8f72-b086c420e642")
STOP = {"a", "an", "the", "of", "and", "in", "to", "on", "for", "with", "from", "by"}
EDITION = r"(?:DVD|VHS|Blu[- ]?ray|4K|UHD|widescreen|full[- ]?screen|special edition|collector'?s edition|region\s*[0-9]|import|remastered|restored|director'?s cut|extended edition|unrated)"


def load_config(path):
    config = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    for key in ("task3_config", "rules_file", "work_dir", "report_dir"):
        config[key] = str((ROOT / config[key]).resolve())
    for key in ("batch_size", "evaluation_pairs"):
        if isinstance(config[key], bool) or not isinstance(config[key], int) or config[key] <= 0:
            raise ValueError(f"{key} must be positive integer")
    task3 = load_task3(config["task3_config"])
    task2 = load_task2(task3["task2_config"])
    if Path(config["work_dir"]) in {ROOT, Path(task3["work_dir"]), Path(task2["work_dir"]), Path(task2["source_run"])}:
        raise ValueError("Task4 output must not overwrite its inputs")
    return config


def load_rules(path):
    rules = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not rules.get("version") or not 0 < rules["candidate_title_similarity"] <= 1:
        raise ValueError("Invalid rule version/threshold")
    if rules["automatic_rules"] != ["title_director_and_two_credited_stars", "title_work_year_and_director", "title_work_year_and_two_credited_stars", "title_identical_long_plot_and_cast_or_director"]:
        raise ValueError("Unsupported automatic rule list")
    if rules["policies"] != {"title_only_auto_merge": False, "missing_year_is_match_evidence": False,
        "unverified_external_id_alone_auto_merge": False, "require_every_cross_cluster_pair_supported": True,
        "compilation_reviews": "product_only", "television": "separate_no_movie_merge",
        "pending_products": "attach_only_if_multifield_match_to_movie_seed", "evaluation_labels": "human_only_not_generated"}:
        raise ValueError("Unsupported policy change requires implementation and new version")
    for key in ("minimum_shared_plot_words", "minimum_shared_plot_unique_words", "nonexact_block_max_products", "runtime_tolerance_minutes"):
        if isinstance(rules[key], bool) or not isinstance(rules[key], int) or rules[key] <= 0:
            raise ValueError("Positive integer rule parameters required")
    return rules


def normalize_title(value):
    value = text(value)
    # Remove only explicitly identified edition brackets, never arbitrary subtitles.
    value = re.sub(r"[\[(]([^\])]+)[\])]", lambda m: " " if re.search(EDITION, m[1], re.I) else m[0], value)
    value = re.sub(r"\s*[\[(](?:18|19|20)\d{2}[\])]\s*$", "", value)
    value = re.sub(r"\b"+EDITION+r"\b", " ", value, flags=re.I)
    value = re.sub(r"\b[0-9]+[- ]discs?\b", " ", value, flags=re.I)
    value = unicodedata.normalize("NFKC", value).casefold()
    return " ".join(re.findall(r"\w+", value, re.UNICODE))


def title_forms(value):
    raw = text(value)
    parts = re.split(r"\s+(?:aka|a\.k\.a\.|also known as)\s+", raw, flags=re.I)
    forms = {normalize_title(raw)} | {normalize_title(part) for part in parts}
    # Accent-insensitive form is for candidate generation, not proof of identity.
    forms |= {"".join(c for c in unicodedata.normalize("NFKD", item) if not unicodedata.combining(c)) for item in forms}
    return sorted(forms - {""})


def name_prefix(value):
    value = re.split(r"[,;:()]", value, maxsplit=1)[0]
    tokens = re.findall(r"[^\W\d_][\w.'’\-]*", unescape(value), re.UNICODE)
    out = []
    for token in tokens[:6]:
        if token in {"and", "from", "as", "in", "with", "who", "of", "for", "is", "was", "by"}:
            break
        if token in {"de", "van", "von", "del"} and out:
            out.append(token)
        elif token[0].isupper() and token.upper() not in {"DVD", "VHS", "NTSC", "PAL", "USA", "THE", "THIS", "SYNOPSIS", "DIRECTOR", "STARRING"}:
            out.append(token.rstrip(".") if len(token)>2 else token)
            if token.endswith(".") and len(token)>2:
                break
        else:
            break
    if 2 <= len(out) <= 5:
        return " ".join(out).removesuffix("'s").removesuffix("’s").casefold()
    return None


def extract_record(row, rules):
    item = json.loads(row["raw_record"])
    sid = source_record_id(row)
    assertions = []
    values = defaultdict(set)
    title = text(item.get("title"))
    descriptions = item.get("description") or []
    if isinstance(descriptions, str):
        descriptions = [descriptions]
    plots, plot_tokens = [], set()
    edition = []
    details = item.get("details") or {}
    if isinstance(details, dict):
        for key, value in details.items():
            name = key.strip().rstrip(":").strip().casefold()
            if name in {"language", "subtitles", "region", "region code", "studio", "distributor"} and isinstance(value, str):
                tags = sorted({tag for alias, tag in rules["language_aliases"].items() if re.search(r"\b"+re.escape(alias)+r"\b", text(value), re.I)}) if name in {"language", "subtitles"} else None
                edition.append(dict(field="region_code" if name in {"region", "region code"} else "distributor" if name in {"studio", "distributor"} else name,
                    raw=value, normalized=text(value), language_tags=tags, field_locator=f"details.{key}", source_record_id=sid,
                    status="candidate_not_selected", missing_normalization_reason="no_supported_language_alias" if tags==[] else None))
    for match in re.finditer(r"\b(?:DVD|VHS|Blu[- ]?ray|UHD)\b", title, re.I):
        edition.append(dict(field="medium", raw=match.group(), normalized="blu_ray" if match.group().lower().startswith("blu") else match.group().lower(), field_locator="title", source_record_id=sid, status="candidate_not_selected"))

    def assert_value(field, value, loc, raw, semantics):
        if value is not None:
            values[field].add(value)
            assertions.append(dict(field=field, normalized=value, raw=raw, field_locator=loc, source_record_id=sid, semantics=semantics))

    for i, raw in enumerate(descriptions):
        if not isinstance(raw, str):
            continue
        loc = f"description[{i}]"
        clean = text(raw)
        for scope, pattern in (
                ("episodic_installment", r"\blimited edition episodic release\b|\bDVD Includes\s*:\s*.*?\bEpisode\s+[0-9]+\b"),
                ("complete_feature", r"\bDVD Includes\s*:\s*.*?\bcomplete feature film\b")):
            match = re.search(pattern, clean, re.I)
            if match:
                assert_value("primary_content_scopes", scope, loc, match.group(),
                    "explicit primary DVD content, not an incidental cast or title reference")
        tokens = re.findall(r"\w+", clean.casefold())
        unique = set(tokens)
        if len(tokens) >= rules["minimum_shared_plot_words"] and len(unique) >= rules["minimum_shared_plot_unique_words"] and re.search(r"\b(?:film|movie|story|stars|starring)\b", clean, re.I):
            plots.append(dict(sha256=hashlib.sha256(" ".join(tokens).encode()).hexdigest(), words=len(tokens), field_locator=loc, source_record_id=sid))
        # Bounded lexical features; original source bytes are never truncated.
        if i < 3:
            plot_tokens.update(token for token in tokens[:512] if token not in STOP and len(token) > 2)
        for match in re.finditer(r"\b(?:this|the) ((?:18|19|20)\d{2}) (?:[a-z-]+ ){0,2}(?:film|movie|remake)\b|^((?:18|19|20)\d{2}) (?:film|movie)\b", clean, re.I):
            assert_value("work_years", int(match[1] or match[2]), loc, match.group(), "source-asserted movie context year; not final release date")
        for match in re.finditer(r"\b(?:running time|runtime|run time)\s*:?\s*([0-9]{2,3})\s*(?:minutes|mins|min)\b|\b([0-9]{2,3})[- ]minute (?:film|movie)\b", clean, re.I):
            assert_value("runtimes", int(match[1] or match[2]), loc, match.group(), "source duration candidate; cuts may differ")
        lines = re.split(r"<(?:br\s*/?|/p|/div)>|\r?\n", raw, flags=re.I)
        continuing_role = None
        for j, line in enumerate(lines):
            line = text(line)
            if continuing_role and not re.match(r"^(?:starring|cast)\s*:?\s+", line, re.I):
                name = name_prefix(line)
                # Only a complete standalone name continues an explicit credit block.
                if name and not re.match(r"^(?:writer|producer|director|editor|music|photography|cinematography|synopsis|special features)\b", line, re.I) and line.casefold().strip(" .") == name.strip(" ."):
                    assert_value(continuing_role, name, loc+f"#line[{j}]", line, "continuation of explicit source role label; no first-N selection")
                elif line:
                    continuing_role = None
            for match in re.finditer(r"\b(?:directed by|director\s*:)\s*(.+)", line, re.I):
                assert_value("directors", name_prefix(match[1]), loc + f"#line[{j}]", line[:500], "explicit role spelling; person identity unresolved")
            if re.match(r"^(?:producer\s*,\s*)?director\s+", line, re.I):
                value = re.sub(r"^(?:producer\s*,\s*)?director\s+", "", line, flags=re.I)
                assert_value("directors", name_prefix(value), loc + f"#line[{j}]", line[:500], "explicit role label; person identity unresolved")
            if re.match(r"^(?:starring|cast)\s*:?\s+", line, re.I):
                value = re.sub(r"^(?:starring|cast)\s*:?\s+", "", line, flags=re.I)
                field = "credited_stars" if line.lower().startswith("starring") else "credited_cast"
                continuing_role = field
                for name in re.split(r",|\s+and\s+", value):
                    assert_value(field, name_prefix(name), loc + f"#line[{j}]", line[:500], "explicit source role label, not arbitrary first-N cast")
        for match in re.finditer(r"\bdirector\s+(.+)", clean, re.I):
            assert_value("director_mentions", name_prefix(match[1]), loc, match.group()[:200], "context mention only; may refer to another work")
        # Character(Name) evidence is identity context, NOT a final cast credit.
        for match in re.finditer(r"([A-Z][\w'’\-]*(?:\s+[A-Z][\w'’\-]*){0,3})\s*\(([^()]{3,80})\)", clean):
            inner = name_prefix(match[2])
            if inner and len(re.findall(r"\w+", match[2])) <= 5:
                assert_value("actor_context", inner, loc, match.group(), "character/name context; not final cast/starring assertion")
        for match in re.finditer(r"\b(?:stars|starring|featuring)\s+(.+)", clean, re.I):
            assert_value("credited_stars", name_prefix(match[1]), loc, match.group()[:200], "explicit stars/starring phrase; person identity unresolved")
        # External IDs are retained but not called verified or sufficient alone.
        for match in re.finditer(r"\btt[0-9]{7,10}\b", raw):
            assert_value("external_id_mentions", match.group(), loc, raw[max(0, match.start()-80):match.end()+80], "unverified contextual IMDb ID; may refer to another work")
    return dict(source_record_id=sid, source_id=row["source_id"], archive_sha256=row["archive_sha256"], record_sequence=row["record_sequence"],
        byte_start=row["byte_start"], byte_end=row["byte_end"], raw_record_sha256=row["raw_record_sha256"],
        title_raw=item.get("title"), title_normalized=title_forms(title), assertions=assertions,
        values={key: sorted(value) for key, value in values.items()}, edition_assertions=edition, long_plots=plots, plot_tokens=sorted(plot_tokens))


def make_profile(product, raw_rows, rules):
    records = [extract_record(row, rules) for row in raw_rows]
    values = defaultdict(set)
    for rec in records:
        for key, vals in rec["values"].items():
            values[key].update(vals)
    titles = sorted({title for rec in records for title in rec["title_normalized"]})
    raw_titles = sorted({rec["title_raw"] for rec in records if isinstance(rec["title_raw"], str)})
    variants = sorted({kind for kind, pattern in {"director_cut": r"director'?s cut", "extended": r"extended", "restored": r"restor|remaster", "unrated": r"unrated", "colorized": r"colou?ri[sz]ed"}.items() if any(re.search(pattern, title, re.I) for title in raw_titles)})
    hints = sorted({int(match[1]) for title in raw_titles for match in re.finditer(r"[\[(]((?:18|19|20)\d{2})[\])]", title)})
    blocked = product["reason"] in {"inner_asin_conflict", "conflicting_candidate_titles", "conflicting_or_incomplete_candidates"}
    blocked |= any(re.search(r"\bkaraoke\b", title, re.I) for title in raw_titles)
    return dict(asin=product["asin"], classification=product["label"], classification_reason=product["reason"],
        review_count=product["review_count"], first_review_id=product["first_review_id"], source_review_sha256=product["source_review_sha256"],
        source_review_run_id=product["source_review_run_id"], records=records, titles=titles, raw_titles=raw_titles, title_year_hints=hints,
        values={key: sorted(value) for key, value in values.items()}, variants=variants,
        eligible=bool(records and not blocked and product["label"] in {"movie", "awaiting_confirmation"}),
        work_type_block_reason="identity/title/content ambiguity" if blocked else None,
        long_plots=sorted({plot["sha256"] for rec in records for plot in rec["long_plots"]}),
        plot_tokens=sorted({token for rec in records for token in rec["plot_tokens"]}))


def pair_id(left, right):
    return digest_json(sorted((left, right)))


def title_score(a, b):
    return max((SequenceMatcher(None, x, y, autojunk=False).ratio() for x in a["titles"] for y in b["titles"]), default=0.0)


def content_scope_conflict(a, b):
    left, right = (set(p['values'].get('primary_content_scopes', [])) for p in (a, b))
    return bool('episodic_installment' in left and 'complete_feature' in right or
                'complete_feature' in left and 'episodic_installment' in right)


def compare(a, b, rules):
    va, vb = a["values"], b["values"]
    shared = lambda key: sorted(set(va.get(key, [])) & set(vb.get(key, [])))
    years_a, years_b = set(va.get("work_years", [])), set(vb.get("work_years", []))
    directors = shared("directors")
    stars = shared("credited_stars")
    cast_context = sorted((set(va.get("actor_context", [])) | set(va.get("credited_stars", []))) & (set(vb.get("actor_context", [])) | set(vb.get("credited_stars", []))))
    exact = bool(set(a["titles"]) & set(b["titles"]))
    score = title_score(a, b)
    plots = sorted(set(a["long_plots"]) & set(b["long_plots"]))
    tokens_a, tokens_b = set(a["plot_tokens"]), set(b["plot_tokens"])
    similarity = len(tokens_a & tokens_b)/len(tokens_a | tokens_b) if tokens_a | tokens_b else None
    facts = dict(title_exact=exact, title_similarity=score, shared_directors=directors, shared_credited_stars=stars,
        shared_actor_context=cast_context, shared_work_years=shared("work_years"), shared_long_plot_hashes=plots,
        lexical_description_jaccard=similarity, left_work_years=sorted(years_a), right_work_years=sorted(years_b),
        shared_unverified_external_id_mentions=shared("external_id_mentions"), left_variants=a["variants"], right_variants=b["variants"],
        left_source_records=[r["source_record_id"] for r in a["records"]], right_source_records=[r["source_record_id"] for r in b["records"]])
    facts.update(left_primary_content_scopes=va.get('primary_content_scopes', []),
        right_primary_content_scopes=vb.get('primary_content_scopes', []))
    def result(decision, reason, strength):
        return dict(proposed_decision=decision, final_decision=decision, rule_id=reason, evidence_strength=strength, facts=facts,
            confidence_semantics="rule evidence strength, not measured probability/accuracy")
    if not a["eligible"] or not b["eligible"]:
        return result("reject", "work_type_or_identity_not_eligible", "excluded")
    if content_scope_conflict(a, b):
        return result("reject", "episodic_installment_not_complete_feature", "explicit_primary_content_conflict")
    # Multiple conflicting years in a single product are never selected silently.
    if len(years_a) > 1 or len(years_b) > 1:
        return result("manual_review", "within_product_year_conflict", "ambiguous")
    year_conflict = bool(years_a and years_b and min(abs(x-y) for x in years_a for y in years_b) > 1)
    da, db = set(va.get("directors", [])), set(vb.get("directors", []))
    mention_a = da | set(va.get("director_mentions", []))
    mention_b = db | set(vb.get("director_mentions", []))
    director_conflict = bool(mention_a and mention_b and not mention_a & mention_b)
    if exact and year_conflict and director_conflict:
        return result("reject", "different_work_year_and_director_evidence", "strong_conflict")
    if year_conflict or da and db and not da & db:
        return result("manual_review", "year_or_director_conflict", "conflicting")
    # Preserve sequel numerals; normalizing edition words must not erase them.
    def sequel(title):
        tokens = title.split()
        roman = {"ii":"2", "iii":"3", "iv":"4", "v":"5", "vi":"6", "vii":"7", "viii":"8", "ix":"9", "x":"10"}
        markers = tuple(token for token in tokens if token in roman or token.isdigit() and len(token)<4)
        nums = tuple(roman.get(token,token) for token in markers)
        return " ".join(token for token in tokens if token not in markers), nums
    if any(sequel(x)[0] == sequel(y)[0] and sequel(x)[1] and sequel(y)[1] and sequel(x)[1] != sequel(y)[1] for x in a["titles"] for y in b["titles"]):
        return result("reject", "different_sequel_numerals", "structural_conflict")
    if any(sequel(x)[0] == sequel(y)[0] and bool(sequel(x)[1]) != bool(sequel(y)[1]) for x in a["titles"] for y in b["titles"]):
        return result("manual_review", "sequence_number_omitted_requires_review", "ambiguous_numbering")
    if a["variants"] != b["variants"]:
        return result("manual_review", "cut_restoration_or_version_requires_review", "edition_exception")
    ra, rb = va.get("runtimes", []), vb.get("runtimes", [])
    if len(ra) > 1 or len(rb) > 1 or ra and rb and min(abs(x-y) for x in ra for y in rb) > rules["runtime_tolerance_minutes"]:
        return result("manual_review", "runtime_or_cut_difference_requires_review", "edition_exception")
    if exact:
        if directors and len(stars) >= 2:
            return result("auto_merge", "title_director_and_two_credited_stars", "strong_multifield")
        if shared("work_years") and directors:
            return result("auto_merge", "title_work_year_and_director", "strong_multifield")
        if shared("work_years") and len(stars) >= 2:
            return result("auto_merge", "title_work_year_and_two_credited_stars", "strong_multifield")
        if plots and (directors or len(cast_context) >= 2):
            return result("auto_merge", "title_identical_long_plot_and_cast_or_director", "strong_multifield")
        return result("manual_review", "same_title_missing_independent_work_evidence", "insufficient")
    return result("manual_review" if score >= rules["candidate_title_similarity"] else "unresolved", "similar_title_not_identity_proof", "weak")


def generate_candidates(profiles, rules):
    aliases, prefixes = defaultdict(set), defaultdict(set)
    for asin, p in profiles.items():
        for title in p["titles"]:
            aliases[title].add(asin)
            words = [word for word in title.split() if word not in STOP]
            if words:
                prefixes[words[0][:4]].add(asin)
    pairs, skipped = {}, set()
    for asin, p in sorted(profiles.items()):
        if p["classification"] != "movie":
            continue
        possible = set()
        for title in p["titles"]:
            possible.update(aliases[title])
            words = [word for word in title.split() if word not in STOP]
            if words:
                block = prefixes[words[0][:4]]
                if len(block) <= rules["nonexact_block_max_products"]:
                    possible.update(block)
                else:
                    skipped.add(words[0][:4])
        for other in sorted(possible - {asin}):
            q = profiles[other]
            if set(p["titles"]) & set(q["titles"]) or title_score(p, q) >= rules["candidate_title_similarity"]:
                left, right = sorted((asin, other))
                pairs[pair_id(left, right)] = (left, right)
    return sorted((key, *value) for key, value in pairs.items()), sorted(skipped)


def complete_link_clusters(profiles, pairs, rules):
    """Deterministic greedy complete linkage: an absent edge is re-evaluated, not assumed."""
    groups = {asin: {asin} for asin, p in profiles.items() if p["classification"] == "movie"}
    owner = {asin: asin for asin in groups}
    checks, events, blocked = [], [], {}
    for pid, left, right, decision in pairs:
        if decision["proposed_decision"] != "auto_merge":
            continue
        for asin in (left, right):
            if asin not in owner:
                groups[asin], owner[asin] = {asin}, asin
        x, y = owner[left], owner[right]
        if x == y:
            continue
        cross = []
        for a in sorted(groups[x]):
            for b in sorted(groups[y]):
                verdict = compare(profiles[a], profiles[b], rules)
                checks.append((pid, *sorted((a, b)), verdict))
                cross.append(verdict["proposed_decision"] == "auto_merge")
        if not all(cross):
            blocked[pid] = "transitive_cluster_cross_pair_not_supported"
            continue
        new, old = sorted((x, y))
        members = groups[new] | groups[old]
        events.append(dict(merge_pair_id=pid, previous_groups=[sorted(groups[new]), sorted(groups[old])], resulting_members=sorted(members)))
        groups[new] = members
        del groups[old]
        for asin in members:
            owner[asin] = new
    return [sorted(group) for _, group in sorted(groups.items()) if any(profiles[a]["classification"] == "movie" for a in group)], checks, events, blocked


def input_registration(config, verify_input=True):
    task3 = load_task3(config["task3_config"])
    if verify_input:
        sys.path.insert(0, str(ROOT / "src/quality"))
        from verify_task3 import verify
        verify(task3)
    stats = json.loads((Path(task3["report_dir"]) / "full_statistics.json").read_text(encoding="utf-8"))
    if stats["status"] != "complete":
        raise ValueError("Full Task3 input required")
    for info in stats["outputs"].values():
        if hash_file(info["path"]) != info["sha256"]:
            raise ValueError("Task3 input artifact changed")
    registration = dict(task3_input_fingerprint=stats["input_fingerprint"], task3_outputs={key: value["sha256"] for key, value in stats["outputs"].items()},
        products=stats["products"], reviews=stats["review_references_retained"], task3_movie_products=stats["identified_movie_products"],
        rules_sha256=hash_file(config["rules_file"]), engine_version=ENGINE_VERSION, engine_sha256=hash_file(__file__), evaluation_pairs=config["evaluation_pairs"],
        development_pair_ids=sorted({pair_id(*pair) for pair in config.get("development_pairs", [])}))
    return task3, registration


def open_db(config, registration):
    Path(config["work_dir"]).mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(Path(config["work_dir"]) / "resolution.sqlite3", uri=True)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
      CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY,value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS runs(run_id TEXT PRIMARY KEY,started_at TEXT,finished_at TEXT,status TEXT,phase TEXT);
      CREATE TABLE IF NOT EXISTS products(asin TEXT PRIMARY KEY,classification TEXT NOT NULL,review_count INTEGER NOT NULL CHECK(review_count>0),metadata_records INTEGER NOT NULL,profile TEXT NOT NULL,profile_hash TEXT NOT NULL,run_id TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS pairs(pair_id TEXT PRIMARY KEY,left_asin TEXT NOT NULL REFERENCES products(asin),right_asin TEXT NOT NULL REFERENCES products(asin),proposed_decision TEXT NOT NULL,final_decision TEXT NOT NULL,rule_id TEXT NOT NULL,evidence TEXT NOT NULL,rule_version TEXT NOT NULL,run_id TEXT NOT NULL,CHECK(left_asin<right_asin));
      CREATE TABLE IF NOT EXISTS works(movie_id TEXT PRIMARY KEY,anchor_source_record_id TEXT NOT NULL UNIQUE,status TEXT NOT NULL,display_title TEXT,display_title_source TEXT,rule_version TEXT NOT NULL,run_id TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS product_status(asin TEXT PRIMARY KEY REFERENCES products(asin),mapping_status TEXT NOT NULL,reason TEXT NOT NULL,run_id TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS product_movie(asin TEXT NOT NULL REFERENCES products(asin),movie_id TEXT NOT NULL REFERENCES works(movie_id),relationship TEXT NOT NULL CHECK(relationship IN ('single_work','contains')),evidence TEXT NOT NULL,run_id TEXT NOT NULL,PRIMARY KEY(asin,movie_id));
      CREATE TABLE IF NOT EXISTS cluster_checks(merge_pair_id TEXT NOT NULL,left_asin TEXT NOT NULL,right_asin TEXT NOT NULL,decision TEXT NOT NULL,evidence TEXT NOT NULL,PRIMARY KEY(merge_pair_id,left_asin,right_asin));
      CREATE TABLE IF NOT EXISTS mapping_history(event_id TEXT PRIMARY KEY,event TEXT NOT NULL,rule_version TEXT NOT NULL,run_id TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS evaluation_pairs(pair_id TEXT PRIMARY KEY REFERENCES pairs(pair_id),stratum TEXT NOT NULL,partition TEXT NOT NULL CHECK(partition='independent_human_evaluation'),sampling_version TEXT NOT NULL);
      CREATE INDEX IF NOT EXISTS work_product ON product_movie(movie_id);
    """)
    existing = db.execute("SELECT value FROM state WHERE key='input'").fetchone()
    if existing and json.loads(existing[0]) != registration:
        db.close()
        raise ValueError("Input/rules/engine changed; preserve decisions and use new work_dir/report_dir")
    with db:
        db.execute("INSERT OR IGNORE INTO state VALUES ('input',?)", (json_text(registration),))
    try:
        task3 = load_task3(config["task3_config"])
        task2 = load_task2(task3["task2_config"])
        db.execute("ATTACH DATABASE ? AS task3", ((Path(task3["work_dir"]) / "classification.sqlite3").as_uri()+"?mode=ro",))
        db.execute("ATTACH DATABASE ? AS metadata", ((Path(task2["work_dir"]) / "metadata.sqlite3").as_uri()+"?mode=ro",))
    except Exception:
        db.close()
        raise
    return db


def state(db, key, default=None):
    row = db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else default


def set_state(db, key, value):
    db.execute("INSERT OR REPLACE INTO state VALUES (?,?)", (key, json_text(value)))


def prepare(db, config, rules, run_id, stop_after_batches=None):
    if state(db, "profiles_complete"):
        return
    last, batches = state(db, "last_asin", ""), 0
    while True:
        products = db.execute("SELECT * FROM task3.products WHERE asin>? ORDER BY asin LIMIT ?", (last, config["batch_size"])).fetchall()
        if not products:
            with db:
                set_state(db, "profiles_complete", True)
            break
        end = products[-1]["asin"]
        records = defaultdict(list)
        for row in db.execute("SELECT * FROM metadata.records WHERE asin>? AND asin<=? ORDER BY asin,source_id,record_sequence", (last, end)):
            records[row["asin"]].append(dict(row))
        with db:
            for product in products:
                profile = make_profile(product, records[product["asin"]], rules)
                db.execute("INSERT INTO products VALUES (?,?,?,?,?,?,?)", (product["asin"], product["label"], product["review_count"], len(records[product["asin"]]), json_text(profile), digest_json(profile), run_id))
            set_state(db, "last_asin", end)
        last, batches = end, batches+1
        if batches == 1 or batches % 10 == 0:
            logging.info("Profiles committed=%s last_asin=%s", db.execute("SELECT COUNT(*) FROM products").fetchone()[0], end)
        if stop_after_batches and batches >= stop_after_batches:
            raise KeyboardInterrupt


def read_profiles(db):
    return {row["asin"]: json.loads(row["profile"]) for row in db.execute("SELECT asin,profile FROM products WHERE metadata_records>0 ORDER BY asin")}


def build_pairs(db, config, rules, run_id, profiles, stop_after_batches=None):
    if state(db, "pairs_complete"):
        return
    proposals, skipped = generate_candidates(profiles, rules)
    last, batches = state(db, "last_pair_id", ""), 0
    proposals = [row for row in proposals if row[0] > last]
    for offset in range(0, len(proposals), config["batch_size"]):
        batch = proposals[offset:offset+config["batch_size"]]
        with db:
            for pid, left, right in batch:
                decision = compare(profiles[left], profiles[right], rules)
                db.execute("INSERT INTO pairs VALUES (?,?,?,?,?,?,?,?,?)", (pid, left, right, decision["proposed_decision"], decision["final_decision"], decision["rule_id"], json_text(decision), rules["version"], run_id))
            set_state(db, "last_pair_id", batch[-1][0])
        batches += 1
        if stop_after_batches and batches >= stop_after_batches:
            raise KeyboardInterrupt
    with db:
        set_state(db, "nonexact_prefix_blocks_skipped", skipped)
        set_state(db, "pairs_complete", True)
    logging.info("Candidate pairs=%s; skipped high-frequency nonexact blocks=%s", db.execute("SELECT COUNT(*) FROM pairs").fetchone()[0], len(skipped))


def materialize_clusters(db, rules, run_id, profiles):
    if state(db, "clusters_complete"):
        return
    pairs = [(row["pair_id"], row["left_asin"], row["right_asin"], json.loads(row["evidence"])) for row in db.execute("SELECT * FROM pairs ORDER BY pair_id")]
    groups, checks, events, blocked = complete_link_clusters(profiles, pairs, rules)
    with db:
        for pid, a, b, decision in checks:
            db.execute("INSERT INTO cluster_checks VALUES (?,?,?,?,?)", (pid, a, b, decision["proposed_decision"], json_text(decision)))
        for pid, reason in blocked.items():
            db.execute("UPDATE pairs SET final_decision='manual_review',rule_id=? WHERE pair_id=?", (reason, pid))
        for event in events:
            db.execute("INSERT INTO mapping_history VALUES (?,?,?,?)", (digest_json(event), json_text(event), rules["version"], run_id))
        for group in groups:
            seed_records = sorted((rec["source_record_id"], asin, rec) for asin in group if profiles[asin]["classification"] == "movie" for rec in profiles[asin]["records"])
            anchor, _, record = seed_records[0]
            mid = "mw_" + uuid.uuid5(NAMESPACE, "creative-work-seed:"+anchor).hex
            db.execute("INSERT INTO works VALUES (?,?,?,?,?,?,?)", (mid, anchor, "multifield_group" if len(group)>1 else "provisional_singleton", record["title_raw"], anchor, rules["version"], run_id))
            for asin in group:
                evidence = dict(source_record_ids=[rec["source_record_id"] for rec in profiles[asin]["records"]], group_members=group,
                    work_identity_status="multifield_supported" if len(group)>1 else "provisional_not_proven_distinct_from_all_other_works")
                db.execute("INSERT INTO product_movie VALUES (?,?,'single_work',?,?)", (asin, mid, json_text(evidence), run_id))
        for row in db.execute("SELECT asin,classification FROM products ORDER BY asin"):
            asin, cls = row["asin"], row["classification"]
            relation = db.execute("SELECT w.status FROM product_movie pm JOIN works w USING(movie_id) WHERE pm.asin=?", (asin,)).fetchone()
            status = "merged" if relation and relation[0] == "multifield_group" else "provisional_work" if relation else "compilation_unresolved" if cls == "compilation" else "pending_classification" if cls == "awaiting_confirmation" else "excluded_content"
            reason = "multifield_match_movie_seed_original_classification_preserved" if status == "merged" else "singleton_not_verified_work_identity" if status == "provisional_work" else "compilation_members_require_evidence_reviews_product_only" if cls == "compilation" else "no_supported_single_movie_mapping"
            db.execute("INSERT INTO product_status VALUES (?,?,?,?)", (asin, status, reason, run_id))
        set_state(db, "clusters_complete", True)
    logging.info("Works=%s; complete-link checks=%s; blocked transitive edges=%s", len(groups), len(checks), len(blocked))


def evaluation_sample(db, config, rules, profiles):
    if state(db, "evaluation_complete"):
        return
    buckets = defaultdict(list)
    development = {pair_id(*pair) for pair in config.get("development_pairs", [])}
    for row in db.execute("SELECT * FROM pairs ORDER BY pair_id"):
        if row["pair_id"] in development:
            continue
        a, b = profiles[row["left_asin"]], profiles[row["right_asin"]]
        facts = json.loads(row["evidence"])["facts"]
        stratum = "mixed_content_or_compilation" if a["classification"] not in {"movie", "awaiting_confirmation"} or b["classification"] not in {"movie", "awaiting_confirmation"} else "cut_or_version" if a["variants"] != b["variants"] else "same_title" if facts["title_exact"] else "similar_title"
        key = digest_json([rules["evaluation_sampling_salt"], row["pair_id"]])
        buckets[stratum].append((key, row["pair_id"]))
    for bucket in buckets.values():
        bucket.sort(reverse=True)
    chosen = []
    while len(chosen) < config["evaluation_pairs"]:
        active = False
        for name in sorted(buckets):
            if buckets[name] and len(chosen) < config["evaluation_pairs"]:
                chosen.append((buckets[name].pop()[1], name))
                active = True
        if not active:
            break
    with db:
        db.executemany("INSERT INTO evaluation_pairs VALUES (?,?,'independent_human_evaluation',?)", [(pid, name, rules["evaluation_sampling_salt"]) for pid, name in chosen])
        set_state(db, "evaluation_complete", True)


def export_query(db, sql, path):
    cursor = db.execute(sql)
    names = [col[0] for col in cursor.description]
    # Explicit integer columns, other payloads are JSON queryable in checkpoint.
    integer = {"review_count", "metadata_records"}
    schema = pa.schema([(name, pa.int64() if name in integer else pa.string()) for name in names])
    temp = path.with_suffix(path.suffix+".tmp")
    count, logical = 0, hashlib.sha256()
    with pq.ParquetWriter(temp, schema, compression="zstd") as writer:
        while batch := cursor.fetchmany(2000):
            rows = [dict(row) for row in batch]
            for row in rows:
                logical.update((json_text(row)+"\n").encode())
            writer.write_table(pa.Table.from_pylist(rows, schema=schema))
            count += len(rows)
    temp.replace(path)
    return dict(path=str(path.resolve()), rows=count, file_size_bytes=path.stat().st_size, sha256=hash_file(path), ordered_rows_sha256=logical.hexdigest())


EXPORTS = {
    "products": ("product_profiles.parquet", "SELECT * FROM products ORDER BY asin"),
    "works": ("movie_works.parquet", "SELECT * FROM works ORDER BY movie_id"),
    "mappings": ("product_movie.parquet", "SELECT * FROM product_movie ORDER BY asin,movie_id"),
    "product_status": ("product_mapping_status.parquet", "SELECT p.asin,p.classification,p.review_count,s.mapping_status,s.reason,s.run_id FROM products p JOIN product_status s USING(asin) ORDER BY p.asin"),
    "decisions": ("merge_decisions.parquet", "SELECT * FROM pairs ORDER BY pair_id"),
    "history": ("mapping_history.parquet", "SELECT * FROM mapping_history ORDER BY event_id")}


def export(db, config, rules, registration):
    outputs = {key: export_query(db, sql, Path(config["work_dir"])/file) for key, (file, sql) in EXPORTS.items()}
    queue = []
    for row in db.execute("SELECT e.*,p.left_asin,p.right_asin FROM evaluation_pairs e JOIN pairs p USING(pair_id) ORDER BY e.pair_id"):
        left = json.loads(db.execute("SELECT profile FROM products WHERE asin=?", (row["left_asin"],)).fetchone()[0])
        right = json.loads(db.execute("SELECT profile FROM products WHERE asin=?", (row["right_asin"],)).fetchone()[0])
        def packet(p):
            return {key: p[key] for key in ("asin", "classification", "raw_titles", "values", "variants", "records")}
        queue.append(dict(pair_id=row["pair_id"], partition=row["partition"], sampling_stratum=row["stratum"], left=packet(left), right=packet(right),
            label=None, labeled_by=None, labeler_type=None, labeled_at=None, labeling_evidence=[], rationale=None,
            instructions="Human: label same_work or different_work with raw evidence and reasoning. Do not copy model predictions; unresolved may be deferred. Never use this held-out queue for tuning."))
    save_json(Path(config["work_dir"])/"evaluation_queue.json", {"sampling_version": rules["evaluation_sampling_salt"], "pairs": queue})
    eval_hash = hash_file(Path(config["work_dir"])/"evaluation_queue.json")
    counts = dict(db.execute("SELECT final_decision,COUNT(*) FROM pairs GROUP BY final_decision"))
    before = registration["task3_movie_products"]
    works = outputs["works"]["rows"]
    grouped = db.execute("SELECT COUNT(*) FROM works WHERE status='multifield_group'").fetchone()[0]
    attached_pending = db.execute("SELECT COUNT(DISTINCT pm.asin) FROM product_movie pm JOIN products p USING(asin) WHERE p.classification='awaiting_confirmation'").fetchone()[0]
    heldout = {row[0] for row in db.execute("SELECT pair_id FROM evaluation_pairs")}
    samples = []
    for row in db.execute("SELECT movie_id FROM works WHERE status='multifield_group' ORDER BY movie_id"):
        members = [dict(p) for p in db.execute("SELECT pm.asin,p.classification FROM product_movie pm JOIN products p USING(asin) WHERE pm.movie_id=? ORDER BY pm.asin", (row["movie_id"],))]
        if any(pair_id(a["asin"], b["asin"]) in heldout for a in members for b in members if a["asin"] < b["asin"]):
            continue
        samples.append(dict(movie_id=row["movie_id"], members=members))
        if len(samples) == 5:
            break
    negative = [dict(row) for row in db.execute("SELECT p.pair_id,p.left_asin,p.right_asin,p.rule_id FROM pairs p LEFT JOIN evaluation_pairs e USING(pair_id) WHERE p.final_decision='reject' AND e.pair_id IS NULL ORDER BY p.pair_id LIMIT 5")]
    evaluation = dict(status="awaiting_independent_human_labels", queue_pairs=len(queue), required_minimum_pairs=100, manually_labeled_pairs=0,
        precision=None, recall=None, f1=None, manual_review_coverage=0.0, queue_path=str((Path(config["work_dir"])/"evaluation_queue.json").resolve()), queue_sha256=eval_hash,
        note="User confirmed no manual labels. This queue is NOT a completed labeled evaluation set. Development cases exclude these pair IDs.")
    save_json(Path(config["report_dir"])/"evaluation_status.json", evaluation)
    stats = dict(observed_at=utc_now(), status="complete_pipeline_pending_human_acceptance", engine_version=ENGINE_VERSION, rule_version=rules["version"],
        input_registration=registration, input_fingerprint=digest_json(registration), products=outputs["products"]["rows"],
        reviews_retained=db.execute("SELECT COALESCE(SUM(review_count),0) FROM products").fetchone()[0], before_provisional_movie_seeds=before,
        after_work_records=works, merged_seed_reduction=before-works, multifield_work_groups=grouped, provisional_singleton_works=works-grouped,
        additional_pending_products_attached_by_multifield_evidence=attached_pending, candidate_pairs=outputs["decisions"]["rows"],
        decisions={name: counts.get(name, 0) for name in DECISIONS}, product_mapping_statuses=dict(db.execute("SELECT mapping_status,COUNT(*) FROM product_status GROUP BY mapping_status")),
        cluster_cross_pair_checks=db.execute("SELECT COUNT(*) FROM cluster_checks").fetchone()[0], transitive_edges_blocked=db.execute("SELECT COUNT(*) FROM pairs WHERE rule_id='transitive_cluster_cross_pair_not_supported'").fetchone()[0],
        nonexact_high_frequency_prefixes_skipped=state(db, "nonexact_prefix_blocks_skipped", []),
        candidate_scope="All target ASINs retained; candidate matching requires a Task3 movie seed and available source metadata. Exact-title blocks are not capped; nonexact frequent-prefix blocks are skipped and reported. Not exhaustive all-pairs matching.",
        review_policy="All reviews stay on original products. Compilation reviews product-only, no copying; television excluded from movie merges. No deduplication/aggregate movie rating in Task4.",
        movie_id_semantics="System UUIDv5 anchored to stable source record identity, never ASIN as movie_id. Identical version/input reruns stable; merge history and old version directories preserve remapping/splits.",
        field_semantics="Raw/normalized assertions used as identity evidence only. Credited names not person entities; no chosen final release dates/canonical titles or field conflict arbitration. display_title is explicitly sourced provisional display label.",
        outputs=outputs, evaluation=evaluation, case_examples=dict(merged_groups=samples, kept_separate_pairs=negative),
        case_limitations="Actual-source examples, not human gold labels. At least5 groups of each type still require source inspection; no fabricated pairs.",
        checkpoint_path=str((Path(config["work_dir"])/"resolution.sqlite3").resolve()))
    save_json(Path(config["report_dir"])/"full_statistics.json", stats)
    return stats


def run(config, stop_after_batches=None, stop_phase="profiles"):
    rules = load_rules(config["rules_file"])
    Path(config["report_dir"]).mkdir(parents=True, exist_ok=True)
    logging.info("Verify full Task3 input and provenance")
    _, registration = input_registration(config)
    started, run_id = time.monotonic(), uuid.uuid4().hex
    with run_lock(config["work_dir"]), closing(open_db(config, registration)) as db:
        with db:
            db.execute("UPDATE runs SET status='interrupted',finished_at=? WHERE status='running'", (utc_now(),))
            db.execute("INSERT INTO runs VALUES (?,?,NULL,'running','profiles')", (run_id, utc_now()))
        status = "complete"
        try:
            prepare(db, config, rules, run_id, stop_after_batches if stop_phase == "profiles" else None)
            profiles = read_profiles(db)
            logging.info("Generate source-supported candidate pairs; movie seeds=%s metadata_products=%s", registration["task3_movie_products"], len(profiles))
            with db:
                db.execute("UPDATE runs SET phase='pairs' WHERE run_id=?", (run_id,))
            build_pairs(db, config, rules, run_id, profiles, stop_after_batches if stop_phase == "pairs" else None)
            materialize_clusters(db, rules, run_id, profiles)
            evaluation_sample(db, config, rules, profiles)
            logging.info("Export decisions, work/product mappings and unlabeled human evaluation queue")
            stats = export(db, config, rules, registration)
            sys.path.insert(0, str(ROOT / "src/quality"))
            from verify_task4 import verify
            save_json(Path(config["report_dir"])/"full_verification.json", verify(config))
        except KeyboardInterrupt:
            status, stats = "interrupted", None
            logging.warning("Interrupted; committed phase batches preserved. No complete report exported.")
        except Exception:
            status = "failed"
            logging.exception("Task4 failed; preserve committed evidence, do not claim successful acceptance")
            raise
        finally:
            with db:
                db.execute("UPDATE runs SET status=?,finished_at=? WHERE run_id=?", (status, utc_now(), run_id))
            save_json(Path(config["report_dir"])/"last_execution.json", dict(db.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()) | dict(elapsed_seconds=time.monotonic()-started,
                products=db.execute("SELECT COUNT(*) FROM products").fetchone()[0], pairs=db.execute("SELECT COUNT(*) FROM pairs").fetchone()[0], deleted_reviews=0, input_fingerprint=digest_json(registration)))
    return status, stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT/"config/task4.example.json")
    parser.add_argument("--stop-after-batches", type=int)
    parser.add_argument("--stop-phase", choices=("profiles", "pairs"), default="profiles")
    args = parser.parse_args()
    if args.stop_after_batches is not None and args.stop_after_batches <= 0:
        parser.error("Positive stop limit required")
    config = load_config(args.config)
    Path(config["report_dir"]).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True,
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(Path(config["report_dir"])/"execution.log", encoding="utf-8")])
    status, stats = run(config, args.stop_after_batches, args.stop_phase)
    print(json.dumps(dict(status=status, statistics=None if stats is None else {key: stats[key] for key in ("products", "candidate_pairs", "after_work_records", "multifield_work_groups", "decisions")}), indent=2))
    return 130 if status == "interrupted" else 0


if __name__ == "__main__":
    sys.exit(main())
