"""Evaluate automatic final work grouping against separate HUMAN labels only."""
from __future__ import annotations

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/"src/transform"))
from resolve_movies import digest_json, input_registration, load_config, save_json
from acquire_products import timestamp
from register_source import hash_file, utc_now


def metrics(gold, predicted):
    if len(gold) != len(predicted):
        raise ValueError("Gold/prediction lengths differ")
    tp = sum(a and b for a, b in zip(gold, predicted))
    fp = sum(not a and b for a, b in zip(gold, predicted))
    fn = sum(a and not b for a, b in zip(gold, predicted))
    tn = sum(not a and not b for a, b in zip(gold, predicted))
    precision = tp/(tp+fp) if tp+fp else None
    recall = tp/(tp+fn) if tp+fn else None
    f1 = None if precision is None or recall is None else 2*precision*recall/(precision+recall) if precision+recall else 0.0
    return dict(tp=tp, fp=fp, fn=fn, tn=tn, precision=precision, recall=recall, f1=f1,
        zero_denominator_policy="Undefined precision/recall is NULL, not perfect. F1 NULL when a component is undefined; F1=0 when both defined and zero.")


def evaluate(config, labels_path):
    _, registration = input_registration(config, verify_input=False)
    package = json.loads(Path(labels_path).read_text(encoding="utf-8-sig"))
    labeled = [row for row in package["pairs"] if row.get("label") in {"same_work", "different_work"}]
    if len(labeled) < 100 or {row["label"] for row in labeled} != {"same_work", "different_work"}:
        raise ValueError("At least100 independent HUMAN labels including both positive and negative pairs required")
    if len({row["pair_id"] for row in labeled}) != len(labeled):
        raise ValueError("Duplicate evaluation pair labels")
    from verify_task4 import verify
    verify(config)
    with closing(sqlite3.connect((Path(config["work_dir"])/"resolution.sqlite3").as_uri()+"?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        if json.loads(db.execute("SELECT value FROM state WHERE key='input'").fetchone()[0]) != registration:
            raise ValueError("Evaluation source/rule binding changed")
        gold, predicted, errors = [], [], []
        for row in labeled:
            item = db.execute("SELECT p.* FROM evaluation_pairs e JOIN pairs p USING(pair_id) WHERE e.pair_id=?", (row["pair_id"],)).fetchone()
            if item is None or row.get("partition") != "independent_human_evaluation" or row["pair_id"] in registration["development_pair_ids"]:
                raise ValueError("Labels must come from the reserved queue, not rule-development pairs")
            if row.get("labeler_type") != "human" or not row.get("labeled_by") or not row.get("labeling_evidence") or not row.get("rationale"):
                raise ValueError("Human labeler, verifiable evidence and reasoning required; do not invent them")
            timestamp(row["labeled_at"])
            left = {r[0] for r in db.execute("SELECT movie_id FROM product_movie WHERE asin=?", (item["left_asin"],))}
            right = {r[0] for r in db.execute("SELECT movie_id FROM product_movie WHERE asin=?", (item["right_asin"],))}
            same = bool(left & right)
            truth = row["label"] == "same_work"
            gold.append(truth)
            predicted.append(same)
            if same != truth:
                errors.append(dict(pair_id=row["pair_id"], left_asin=item["left_asin"], right_asin=item["right_asin"], kind="false_merge" if same else "missed_merge", human_label=row["label"]))
        queue_count = db.execute("SELECT COUNT(*) FROM evaluation_pairs").fetchone()[0]
    result = dict(observed_at=utc_now(), status="evaluated_on_declared_independent_human_labels", input_fingerprint=digest_json(registration),
        labeled_pairs=len(labeled), reserved_queue_pairs=queue_count, label_coverage=len(labeled)/queue_count,
        labels_path=str(Path(labels_path).resolve()), labels_sha256=hash_file(labels_path), metrics=metrics(gold, predicted), errors=errors,
        scope="Constructed held-out pair sample only, not full-dataset accuracy. Human provenance is declared and must be auditable; code cannot certify that a person actually performed labeling.")
    save_json(Path(config["report_dir"])/"evaluation_results.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=ROOT/"config/task4.example.json")
    parser.add_argument("--labels", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(evaluate(load_config(args.config), args.labels), indent=2))


if __name__ == "__main__":
    main()
