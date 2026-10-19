"""Validate declared HUMAN judgments; never generate labels or certify authorship."""
from collections import Counter
from datetime import datetime
import json
from pathlib import Path


def judgment(value, allowed):
    if value.get('label') not in {*allowed, 'unresolved'}:
        raise ValueError('Unknown human label')
    if value.get('labeler_type') != 'human':
        raise ValueError('Human provenance required; AI/synthetic labels are not accepted')
    for key in ('labeled_by', 'labeled_at', 'labeling_evidence', 'rationale'):
        if not isinstance(value.get(key), str) or not value[key].strip():
            raise ValueError('Missing human provenance: ' + key)
    if datetime.fromisoformat(value['labeled_at'].replace('Z', '+00:00')).tzinfo is None:
        raise ValueError('Human labeling timestamp requires a timezone')
    return value['label']


def final_judgment(row, allowed):
    reviews = row.get('reviews') or []
    if not reviews:
        return (dict(row), False) if row.get('label') is None else (dict(row, label=judgment(row, allowed)), False)
    if not isinstance(reviews, list) or not all(isinstance(r, dict) for r in reviews):
        raise ValueError('Reviews must be a list of human judgment records')
    labels = [judgment(r, allowed) for r in reviews]
    if len({r['labeled_by'].strip().casefold() for r in reviews}) != len(reviews):
        raise ValueError('Duplicate reviewer is not independent double annotation')
    disagreed = len(set(labels)) > 1
    if disagreed:
        selected = row.get('adjudication')
        if not isinstance(selected, dict):
            raise ValueError('Disagreement requires an explicit human adjudication')
        label = judgment(selected, allowed)
    else:
        selected, label = reviews[0], labels[0]
    if row.get('label') is not None and row['label'] != label:
        raise ValueError('Final label conflicts with recorded human review/adjudication')
    return dict(row, **{k: selected[k] for k in ('label', 'labeler_type', 'labeled_by', 'labeled_at', 'labeling_evidence', 'rationale')}), disagreed


def load_labels(path, allowed, minimum=100):
    package = json.loads(Path(path).read_text(encoding='utf-8-sig'))
    rows = package.get('pairs')
    if not isinstance(rows, list) or not all(isinstance(r, dict) and isinstance(r.get('pair_id'), str) and r['pair_id'] for r in rows):
        raise ValueError('A list of uniquely identified reserved pairs is required')
    if len({r['pair_id'] for r in rows}) != len(rows):
        raise ValueError('Duplicate evaluation pairs, including pending pairs')
    completed, reviewed, disputes = [], 0, 0
    for row in rows:
        value, disputed = final_judgment(row, allowed)
        reviewed += value.get('label') is not None
        disputes += disputed
        if value.get('label') in allowed:
            completed.append(value)
    if len(completed) < minimum or {r['label'] for r in completed} != set(allowed):
        raise ValueError(f'At least{minimum} independent HUMAN labels including both classes required')
    progress = dict(submitted_pairs=len(rows), human_reviewed_pairs=reviewed,
        resolved_human_pairs=len(completed), unresolved_human_pairs=reviewed-len(completed),
        not_yet_reviewed_pairs=len(rows)-reviewed, adjudicated_disagreements=disputes,
        gold_class_counts=dict(Counter(r['label'] for r in completed)))
    return package, completed, progress


def verify_binding(package, registry_sha256, model_fingerprint):
    binding = package.get('evaluation_binding')
    if binding is not None and binding != dict(registry_sha256=registry_sha256, model_fingerprint=model_fingerprint):
        raise ValueError('Annotation package refers to a changed registry or model')


def coverage(progress, reserved_count):
    if progress['submitted_pairs'] > reserved_count:
        raise ValueError('Submitted evaluation set exceeds reserved queue')
    return dict(**progress, reserved_queue_pairs=reserved_count,
        manual_review_coverage=progress['human_reviewed_pairs']/reserved_count if reserved_count else None,
        resolved_label_coverage=progress['resolved_human_pairs']/reserved_count if reserved_count else None,
        human_unresolved_rate=progress['unresolved_human_pairs']/progress['human_reviewed_pairs'] if progress['human_reviewed_pairs'] else None,
        scope='Declared human annotation over the reserved sample; declarations cannot prove human authorship.')
