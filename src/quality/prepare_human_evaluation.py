"""Export blinded, editable annotation packets without inventing human judgments."""
import argparse
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'src/transform'))
from integration_common import load_config, ro, required, save_json, hash_file, digest_json, utc_now
from resolve_movies import load_config as movie_config, input_registration


def blinded_packet(source, kind, registry_hash, model_fingerprint):
    rows = []
    for original in source['pairs']:
        row = dict(pair_id=original['pair_id'], partition='independent_human_evaluation')
        for side in ('left', 'right'):
            context = deepcopy(original[side])
            # These person fields were attached by the model, not independently
            # observed source credits. Hide them, and hide model-based strata.
            for key in ('person_id', 'nconst', 'identity_status', 'birth_year', 'death_year', 'classification'):
                context.pop(key, None)
            row[side] = context
        row.update(label=None, labeler_type=None, labeled_by=None, labeled_at=None,
            labeling_evidence=None, rationale=None, reviews=[], adjudication=None)
        rows.append(row)
    return dict(format_version='blinded-human-annotation-v1', entity_kind=kind,
        evaluation_binding=dict(registry_sha256=registry_hash, model_fingerprint=model_fingerprint),
        predictions_hidden=True, gold_labels_generated=0,
        instructions='Use original evidence independently. Fill direct HUMAN judgment fields, or reviews plus human adjudication for disagreements. unresolved is allowed but does not count toward the >=100 resolved labels. Do not open model-derived registry identities while annotating.',
        pairs=rows)


def prepare(c, output):
    output = Path(output).resolve()
    tc = movie_config(c['task4_config'])
    _, registration = input_registration(tc, verify_input=False)
    movie_path = Path(tc['work_dir'])/'evaluation_queue.json'
    person_path = ROOT/c['person_evaluation_registry']
    movies = json.loads(movie_path.read_text(encoding='utf-8'))
    people = json.loads(person_path.read_text(encoding='utf-8'))
    movie_stats=json.loads((Path(tc['report_dir'])/'full_statistics.json').read_text(encoding='utf-8'))
    person_audit=json.loads((Path(c['report_root'])/'task5/evaluation_queue_audit.json').read_text(encoding='utf-8'))
    if hash_file(movie_path)!=movie_stats['evaluation']['queue_sha256'] or hash_file(person_path)!=person_audit['registry']['sha256']:
        raise ValueError('Reserved evaluation registry changed; do not export altered samples')
    with closing(ro(Path(c['work_dir'])/'integration.sqlite3')) as db:
        person_fingerprint = required(db, 'task5')
    if people['integration_task5_fingerprint'] != person_fingerprint:
        raise ValueError('Person registry model binding changed')
    source_files = {'movies': movie_path, 'people': person_path}
    packets = {'movies': blinded_packet(movies, 'movie', hash_file(movie_path), digest_json(registration)),
        'people': blinded_packet(people, 'person', hash_file(person_path), person_fingerprint)}
    # Check every destination before writing any: never overwrite user's labels.
    for kind, packet in packets.items():
        dest = output/(kind+'.labels.json')
        if dest.exists() and json.loads(dest.read_text(encoding='utf-8')) != packet:
            raise ValueError('Existing annotation packet differs; preserve labels and use a new directory')
    results = {}
    for kind, packet in packets.items():
        dest = output/(kind+'.labels.json')
        if not dest.exists():
            save_json(dest, packet)
        results[kind] = dict(path=str(dest), sha256=hash_file(dest), reserved_pairs=len(packet['pairs']),
            human_labeled_pairs=0, registry_path=str(source_files[kind]),
            evaluation_binding=packet['evaluation_binding'])
    report = dict(observed_at=utc_now(), status='ready_for_independent_HUMAN_annotation_NOT_evaluated',
        packets=results, no_model_predictions_or_gold_labels_generated=True,
        person_model_identity_columns_removed=['nconst', 'identity_status', 'birth_year', 'death_year'],
        sampling_strata_hidden_from_annotators=True, metrics=dict(precision=None, recall=None, f1=None),
        scope='Engineering handoff only; >=100 genuinely human-resolved pairs per entity, both classes and difficult-case coverage still required.')
    save_json(Path(c['report_root'])/'final/human_evaluation_handoff.json', report)
    return report


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--config', type=Path, default=ROOT/'config/integration.example.json')
    p.add_argument('--output-dir', type=Path, default=ROOT/'data/evaluation/annotation_v1')
    a = p.parse_args()
    print(json.dumps(prepare(load_config(a.config), a.output_dir), ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
