"""Verify actual cross-edition work groups, not independent HUMAN gold labels."""
from contextlib import closing
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'src/transform'))
from integration_common import load_config, ro, save_json, hash_file, utc_now
from resolve_movies import load_config as movie_config, load_rules, compare, pair_id

NEW_PAIRS = [('6304994540', '6304994567'), ('B0012QE4Q2', 'B0017APPSE')]
SCOPE_COUNTEREXAMPLE = ('B002E2QH1I', 'B00383XYN6')


def verify(c):
    tc = movie_config(c['task4_config'])
    rules = load_rules(tc['rules_file'])
    with closing(ro(Path(tc['work_dir'])/'resolution.sqlite3')) as d:
        groups = []
        actual_group_count = d.execute("SELECT count(*) FROM works WHERE status='multifield_group'").fetchone()[0]
        for w in d.execute("SELECT * FROM works WHERE status='multifield_group' ORDER BY movie_id"):
            profiles = [json.loads(r[0]) for r in d.execute('SELECT p.profile FROM products p JOIN product_movie m USING(asin) WHERE m.movie_id=? ORDER BY asin', (w['movie_id'],))]
            comparisons = []
            held_out = False
            for i, left in enumerate(profiles):
                for right in profiles[i+1:]:
                    verdict = compare(left, right, rules)
                    if verdict['proposed_decision'] != 'auto_merge':
                        raise ValueError('Actual cluster contains an unsupported pair')
                    if d.execute('SELECT 1 FROM evaluation_pairs WHERE pair_id=?', (pair_id(left['asin'], right['asin']),)).fetchone():
                        held_out = True
                    comparisons.append(dict(left_asin=left['asin'], right_asin=right['asin'], rule_id=verdict['rule_id'], facts=verdict['facts']))
            if held_out:
                continue  # Do not expose reserved predictions as teaching cases.
            groups.append(dict(movie_id=w['movie_id'], title=w['display_title'], asins=[p['asin'] for p in profiles], comparisons=comparisons,
                sources=[dict(asin=p['asin'], source_record_id=r['source_record_id'], archive_sha256=r['archive_sha256'], record_sequence=r['record_sequence'], byte_start=r['byte_start'], byte_end=r['byte_end'], raw_record_sha256=r['raw_record_sha256']) for p in profiles for r in p['records']]))
        if len(groups) < 5:
            raise ValueError('At least five actually materialized independent work groups required')
        new_groups = []
        for a, b in NEW_PAIRS:
            matches = [g for g in groups if {a, b} <= set(g['asins'])]
            if len(matches) != 1:
                raise ValueError('New case is not an actual materialized work group')
            new_groups.append(matches[0]['movie_id'])
        if len(set(new_groups)) != 2:
            raise ValueError('Two new cases must represent two different works')
        profiles = [json.loads(d.execute('SELECT profile FROM products WHERE asin=?', (asin,)).fetchone()[0]) for asin in SCOPE_COUNTEREXAMPLE]
        counter = compare(*profiles, rules)
        if counter['rule_id'] != 'episodic_installment_not_complete_feature' or counter['final_decision'] != 'reject':
            raise ValueError('Episode/feature counterexample was not kept separate')
        packet = Path(tc['work_dir'])/'verified_merge_case_evidence.json'
        save_json(packet, dict(scope='source_supported_development_cases_NOT_HUMAN_evaluation', groups=groups, counterexample=dict(asins=SCOPE_COUNTEREXAMPLE, verdict=counter)))
        result = dict(observed_at=utc_now(), status='passed_at_least_five_actual_source_supported_work_groups', actual_work_groups=actual_group_count, demonstrated_nonheldout_groups=len(groups), required=5,
            distinct_new_work_groups=2, new_cases=[dict(movie_id=g['movie_id'], title=g['title'], asins=g['asins']) for g in groups if g['movie_id'] in new_groups],
            all_cluster_pairs_rechecked=True, demonstration_evaluation_overlap=0, episode_feature_false_merge_prevented=True,
            evidence_packet=dict(path=str(packet), sha256=hash_file(packet), bytes=packet.stat().st_size),
            counts_changed_by_real_mappings_not_report_edits=True, independent_human_evaluation_completed=False)
        save_json(Path(tc['report_dir'])/'five_group_acceptance.json', result)
        return result


if __name__ == '__main__':
    print(json.dumps(verify(load_config()), ensure_ascii=False, indent=2))
