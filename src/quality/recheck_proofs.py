"""Actually repeat corrected stages/targets and record reconciliation evidence."""
from contextlib import closing
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/transform'));sys.path.insert(0,str(ROOT/'src/load'));sys.path.insert(0,str(ROOT/'src/quality'))
from integration_common import *
import resolve_people, enrich_release, select_fields, build_target, offline_acceptance, verify_task5

TABLES=['raw_reviews','logical_reviews','reviewers','editions','movies','people','credits','movie_person','movie_genres','source_records','assertions','field_selections','selection_members','lineage_edges','cross_edition_candidates']
def counts(path,tables):
    with closing(ro(path)) as d:return {t:d.execute(f'SELECT count(*) FROM "{t}"').fetchone()[0] for t in tables}

def run(c):
    for name,stage in [('task5',resolve_people),('task6',enrich_release),('task7',select_fields)]:
        baseline=json.loads((Path(c['report_root'])/name/'full_statistics.json').read_text(encoding='utf-8'))['tables']
        repeated=stage.run(c)['tables']
        if baseline!=repeated:raise ValueError('Repeated integration stage changed '+name)
        save_json(Path(c['report_root'])/name/'idempotency.json',dict(status='passed',version=Path(c['work_dir']).name,same_input_repeated=True,all_ordered_table_hashes_equal=True,tables=repeated,execution_evidence='Actual same-input stage function repeated; ordered table hashes compared before and after, not inferred from cached statistics.'))
    save_json(Path(c['report_root'])/'task5/full_verification.json',verify_task5.verify(c))
    target=Path(c['target_dir'])/'etl.sqlite3';before=counts(target,TABLES);result=build_target.run(c);after=counts(target,TABLES)
    if before!=after or result['status']!='already_complete_no_insert':raise ValueError('Target idempotence failure')
    load=json.loads((Path(c['report_root'])/'task9/load_statistics.json').read_text(encoding='utf-8'))
    physical=hash_file(target)
    if physical!=load['sha256']:raise ValueError('Published immutable target changed during repeat')
    registration=json.loads((Path(c['target_dir'])/'load_registration.json').read_text(encoding='utf-8'))
    save_json(Path(c['report_root'])/'task9/input_registration.json',registration)
    # Never copy a historical v7 interruption claim onto a newer target. This
    # checker observes a real current sample exit130/resume and records its scope.
    from verify_movie_merge_pipeline import verify as current_acceptance
    acceptance=current_acceptance(c)
    return dict(status='passed',integration_stage_repeats=3,full_target_counts=after,current_pipeline_acceptance=acceptance)
if __name__=='__main__':print(js(run(load_config())))
