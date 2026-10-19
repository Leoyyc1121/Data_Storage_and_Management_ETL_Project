"""Acceptance of versioned five-group ETL output, replay and sample recovery.

Does not create HUMAN labels or count proposals as materialized movie groups.
Run after the main DAG has completed; large input files remain local-only.
"""
from contextlib import closing
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'))
sys.path.insert(0,str(ROOT/'src/load'))
sys.path.insert(0,str(ROOT/'src/quality'))
from integration_common import load_config,ro,save_json,utc_now,hash_file
from build_target import run as load_target
from verify_movie_merge_cases import verify as verify_cases,NEW_PAIRS

TABLES=['raw_reviews','logical_reviews','reviewers','editions','movies','people','credits','movie_person','movie_genres',
    'source_records','assertions','field_selections','selection_members','lineage_edges','cross_edition_candidates']


def counts(path):
    with closing(ro(path)) as d:
        return {table:d.execute('SELECT count(*) FROM '+table).fetchone()[0] for table in TABLES}


def child(arguments,label,expected=0):
    log=ROOT/'reports/final'/('movie5-'+label+'.log')
    with log.open('w',encoding='utf-8') as output:
        process=subprocess.run([sys.executable,'-B',*arguments],cwd=ROOT,stdout=output,stderr=subprocess.STDOUT,
            env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',PYTHONUTF8='1',PYTHONIOENCODING='utf-8'))
    if process.returncode!=expected:
        raise ValueError('Acceptance subprocess failed: '+label+'; inspect '+str(log))
    return dict(exit_code=process.returncode,log_path=str(log),sha256=hash_file(log))


def verify(c):
    needed={'task3','task4','movie_case_audit','imdb_download','imdb_import','task5','task5_verify','alias_audit',
        'task6','task7','task7_verify','task9','target_verify','demo_queries','field_completeness'}
    main=json.loads((ROOT/'reports/pipeline/last_execution.json').read_text(encoding='utf-8'))
    jobs={r['job']:r for r in main['results']}
    if not needed<=set(jobs):
        main=json.loads((ROOT/'reports/pipeline/movie5_primary_execution.json').read_text(encoding='utf-8'))
        jobs={r['job']:r for r in main['results']}
    if main['status']!='complete' or not needed<=set(jobs) or any(jobs[j]['status']!='complete' for j in needed):
        raise ValueError('Complete affected-stage main DAG execution required, not disconnected edits')
    if main['definition_sha256']!=hash_file(ROOT/'config/pipeline.example.json'):
        raise ValueError('Main run job-definition binding changed')
    cases=verify_cases(c)
    target=Path(c['target_dir'])/'etl.sqlite3'
    before=counts(target)
    if (before['raw_reviews'],before['editions'],before['reviewers'])!=(7911684,253059,889176):
        raise ValueError('Full source population changed')
    mappings=[]
    with closing(ro(target)) as d:
        actual_groups=d.execute('SELECT count(*) FROM (SELECT movie_id FROM product_movie GROUP BY movie_id HAVING count(DISTINCT asin)>1)').fetchone()[0]
        if actual_groups<5:raise ValueError('Five work groups not loaded into the actual target')
        for pair in NEW_PAIRS:
            rows=[dict(r) for asin in pair for r in d.execute('SELECT asin,movie_id,relationship FROM product_movie WHERE asin=?',(asin,))]
            if len(rows)!=2 or len({r['movie_id'] for r in rows})!=1 or any(r['relationship']!='single_work' for r in rows):
                raise ValueError('New pair missing from actual final-target mappings')
            mappings.append(rows)
    repeat=load_target(c)
    after=counts(target)
    if repeat['status']!='already_complete_no_insert' or before!=after:
        raise ValueError('Same-input target replay was not idempotent')
    import offline_acceptance as offline
    checkpoint=ROOT/'data/acceptance/fixed_v3/acceptance_checkpoint.json'
    progress=json.loads(checkpoint.read_text(encoding='utf-8')) if checkpoint.exists() else {}
    commands=progress.get('commands',{})
    for item in commands.values():
        if hash_file(item['log_path'])!=item['sha256']:
            raise ValueError('Historical acceptance command log changed; preserve and inspect it')
    if not (offline.BASE/'evidence.json').exists():
        commands['export']=child(['-m','src.quality.offline_acceptance','export'],'offline-export')
    sample_report=ROOT/'reports/final/offline_acceptance.json'
    if 'interruption' not in commands:
        if (offline.BASE/'run_v1/target/etl.sqlite3').exists():
            raise ValueError('No registered interruption for this completed sample; do not invent one')
        commands['interruption']=child(['-m','src.quality.offline_acceptance','run','--stop-after-phase','integration'],'offline-interruption',130)
        save_json(checkpoint,dict(commands=commands))
    if 'first_resume' not in progress:
        commands['resume']=child(['-m','src.quality.offline_acceptance','run'],'offline-resume')
        first=json.loads(sample_report.read_text(encoding='utf-8'))
        progress=dict(commands=commands,first_resume=first)
        save_json(checkpoint,progress)
    else:
        first=progress['first_resume']
    commands['repeat']=child(['-m','src.quality.offline_acceptance','run'],'offline-repeat')
    second=json.loads(sample_report.read_text(encoding='utf-8'))
    if first['counts']!=second['counts'] or second['target_action']!='already_complete_no_insert':
        raise ValueError('Offline repeat changed entities or relationships')
    save_json(checkpoint,dict(commands=commands,first_resume=first))
    sample_target=offline.BASE/'run_v1/target/etl.sqlite3'
    with closing(ro(sample_target)) as d:
        sample_groups=d.execute('SELECT count(*) FROM (SELECT movie_id FROM product_movie GROUP BY movie_id HAVING count(DISTINCT asin)>1)').fetchone()[0]
    if sample_groups<5:raise ValueError('Fixed raw-evidence sample does not replay at least five groups')
    fixed=json.loads((ROOT/'reports/final/offline_sample_manifest.json').read_text(encoding='utf-8'))
    recovery=dict(status='passed_actual_offline_target_interruption_and_repeat',scope='fixed_v3 real-evidence sample, not full-dataset interruption or HUMAN evaluation',
        sample_packet_sha256=fixed['packet']['sha256'],current_run_directory=str(offline.BASE/'run_v1'),
        interrupted_after_phase='integration',observed_interruption_python_exit_code=130,resumed_action=first['target_action'],
        same_input_repeat_action=second['target_action'],before_counts=first['counts'],after_counts=second['counts'],
        actual_cross_edition_groups=sample_groups,query_count_executed=len(second['queries']),network_requests_during_replay=0,commands=commands)
    save_json(ROOT/'reports/final/offline_recovery.json',recovery)
    published=json.loads((ROOT/'reports/task9/load_statistics.json').read_text(encoding='utf-8'))
    save_json(ROOT/'reports/task9/idempotency_recovery.json',dict(status='passed',input_fingerprint=published['input_fingerprint'],
        physical_file_size_bytes=target.stat().st_size,published_file_sha256=published['sha256'],
        intentional_interruption=dict(scope='Current full target was not intentionally interrupted; actual current fixed_v3 interruption/resume is recorded separately.',evidence='reports/final/offline_recovery.json'),
        completed_via_dag=dict(run_id=main['run_id'],status=main['status'],jobs=[dict(job=r['job'],status=r['status'],exit_code=r['exit_code']) for r in main['results']]),
        same_input_repeat=dict(action=repeat['status'],before_counts=before,after_counts=after,all_fifteen_counts_compared_equal=True),
        source_update_demonstration='Task3 content evidence v2 to v3 and Task4 primary-content scope v4 recomputed final_v8 without overwriting final_v7.',scope='Actual engineering evidence, NOT HUMAN accuracy.'))
    result=dict(observed_at=utc_now(),status='passed_five_group_versioned_pipeline_acceptance',main_dag_run_id=main['run_id'],
        affected_jobs_complete=len(needed),actual_full_target_work_groups=actual_groups,new_pair_target_mappings=mappings,
        raw_review_retention=before['raw_reviews'],logical_reviews=before['logical_reviews'],same_input_all_fifteen_counts_equal=True,
        offline_sample_groups=sample_groups,offline_actual_exit130_resume_repeat=True,source_downloads_or_HTTP_requests_in_this_recompute=0,
        old_versions_preserved=True,case_evidence='reports/task4/five_group_acceptance.json',human_labels_generated=0,
        limitation='Five-group requirement and reproducible engineering verified; independent HUMAN movie/person evaluation remains unfinished.')
    save_json(ROOT/'reports/final/movie_merge_pipeline_acceptance.json',result)
    return result


if __name__=='__main__':
    print(json.dumps(verify(load_config()),ensure_ascii=False,indent=2))
