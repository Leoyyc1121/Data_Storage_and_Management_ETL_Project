"""Export and run a fixed real-evidence offline sample, never a human gold set.

Raw/IMDb-derived evidence is local-only. Public manifest contains hashes, source
locators and aggregate coverage. Restore/export once, then run without network.
"""
import argparse
import base64
from collections import Counter,defaultdict
from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import uuid
import pyarrow.parquet as pq
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'));sys.path.insert(0,str(ROOT/'src/extract'));sys.path.insert(0,str(ROOT/'src/load'))
import integration_common as ic
from integration_common import *
import resolve_movies as movies
import resolve_people as people
import select_fields as fields
import enrich_release as release
import clean_reviews as reviews
from parse_reviews import parse_record
from classify_products import load_config as task3_config,load_rules as class_rules,classify_record,extract_fields,consensus
from acquire_products import load_config as task2_config
from acquire_imdb import load_config as imdb_config
from build_target import run as load_target
BASE=ROOT/'data/acceptance/fixed_v3'
ASINS=['630242335X','6305252599','B006QS9M1S','B006QS9MC2','B001EBO90G','B001L67A3M','B000WTVZGC','B003ZZ7TPG',
    '6304994540','6304994567','B0012QE4Q2','B0017APPSE','B002E2QH1I','B00383XYN6']

def verify_policy_failure(value):
    raw=base64.b64decode(value['raw_response_base64'])
    if hashlib.sha256(raw).hexdigest()!=value['raw_response_sha256']:
        raise ValueError('Offline policy-failure raw response changed')
    policy=value['original_policy']['policy']
    if policy['http_status']!=503 or policy['rules_usable'] is not False or policy['content_hash']!=value['raw_response_sha256']:
        raise ValueError('Offline failure is not the registered unavailable robots response')
    rows=value['product_statuses']
    if not rows or any(r['status']!='not_collected' or r['attempts']!=0 or r['http_status'] is not None or r['acquired_at'] is not None for r in rows):
        raise ValueError('Offline sample must not turn policy failure into product success')
    return dict(status='passed_original_policy_response_and_product_status_replay',policy_http_status=503,
        sample_products=len(rows),product_attempts=0,raw_response_sha256=value['raw_response_sha256'],network_requests=0)
def export(c):
    BASE.mkdir(parents=True,exist_ok=True);packet=BASE/'evidence.json'
    if packet.exists():raise ValueError('Fixed packet already exists; preserve it. Use run, not overwrite.')
    tc=movies.load_config(c['task4_config']);t3=task3_config(tc['task3_config']);t2=task2_config(t3['task2_config']);im=imdb_config(c['imdb_config']);asins=set(ASINS)
    with closing(ro(Path(tc['work_dir'])/'resolution.sqlite3')) as t4,closing(ro(Path(c['work_dir'])/'integration.sqlite3')) as integrated,closing(ro(Path(t2['work_dir'])/'metadata.sqlite3')) as meta:
        for label in ['non_movie','compilation','television_series']:
            r=t4.execute('SELECT asin FROM products WHERE classification=? AND metadata_records>0 ORDER BY asin LIMIT 1',(label,)).fetchone()
            if r:asins.add(r[0])
        for r in t4.execute("SELECT asin FROM products WHERE json_extract(profile,'$.classification_reason')='inner_asin_conflict'"):asins.add(r[0])
        for r in integrated.execute("SELECT movie_id FROM work_matches WHERE status='verified_multifield' ORDER BY movie_id LIMIT 2"):
            asins.update(x[0] for x in t4.execute('SELECT asin FROM product_movie WHERE movie_id=?',(r[0],)))
        # Source-supported substantive variants are demonstration/development
        # examples, NOT human evaluation labels. Include actual work evidence.
        from resolve_people import name_relation
        alias_people=set()
        for r in integrated.execute('SELECT a.movie_id,a.raw_name,p.person_id,p.canonical_name FROM person_aliases a JOIN people p USING(person_id) WHERE p.nconst IS NOT NULL ORDER BY p.person_id,a.alias_id'):
            if name_relation(r['raw_name'],r['canonical_name'])=='contextual_middle_name_or_surname_expansion' and (r['person_id'] in alias_people or len(alias_people)<5):
                alias_people.add(r['person_id']);asins.update(x[0] for x in t4.execute('SELECT asin FROM product_movie WHERE movie_id=?',(r['movie_id'],)))
        raw_meta=[dict(r) for asin in sorted(asins) for r in meta.execute('SELECT * FROM records WHERE asin=? ORDER BY source_id,record_sequence',(asin,))]
        for r in raw_meta:r['raw_record_base64']=base64.b64encode(r.pop('raw_record')).decode()
        schema=[r[0] for r in t4.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
    found={};manifest=json.loads((ROOT/'reports/task1/full_v2_output_manifest.json').read_text(encoding='utf-8'))
    for f in manifest['files']:
        for batch in pq.ParquetFile(Path(c['source_reviews'])/f['path']).iter_batches(columns=['product_id','record_sequence','byte_start','byte_end','start_line','end_line']):
            for row in batch.to_pylist():
                if row['product_id'] in asins and row['product_id'] not in found:found[row['product_id']]=row
        if len(found)==len(asins):break
    if set(found)!=asins:raise ValueError('Offline sample ASIN has no original review')
    raw_reviews=[]
    with (ROOT/'data/raw/movies.txt').open('rb') as stream:
        for asin,r in sorted(found.items()):
            stream.seek(r['byte_start']);raw=stream.read(r['byte_end']-r['byte_start']);raw_reviews.append(dict(**r,raw_base64=base64.b64encode(raw).decode(),raw_sha256=hashlib.sha256(raw).hexdigest()))
    original_policy=json.loads((ROOT/'reports/task2/acquisition_policy.json').read_text(encoding='utf-8'))
    response=Path(original_policy['policy']['snapshot_path']).read_bytes()
    with closing(ro(Path(t2['work_dir'])/'acquisition.sqlite3')) as acquired:
        statuses=[dict(acquired.execute('SELECT asin,status,http_status,attempts,acquired_at FROM products WHERE asin=?',(asin,)).fetchone()) for asin in sorted(asins)]
    failure=dict(status='not_collected',evidence_reference='reports/task2/acquisition_policy.json',original_policy=original_policy,
        raw_response_base64=base64.b64encode(response).decode(),raw_response_sha256=hashlib.sha256(response).hexdigest(),
        product_statuses=statuses,reason='retained_original_robots_503_policy_failure_not_a_successful_page')
    verify_policy_failure(failure)
    value=dict(version='fixed-real-evidence-v3',asins=sorted(asins),metadata_records=raw_meta,reviews=raw_reviews,source_review_sha256=manifest['source_sha256'],source_review_run_id=manifest['run_id'],task4_schema=schema,release_schema=None,
        acquisition_failure=failure)
    with closing(ro(Path(c['work_dir'])/'integration.sqlite3')) as d:value['release_schema']=d.execute("SELECT sql FROM sqlite_master WHERE name='release_info'").fetchone()[0]
    save_json(packet,value)
    with closing(ro(Path(im['work_dir'])/'catalog.sqlite3')) as source,closing(sqlite3.connect(BASE/'catalog.sqlite3')) as dest:source.backup(dest)
    result=dict(version=value['version'],status='fixed_local_raw_evidence_exported',asins=sorted(asins),review_records=len(raw_reviews),metadata_records=len(raw_meta),
        packet=dict(path=str(packet.resolve()),bytes=packet.stat().st_size,sha256=hash_file(packet)),catalog=dict(path=str((BASE/'catalog.sqlite3').resolve()),sha256=hash_file(BASE/'catalog.sqlite3')),
        source_review_sha256=manifest['source_sha256'],coverage=['real_cross_edition_groups','non_movies','compilations','missing_full_dates','source_inner_asin_conflicts','actual_policy_acquisition_failure','substantive_contextual_person_aliases'],
        substantive_alias_groups=len(alias_people),unmet=(['Fewer than5 substantive contextual alias groups in fixed source evidence.'] if len(alias_people)<5 else [])+['Independent HUMAN movie/person labels remain absent.'],distribution='Local raw/IMDb-derived evidence not committed. Restore registered sources and run export once, preserve hashes; subsequent run needs no network. Old fixed_v1 evidence preserved.')
    save_json(ROOT/'reports/final/offline_sample_manifest.json',result);return result
def execute(c,stop_after_phase=None):
    public=json.loads((ROOT/'reports/final/offline_sample_manifest.json').read_text(encoding='utf-8'))
    for key,filename in (('packet','evidence.json'),('catalog','catalog.sqlite3')):
        # Recorded historical locations are provenance, not machine bindings.
        if hash_file(BASE/filename)!=public[key]['sha256']:raise ValueError('Fixed evidence packet changed')
    packet=json.loads((BASE/'evidence.json').read_text(encoding='utf-8'));work=BASE/c.get('offline_run_name','run_v1');work.mkdir(parents=True,exist_ok=True)
    failure_replay=verify_policy_failure(packet['acquisition_failure'])
    if {row['asin'] for row in packet['acquisition_failure']['product_statuses']}!=set(packet['asins']):
        raise ValueError('Offline acquisition-status manifest does not cover the fixed ASIN population')
    core_files=['src/extract/parse_reviews.py','src/transform/classify_products.py','src/transform/resolve_movies.py','src/transform/resolve_people.py','src/transform/enrich_release.py','src/transform/select_fields.py','src/transform/clean_reviews.py','src/load/build_target.py']
    signature=digest_json(dict(packet=public['packet']['sha256'],catalog=public['catalog']['sha256'],helper=hash_file(__file__),core={f:hash_file(ROOT/f) for f in core_files}))
    reg=work/'run_registration.json'
    if reg.exists() and json.loads(reg.read_text())['fingerprint']!=signature:raise ValueError('Offline source/core/helper changed; preserve old run and select a new version directory')
    save_json(reg,dict(fingerprint=signature,scope='fixed_acceptance_sample_not_full_dataset'))
    sys.path.insert(0,str(ROOT/'src/quality'))
    from verify_catalog import verify as verify_raw_catalog
    catalog_verification=verify_raw_catalog(BASE/'catalog.sqlite3')
    cfg=dict(c,work_dir=str(work),target_dir=str(work/'target'),report_root=str(work/'reports'),catalog_path=str(BASE/'catalog.sqlite3'),expected_reviews=len(packet['reviews']),scope='fixed_acceptance_sample_not_full_dataset',evaluation_pairs=12)
    tc=movies.load_config(c['task4_config']);t3=task3_config(tc['task3_config']);crules=class_rules(t3['rules_file']);mrules=movies.load_rules(tc['rules_file'])
    # Source registration and raw record hashing, before any transform.
    with closing(sqlite3.connect(work/'metadata.sqlite3')) as meta:
        meta.row_factory=sqlite3.Row
        meta.execute('CREATE TABLE IF NOT EXISTS records(asin TEXT,source_id TEXT,archive_sha256 TEXT,record_sequence INTEGER,byte_start INTEGER,byte_end INTEGER,raw_record_sha256 TEXT,raw_record BLOB,source_downloaded_at TEXT,PRIMARY KEY(source_id,record_sequence))')
        with meta:
            for r in packet['metadata_records']:
                raw=base64.b64decode(r['raw_record_base64'])
                if hashlib.sha256(raw).hexdigest()!=r['raw_record_sha256']:raise ValueError('Fixed raw metadata hash')
                meta.execute('INSERT OR IGNORE INTO records VALUES (?,?,?,?,?,?,?,?,?)',tuple(r[k] for k in ['asin','source_id','archive_sha256','record_sequence','byte_start','byte_end','raw_record_sha256'])+(raw,r['source_downloaded_at']))
        with closing(sqlite3.connect(work/'resolution.sqlite3')) as t4,closing(open_db(cfg)) as d,closing(ro(BASE/'catalog.sqlite3')) as imdb:
            t4.row_factory=sqlite3.Row
            if not t4.execute("SELECT 1 FROM sqlite_master WHERE name='products'").fetchone():t4.executescript(';\n'.join(packet['task4_schema'])+';')
            people.schema(d);d.execute(packet['release_schema'].replace('CREATE TABLE ','CREATE TABLE IF NOT EXISTS ',1));fields.schema(d)
            stage=d.execute("SELECT status FROM stage_state WHERE stage='task7'").fetchone()
            if not stage or stage[0]!='complete':
                with t4:
                    for asin in packet['asins']:
                        rows=list(meta.execute('SELECT * FROM records WHERE asin=? ORDER BY source_id,record_sequence',(asin,)));candidates=[]
                        for r in rows:
                            item=json.loads(r['raw_record']);decision=classify_record(item,crules);candidates.append(dict(**decision,fields=extract_fields(item,crules)))
                        label,reason,confidence=consensus(candidates)
                        source=dict(asin=asin,label=label,reason=reason,review_count=1,first_review_id=None,source_review_sha256=packet['source_review_sha256'],source_review_run_id=packet['source_review_run_id'])
                        p=movies.make_profile(source,rows,mrules)
                        t4.execute('INSERT INTO products VALUES (?,?,?,?,?,?,?)',(asin,label,1,len(rows),js(p),digest_json(p),'offline'))
                    profiles={r['asin']:json.loads(r['profile']) for r in t4.execute('SELECT * FROM products')}
                    pairs,_=movies.generate_candidates(profiles,mrules)
                    for pid,a,b in pairs:
                        e=movies.compare(profiles[a],profiles[b],mrules)
                        t4.execute('INSERT INTO pairs VALUES (?,?,?,?,?,?,?,?,?)',(pid,a,b,e['proposed_decision'],e['final_decision'],e['rule_id'],js(e),mrules['version'],'offline'))
                    movies.materialize_clusters(t4,mrules,'offline',profiles)
                rid=begin(d,'task5',dict(packet=public['packet']['sha256'],code=hash_file(__file__)))
                if rid:
                    with d:people.build(d,cfg,t4,imdb,meta,rid);finish(d,'task5',rid)
                rid=begin(d,'task6',dict(task5=required(d,'task5'),rule=release.VERSION))
                if rid:
                    with d:
                        for m in d.execute('SELECT * FROM work_matches').fetchall():
                            t=imdb.execute('SELECT * FROM titles WHERE tconst=?',(m['tconst'],)).fetchone() if m['tconst'] else None;r=release.release_record(m,t);aid=None
                            if r['source_record_id']:aid=assertion(d,rid,'movie',m['movie_id'],'release_year',t['release_year'],r['release_year'],r['source_record_id'],'startYear',release.VERSION,r['year_reason'])
                            d.execute('INSERT INTO release_info VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(m['movie_id'],r['release_year'],r['release_date'],r['date_precision'],r['region'],r['release_type'],r['year_reason'],r['date_reason'],r['inferred_upper_bound'],r['inference_reason'],r['source_record_id'],aid))
                        finish(d,'task6',rid)
                rid=begin(d,'task7',dict(task6=required(d,'task6'),rule=fields.VERSION))
                if rid:
                    with d:fields.build(d,cfg,t4,imdb,meta,crules,rid);finish(d,'task7',rid)
    review_cfg=dict(cfg,review_version='review_work')
    with closing(reviews.open_reviews(review_cfg)) as rd:
        if not reviews.get(rd,'complete',False):
            parsed=[]
            for r in packet['reviews']:
                raw=base64.b64decode(r['raw_base64'])
                if hashlib.sha256(raw).hexdigest()!=r['raw_sha256']:raise ValueError('Fixed review raw hash')
                p=parse_record(raw,r['record_sequence'],packet['source_review_sha256'],tuple(r[k] for k in ('byte_start','byte_end','start_line','end_line')),packet['source_review_run_id']);parsed.append(p)
            with rd:
                rd.execute('INSERT INTO review_runs VALUES (?,?,?,?)',('offline',utc_now(),utc_now(),'complete'));reviews.insert_batch(rd,sorted(parsed,key=lambda x:x['record_sequence']),'offline')
                reviews.set_state(rd,'input',public['packet']['sha256']);reviews.set_state(rd,'complete',True)
    # Generated local config is an artifact, not a source-code edit.
    sample_t4=dict(tc,work_dir=str(work));save_json(work/'task4.json',sample_t4)
    save_json(work/'reviews.json',review_cfg);cfg.update(task4_config=str(work/'task4.json'),review_config=str(work/'reviews.json'))
    result=load_target(cfg,stop_after_phase)
    with closing(ro(Path(cfg['target_dir'])/'etl.sqlite3')) as target:
        counts={t:target.execute(f'SELECT count(*) FROM {t}').fetchone()[0] for t in ['raw_reviews','logical_reviews','editions','movies','people','credits','field_selections','lineage_edges']}
        if target.execute('PRAGMA foreign_key_check').fetchone():raise ValueError('Offline target FK')
    outcome=dict(status='passed_offline_core_end_to_end',scope=cfg['scope'],counts=counts,packet_sha256=public['packet']['sha256'],target_action=result['status'],
        actual_network_requests=0,human_evaluation_completed=False,coverage_limitations=public['unmet'],acquisition_failure_replay=failure_replay)
    sys.path.insert(0,str(ROOT/'src/quality'))
    from run_queries import run as execute_queries
    outcome['queries']=execute_queries(cfg);outcome['catalog_raw_field_replay']=catalog_verification
    from alias_audit import audit as verify_alias_cases
    outcome['substantive_alias_audit']=verify_alias_cases(cfg)
    if outcome['substantive_alias_audit']['substantive_person_groups']<5:raise ValueError('Fixed sample failed actual substantive alias coverage')
    save_json(work/'integration.json',cfg)
    save_json(ROOT/'reports/final/offline_acceptance.json',outcome);return outcome
def main():
    global BASE
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['export','run']);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');p.add_argument('--stop-after-phase',choices=['review_copy','integration','graph']);p.add_argument('--sample-dir',type=Path,default=BASE);p.add_argument('--run-name',default='run_v1');a=p.parse_args();c=load_config(a.config)
    if not re.fullmatch(r'[A-Za-z0-9_-]+',a.run_name):p.error('Run name must be a simple version name')
    BASE=a.sample_dir.resolve();c['offline_run_name']=a.run_name
    try:print(js(export(c) if a.action=='export' else execute(c,a.stop_after_phase)))
    except KeyboardInterrupt:raise SystemExit(130)
if __name__=='__main__':main()
