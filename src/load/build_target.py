"""Task9: phased recoverable SQLite target load and field-level lineage graph.

Reviews use a queryable per-field provenance view instead of materializing
hundreds of millions of identical extraction-rule rows. All raw locators remain.
"""
import argparse
from collections import defaultdict
from contextlib import closing
import json
import logging
import os
from pathlib import Path
import sqlite3
import sys
import uuid
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
from resolve_movies import load_config as task4_config
from clean_reviews import review_root

VERSION='target-field-lineage-v1'
def state(d,key):
    try:r=d.execute('SELECT value FROM target_state WHERE key=?',(key,)).fetchone()
    except sqlite3.OperationalError:return None
    return json.loads(r[0]) if r else None
def set_state(d,key,value):d.execute('INSERT OR REPLACE INTO target_state VALUES (?,?)',(key,js(value)))
def edge(d,child_kind,child,parent_kind,parent,role):
    d.execute('INSERT OR IGNORE INTO lineage_edges VALUES (?,?,?,?,?)',(child_kind,child,parent_kind,parent,role))
def recorded_selection(d,run,etype,eid,field,raw,value,sid,locator,reason,review_id=None):
    aid=assertion(d,run,etype,eid,field,raw,value,sid,locator,VERSION,reason)
    op=operation(d,run,'derive_identifier_or_processing_status',VERSION,[aid])
    selection='selection:'+digest_json([etype,eid,field,VERSION]);status='selected' if value is not None else 'missing'
    d.execute('INSERT INTO field_selections VALUES (?,?,?,?,?,?,?,?,?)',(selection,etype,eid,field,js(value),status,reason,VERSION,op))
    d.execute('INSERT INTO selection_members VALUES (?,?,?,?)',(selection,aid,'selected' if value is not None else 'not_selected',reason))
    if review_id:d.execute('INSERT INTO review_assertion_sources VALUES (?,?)',(aid,review_id))
    return aid
def graph(d,c,run):
    d.executescript('''
    CREATE TABLE IF NOT EXISTS lineage_edges(child_kind TEXT NOT NULL,child_id TEXT NOT NULL,parent_kind TEXT NOT NULL,parent_id TEXT NOT NULL,role TEXT NOT NULL,PRIMARY KEY(child_kind,child_id,parent_kind,parent_id,role));
    CREATE TABLE IF NOT EXISTS movie_person(movie_id TEXT REFERENCES movies(movie_id),person_id TEXT REFERENCES people(person_id),role TEXT NOT NULL,PRIMARY KEY(movie_id,person_id,role));
    CREATE TABLE IF NOT EXISTS movie_genres(movie_id TEXT REFERENCES movies(movie_id),genre TEXT NOT NULL,assertion_id TEXT REFERENCES assertions(assertion_id),PRIMARY KEY(movie_id,genre,assertion_id));
    CREATE TABLE IF NOT EXISTS review_field_rules(field TEXT PRIMARY KEY,raw_prefix TEXT NOT NULL,raw_column TEXT NOT NULL,normalized_column TEXT NOT NULL,operation_type TEXT NOT NULL,semantics TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS review_source_registry(source_sha256 TEXT PRIMARY KEY,source_url TEXT NOT NULL,source_path TEXT NOT NULL,downloaded_at TEXT,missing_time_reason TEXT);
    CREATE TABLE IF NOT EXISTS review_assertion_sources(assertion_id TEXT PRIMARY KEY REFERENCES assertions(assertion_id),source_review_id TEXT NOT NULL REFERENCES raw_reviews(source_review_id));
    ''')
    for r in d.execute('SELECT DISTINCT movie_id,person_id,role FROM credits'):d.execute('INSERT INTO movie_person VALUES (?,?,?)',tuple(r))
    for r in d.execute("SELECT s.entity_id,a.assertion_id,a.normalized_value FROM field_selections s JOIN selection_members m USING(selection_id) JOIN assertions a USING(assertion_id) WHERE s.entity_type='movie' AND s.field='genres' AND m.status='selected'"):
        for value in json.loads(r['normalized_value']):d.execute('INSERT OR IGNORE INTO movie_genres VALUES (?,?,?)',(r['entity_id'],value,r['assertion_id']))
    # Final person fields receive assertions with original TSV column or source-role evidence.
    for p in d.execute('SELECT * FROM people'):
        source=d.execute('SELECT * FROM source_records WHERE source_record_id=?',(p['name_source_record_id'],)).fetchone()
        provisional_locator=None
        if not p['nconst']:
            loc=d.execute('SELECT a.field_locator FROM credits c JOIN assertions a USING(assertion_id) WHERE c.person_id=? AND c.source_record_id=? ORDER BY c.credit_id LIMIT 1',(p['person_id'],p['name_source_record_id'])).fetchone()
            provisional_locator=loc[0] if loc else 'source_context_name_unresolved'
        for field,key in [('canonical_name','primaryName' if p['nconst'] else provisional_locator),('nconst','nconst' if p['nconst'] else provisional_locator),('birth_year','birthYear' if p['nconst'] else provisional_locator),('death_year','deathYear' if p['nconst'] else provisional_locator)]:
            value=p[field];raw=value
            if p['nconst']:
                cols=source['raw_record'].decode('utf-8').rstrip('\r\n').split('\t');index={'canonical_name':1,'nconst':0,'birth_year':2,'death_year':3}[field];raw=cols[index]
            aid=assertion(d,run,'person',p['person_id'],field,raw,value,p['name_source_record_id'],key,VERSION,'verified_name_record' if p['nconst'] else 'unresolved_source_context_name_not_global_identity')
            selection='selection:'+digest_json(['person',p['person_id'],field,VERSION]);op=operation(d,run,'select_person_identity_field',VERSION,[aid])
            d.execute('INSERT INTO field_selections VALUES (?,?,?,?,?,?,?,?,?)',(selection,'person',p['person_id'],field,js(value),'selected' if value is not None else 'missing','external_context_supported' if p['nconst'] else 'provisional_source_context',VERSION,op))
            d.execute('INSERT INTO selection_members VALUES (?,?,?,?)',(selection,aid,'selected' if value is not None else 'not_selected','source_value_available' if value is not None else 'source_missing_or_identity_unresolved'))
        recorded_selection(d,run,'person',p['person_id'],'person_id',dict(nconst=p['nconst'],source_record_id=p['name_source_record_id']),p['person_id'],p['name_source_record_id'],'$record_identity','UUIDv5 external-ID or explicitly provisional source-context anchor')
        recorded_selection(d,run,'person',p['person_id'],'identity_status',p['identity_status'],p['identity_status'],p['name_source_record_id'],'$processing_status','Context verification disposition, not a raw biographical fact')
    for w in d.execute('SELECT m.*,w.anchor_source_record_id FROM movies m JOIN works w USING(movie_id)'):
        mid=w['movie_id'];sid=w['anchor_source_record_id'];match=d.execute('SELECT * FROM work_matches WHERE movie_id=?',(mid,)).fetchone();e=json.loads(match['evidence'])
        recorded_selection(d,run,'movie',mid,'movie_id',sid,mid,sid,'$record_identity','UUIDv5 of creative-work-seed source record anchor, not ASIN')
        recorded_selection(d,run,'movie',mid,'identity_status',match['reason'],w['identity_status'],sid,'$processing_status','External identity matching disposition; Task4 merge-group status is retained separately')
        for field in ['tconst','date_precision']:
            source=e['selected_candidate']['title_source_record_id'] if w['tconst'] else sid
            recorded_selection(d,run,'movie',mid,field,w[field],w[field],source,'tconst' if field=='tconst' else 'startYear#precision','Verified external ID or explicit year-only/unknown precision, not a fabricated date')
    product_sources=defaultdict(list)
    for source in d.execute("SELECT source_record_id,raw_record FROM source_records WHERE source_record_id LIKE 'meta:%'"):
        product_sources[json.loads(source['raw_record'])['asin']].append(source['source_record_id'])
    for asin,sources in product_sources.items():
        for source in sources:edge(d,'product_context','product_context:'+asin,'source',source,'classification_and_edition_metadata_input')
    # Every original product has at least one preserved source review, even if metadata is unavailable.
    for e in d.execute('SELECT e.*,r.source_review_id,r.raw_record_sha256 FROM editions e JOIN raw_reviews r ON r.record_sequence=(SELECT min(l.representative_sequence) FROM logical_reviews l WHERE l.asin=e.asin)'):
        for field,key in [('asin','product/productId'),('classification','$task3_disposition'),('mapping_status','$task4_disposition'),('review_count','$count_original_reviews_for_ASIN')]:
            aid=recorded_selection(d,run,'product',e['asin'],field,e[field],e[field],None,key,'Source-review entity anchor; classification/mapping are recorded rule dispositions, review_count is the original raw-record denominator',e['source_review_id'])
            if field in ('classification','mapping_status'):edge(d,'assertion',aid,'product_context','product_context:'+e['asin'],'upstream_source_decision_context')
    for a in d.execute('SELECT * FROM assertions'):
        edge(d,'assertion',a['assertion_id'],'operation',a['operation_id'],'produced_by')
        if a['source_record_id']:edge(d,'assertion',a['assertion_id'],'source',a['source_record_id'],'raw_field_evidence')
    for a in d.execute('SELECT * FROM review_assertion_sources'):edge(d,'assertion',a['assertion_id'],'raw_review',a['source_review_id'],'review_record_anchor')
    for op in d.execute('SELECT * FROM operations'):
        for parent in json.loads(op['parents']):
            kind='operation' if parent.startswith('op:') else 'assertion' if parent.startswith('a:') else 'source'
            edge(d,'operation',op['operation_id'],kind,parent,'processing_input')
    for s in d.execute('SELECT * FROM field_selections'):
        edge(d,'selection',s['selection_id'],'operation',s['operation_id'],'selection_rule')
    # Attach identity matching as an explicit prerequisite of every work field.
    for w in d.execute('SELECT * FROM work_matches'):
        e=json.loads(w['evidence']);sid='work_match:'+w['movie_id'];sources=[]
        for candidate in d.execute('SELECT source_record_id FROM identity_candidate_sources WHERE movie_id=?',(w['movie_id'],)):
            edge(d,'work_match',sid,'source',candidate[0],'selected_or_unselected_identity_candidate_input')
        if w['tconst']:
            chosen=e['selected_candidate'];sources=chosen['source_record_ids']+[chosen['title_source_record_id']]
            sources += [r[0] for r in d.execute('SELECT DISTINCT source_record_id FROM credits WHERE movie_id=?',(w['movie_id'],))]
        for source in sorted(set(sources)):edge(d,'work_match',sid,'source',source,'identity_context_not_title_only')
        for a in d.execute("SELECT assertion_id FROM assertions WHERE entity_type='movie' AND entity_id=?",(w['movie_id'],)):
            edge(d,'assertion',a[0],'work_match',sid,'work_identity_prerequisite')
        for s in d.execute("SELECT selection_id FROM field_selections WHERE entity_type='movie' AND entity_id=?",(w['movie_id'],)):
            edge(d,'selection',s[0],'work_match',sid,'identity_prerequisite_even_for_missing_fields')
    rules=[('asin','product/productId','product_id','product_id','prefix_parse','Original ASIN, not movie identity'),('reviewer','review/userId','reviewer_id','reviewer_id','prefix_parse','Source reviewer identifier, not person/cast'),('profile_name','review/profileName','profile_name','profile_name','strict_utf8_parse','Undecodable fields NULL with original bytes'),('summary','review/summary','summary','summary_clean','html_unicode_whitespace','Raw text preserved; cleaned column separate'),('text','review/text','review_text','review_text_clean','html_unicode_whitespace','Raw text preserved; cleaned column separate'),('score','review/score','score_raw','score','validate_range','Finite 1..5 else NULL plus quality issue'),('helpful_votes','review/helpfulness','helpfulness_raw','helpful_votes','parse_integer_pair','Violations preserved and flagged'),('total_votes','review/helpfulness','helpfulness_raw','total_votes','parse_integer_pair','Violations preserved and flagged'),('helpfulness_ratio','review/helpfulness','helpfulness_raw','helpfulness_ratio','validated_division','Only 0<=helpful<=total and total>0'),('time','review/time','review_time_raw','review_time_utc','unix_utc_validate','Not a work-release-date inference')]
    d.executemany('INSERT INTO review_field_rules VALUES (?,?,?,?,?,?)',rules)
    registered=json.loads((ROOT/'reports/task1/source_manifest.reported.json').read_text(encoding='utf-8'))
    sha=d.execute('SELECT source_file_sha256 FROM raw_reviews LIMIT 1').fetchone()[0]
    d.execute('INSERT INTO review_source_registry VALUES (?,?,?,?,?)',(sha,'https://snap.stanford.edu/data/movies.txt.gz',str(ROOT/'data/raw/movies.txt'),registered.get('downloaded_at'),'Exact original compressed-download time not verified; preserve NULL if unknown'))
def copy_integration(d,c,t4):
    d.execute('ATTACH DATABASE ? AS integration',(str(Path(c['work_dir'])/'integration.sqlite3'),))
    d.execute('ATTACH DATABASE ? AS movie_resolution',(str(Path(t4['work_dir'])/'resolution.sqlite3'),))
    d.execute('PRAGMA defer_foreign_keys=ON')
    for database,tables in [('integration',None),('movie_resolution',['product_movie','product_status','works','pairs','cluster_checks','mapping_history'])]:
        entries=list(d.execute(f"SELECT name,sql FROM {database}.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"))
        for name,sql in entries:
            if tables is not None and name not in tables:continue
            # Task4 product FK references final editions through a lightweight compatibility view/table.
            if database=='movie_resolution':sql=sql.replace('REFERENCES products(asin)','REFERENCES editions(asin)')
            d.execute(sql);d.execute(f'INSERT INTO "{name}" SELECT * FROM {database}."{name}"')
    # Indexes and constraints are recreated from the source modeling scripts.
    for name,sql in d.execute("SELECT name,sql FROM integration.sqlite_master WHERE type='index' AND sql IS NOT NULL").fetchall():d.execute(sql)
    d.execute('CREATE TABLE upstream_registrations(stage TEXT PRIMARY KEY,registration TEXT NOT NULL)')
    for stage,path in [('task1','task1/source_manifest.reported.json'),('task2','task2/full_statistics.json'),('task3','task3/full_statistics.json'),('task4','task4/full_statistics.json')]:
        d.execute('INSERT INTO upstream_registrations VALUES (?,?)',(stage,(ROOT/'reports'/path).read_text(encoding='utf-8')))
    # Preserve rejected/ambiguous identity-input records as well as adopted
    # fields. Reverse lineage must not lose an unselected external candidate.
    from acquire_imdb import load_config as imdb_config
    im=imdb_config(c['imdb_config']);catalog=c.get('catalog_path',str(Path(im['work_dir'])/'catalog.sqlite3'))
    d.execute('ATTACH DATABASE ? AS reference_catalog',(catalog,))
    registrations={r['file']:json.loads(r['registration']) for r in d.execute('SELECT * FROM reference_catalog.sources')}
    batch=[]
    for r in d.execute('SELECT * FROM reference_catalog.raw_records'):
        m=registrations[r['file']];batch.append((r['source_record_id'],m['source_id'],m['sha256'],r['record_sequence'],r['byte_start'],r['byte_end'],r['raw_sha256'],r['raw_record'],m['url'],m['downloaded_at']))
        if len(batch)>=5000:d.executemany('INSERT OR IGNORE INTO source_records VALUES (?,?,?,?,?,?,?,?,?,?)',batch);batch=[]
    if batch:d.executemany('INSERT OR IGNORE INTO source_records VALUES (?,?,?,?,?,?,?,?,?,?)',batch)
    d.execute('CREATE TABLE identity_candidate_sources(movie_id TEXT,tconst TEXT,source_record_id TEXT REFERENCES source_records(source_record_id),source_role TEXT,PRIMARY KEY(movie_id,tconst,source_record_id,source_role),FOREIGN KEY(movie_id,tconst) REFERENCES work_match_candidates(movie_id,tconst))')
    for table,role in [('titles','title'),('crew','crew')]:
        d.execute(f'INSERT OR IGNORE INTO identity_candidate_sources SELECT w.movie_id,w.tconst,r.source_record_id,? FROM work_match_candidates w JOIN reference_catalog.{table} r USING(tconst)',(role,))
    d.execute("INSERT OR IGNORE INTO identity_candidate_sources SELECT w.movie_id,w.tconst,p.source_record_id,'principal_actor_context' FROM work_match_candidates w JOIN reference_catalog.principals p USING(tconst) WHERE p.category IN ('actor','actress')")
    d.execute("INSERT OR IGNORE INTO identity_candidate_sources SELECT w.movie_id,w.tconst,n.source_record_id,'actor_name_context' FROM work_match_candidates w JOIN reference_catalog.principals p USING(tconst) JOIN reference_catalog.names n USING(nconst) WHERE p.category IN ('actor','actress')")
    d.execute("INSERT OR IGNORE INTO identity_candidate_sources SELECT w.movie_id,w.tconst,n.source_record_id,'director_name_context' FROM work_match_candidates w JOIN reference_catalog.crew r USING(tconst) JOIN json_each(r.directors) j JOIN reference_catalog.names n ON j.value=n.nconst")
def cross_edition(d):
    representatives={};candidates=0
    for r in d.execute("SELECT m.movie_id,r.record_sequence,r.product_id,r.reviewer_id,r.review_time_unix,r.review_text_clean FROM product_movie m JOIN raw_reviews r INDEXED BY review_product_index ON m.asin=r.product_id WHERE m.relationship='single_work' AND r.reviewer_id IS NOT NULL AND r.review_time_unix IS NOT NULL AND r.review_text_clean IS NOT NULL AND r.review_text_clean!=''"):
        key=digest_json([r['movie_id'],r['reviewer_id'],r['review_time_unix'],r['review_text_clean']]);previous=representatives.get(key)
        if previous and previous[1]!=r['product_id']:
            if previous[2]!=r['review_text_clean']:raise ValueError('Cross-edition candidate hash collision')
            left,right=sorted([previous[0],r['record_sequence']]);cid='cross:'+digest_json([r['movie_id'],left,right])
            d.execute('INSERT OR IGNORE INTO cross_edition_candidates VALUES (?,?,?,?,?)',(cid,left,right,'manual_review','Same mapped work/reviewer/time/cleaned text across different ASIN; score/helpfulness may differ. Candidate only, all raw/logical reviews retained.'));candidates+=1
        else:representatives.setdefault(key,(r['record_sequence'],r['product_id'],r['review_text_clean']))
    return candidates
def run(c,stop_after_phase=None):
    review_cfg=load_config(ROOT/c.get('review_config','config/reviews.example.json'));tc=task4_config(c['task4_config'])
    review_path=review_root(review_cfg)/'reviews.sqlite3';root=Path(c['target_dir']);root.mkdir(parents=True,exist_ok=True)
    report_dir=Path(c['report_root'])/'task9';report_dir.mkdir(parents=True,exist_ok=True)
    with closing(ro(Path(c['work_dir'])/'integration.sqlite3')) as integration,closing(ro(review_path)) as reviews:
        if json.loads(reviews.execute("SELECT payload FROM review_state WHERE key='complete'").fetchone()[0]) is not True:raise ValueError('Complete Task8 prerequisite required')
        from acquire_imdb import load_config as imdb_config
        im=imdb_config(c['imdb_config']);catalog=c.get('catalog_path',str(Path(im['work_dir'])/'catalog.sqlite3'))
        inputs=dict(task7=required(integration,'task7'),review_fingerprint=json.loads(reviews.execute("SELECT payload FROM review_state WHERE key='input'").fetchone()[0]),catalog_sha256=hash_file(catalog),version=VERSION,implementation=hash_file(__file__),views=hash_file(ROOT/'sql/target_views.sql'),scope=c.get('scope','full'),expected_reviews=c.get('expected_reviews',7911684))
    fingerprint=digest_json(inputs);final=root/'etl.sqlite3';building=root/'etl.building.sqlite3';registration=root/'load_registration.json'
    with run_lock(root):
        if registration.exists() and json.loads(registration.read_text())['input_fingerprint']!=fingerprint:raise ValueError('Target input/rule version changed; new target directory required')
        save_json(registration,dict(input_fingerprint=fingerprint,inputs=inputs,registered_at=utc_now()))
        if final.exists():
            with closing(ro(final)) as d:
                if state(d,'ready') is not True or state(d,'input')!=fingerprint:raise ValueError('Unexpected target state')
            return dict(status='already_complete_no_insert',target_path=str(final.resolve()),input_fingerprint=fingerprint)
        with closing(sqlite3.connect(building)) as d:
            d.row_factory=sqlite3.Row
            d.execute('PRAGMA cache_size=-524288');d.execute('PRAGMA mmap_size=1073741824')
            if not state(d,'review_copy_complete'):
                logging.info('Copy complete review database using SQLite online backup')
                with closing(ro(review_path)) as source:source.backup(d,pages=8192,sleep=0.01)
                d.execute('CREATE TABLE IF NOT EXISTS target_state(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
                with d:set_state(d,'input',fingerprint);set_state(d,'review_copy_complete',True)
            if stop_after_phase=='review_copy':raise KeyboardInterrupt
            d.execute('PRAGMA foreign_keys=ON')
            if not state(d,'integration_complete'):
                logging.info('Atomic load of entities, candidates, selections and raw source records')
                with d:copy_integration(d,c,tc);set_state(d,'integration_complete',True)
                d.execute('DETACH DATABASE integration');d.execute('DETACH DATABASE movie_resolution');d.execute('DETACH DATABASE reference_catalog')
            if stop_after_phase=='integration':raise KeyboardInterrupt
            if not state(d,'graph_complete'):
                rid=uuid.uuid4().hex
                with d:
                    d.execute('INSERT INTO etl_runs VALUES (?,?,?,?,?,?)',(rid,'task9',utc_now(),None,'running',fingerprint))
                    graph(d,c,rid);d.execute("UPDATE etl_runs SET status='complete',finished_at=? WHERE run_id=?",(utc_now(),rid));set_state(d,'graph_complete',True)
            if stop_after_phase=='graph':raise KeyboardInterrupt
            logging.info('Build target indexes and queryable lineage/statistics views')
            d.executescript((ROOT/'sql/target_views.sql').read_text(encoding='utf-8'))
            if not state(d,'cross_edition_candidates_complete'):
                with d:cross_edition(d);set_state(d,'cross_edition_candidates_complete',True)
            if d.execute('PRAGMA foreign_key_check').fetchone():raise ValueError('Target foreign key violation')
            if d.execute('SELECT count(*) FROM raw_reviews').fetchone()[0]!=c.get('expected_reviews',7911684):raise ValueError('Target review count mismatch for declared scope')
            with d:set_state(d,'ready',True)
            d.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        os.replace(building,final)
    result=dict(status='complete',target_path=str(final.resolve()),input_fingerprint=fingerprint,file_size_bytes=final.stat().st_size,sha256=hash_file(final),
        semantics='SQLite equivalent relational target, preserving every original review. Raw/normalized field assertions, selection/unselected members, rule/run/times, source locators and parent-child graph. Review provenance is a queryable view; no fake human labels.')
    save_json(report_dir/'load_statistics.json',result);return result
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');p.add_argument('--stop-after-phase',choices=['review_copy','integration','graph']);a=p.parse_args();logging.basicConfig(level=logging.INFO)
    try:print(js(run(load_config(a.config),a.stop_after_phase)))
    except KeyboardInterrupt:raise SystemExit(130)
if __name__=='__main__':main()
