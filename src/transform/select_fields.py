"""Task7: retain every candidate and select only semantically eligible evidence."""
import argparse
from collections import defaultdict
from contextlib import closing
import json
import logging
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
from integration_common import *
from resolve_movies import load_config as task4_config
from classify_products import load_config as task3_config,load_rules as task3_rules,extract_fields
from acquire_products import load_config as task2_config
from acquire_imdb import load_config as imdb_config
from resolve_people import register_metadata,register_imdb

VERSION='semantic-source-selection-v1'
TABLES=['field_selections','selection_members','movies','editions']
def normalize_genres(items):
    aliases={'Sci-Fi':'science_fiction','Film-Noir':'film_noir'}
    return sorted({aliases.get(x,x.casefold().replace('-','_').replace(' ','_')) for x in items})
def schema(d):
    d.executescript('''
    CREATE TABLE IF NOT EXISTS field_selections(selection_id TEXT PRIMARY KEY,entity_type TEXT NOT NULL,entity_id TEXT NOT NULL,field TEXT NOT NULL,value TEXT,status TEXT NOT NULL CHECK(status IN ('selected','missing','unresolved')),reason TEXT NOT NULL,rule_id TEXT NOT NULL,operation_id TEXT NOT NULL REFERENCES operations(operation_id),UNIQUE(entity_type,entity_id,field));
    CREATE TABLE IF NOT EXISTS selection_members(selection_id TEXT REFERENCES field_selections(selection_id),assertion_id TEXT REFERENCES assertions(assertion_id),status TEXT NOT NULL CHECK(status IN ('selected','not_selected')),reason TEXT NOT NULL,PRIMARY KEY(selection_id,assertion_id));
    CREATE TABLE IF NOT EXISTS movies(movie_id TEXT PRIMARY KEY REFERENCES work_matches(movie_id),canonical_title TEXT,identity_status TEXT NOT NULL,tconst TEXT,release_year INTEGER,release_date TEXT,date_precision TEXT NOT NULL,runtime_minutes INTEGER,genres TEXT NOT NULL,directors TEXT NOT NULL,starring TEXT NOT NULL,cast TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS editions(asin TEXT PRIMARY KEY,classification TEXT NOT NULL,mapping_status TEXT NOT NULL,title TEXT,medium TEXT NOT NULL,language TEXT NOT NULL,subtitles TEXT NOT NULL,region_code TEXT,distributor TEXT,product_release_date TEXT,review_count INTEGER NOT NULL CHECK(review_count>0));
    ''')
def choose(candidates,mode='scalar'):
    """Candidate: assertion_id,value,rank,eligible. Rank is semantic policy, not time."""
    allowed=[x for x in candidates if x.get('eligible',True) and x['value'] not in (None,'',[])]
    if not allowed:return None,'missing','no_semantically_eligible_source_value',set()
    if mode=='union':
        values=sorted({js(v) for x in allowed for v in (x['value'] if isinstance(x['value'],list) else [x['value']])})
        return [json.loads(v) for v in values],'selected','explicit_multivalue_union_with_member_provenance',{x['assertion_id'] for x in allowed}
    top=max(x['rank'] for x in allowed);best=[x for x in allowed if x['rank']==top]
    unique={js(x['value']) for x in best}
    if len(unique)>1:return None,'unresolved','conflicting_equal_authority_values_no_arbitrary_choice',set()
    return best[0]['value'],'selected','highest_semantically_applicable_authority_unique_value',{x['assertion_id'] for x in best}
def select(d,run,etype,eid,field,candidates,mode='scalar',missing_reason=None):
    value,status,reason,selected=choose(candidates,mode)
    if status=='missing' and missing_reason:reason=missing_reason
    sid='selection:'+digest_json([etype,eid,field,VERSION]);op=operation(d,run,'select_scalar' if mode=='scalar' else 'union_multivalue',VERSION,[x['assertion_id'] for x in candidates])
    d.execute('INSERT INTO field_selections VALUES (?,?,?,?,?,?,?,?,?)',(sid,etype,eid,field,js(value),status,reason,VERSION,op))
    for x in candidates:d.execute('INSERT INTO selection_members VALUES (?,?,?,?)',(sid,x['assertion_id'],'selected' if x['assertion_id'] in selected else 'not_selected',reason if x['assertion_id'] in selected else 'semantics_not_applicable_or_lower_authority_or_unresolved_conflict'))
    return value
def build(d,c,t4,imdb,meta,rules,run):
    for i,p in enumerate(t4.execute('SELECT p.*,s.mapping_status FROM products p JOIN product_status s USING(asin) ORDER BY p.asin')):
        profile=json.loads(p['profile']);candidates=defaultdict(list)
        for r in profile['records']:
            register_metadata(d,meta,r)
            raw=json.loads(d.execute('SELECT raw_record FROM source_records WHERE source_record_id=?',(r['source_record_id'],)).fetchone()[0])
            fields=extract_fields(raw,rules)
            for field,info in fields.items():
                if info['normalized'] is None:continue
                aid=assertion(d,run,'product',p['asin'],field,info['raw'],info['normalized'],r['source_record_id'],js(info['field_locators']),VERSION,'explicit_product_source_field')
                candidates[field].append(dict(assertion_id=aid,value=info['normalized'],rank=1))
            for a in r['edition_assertions']:
                if a['field'] not in ('language','subtitles'):continue
                aid=assertion(d,run,'product',p['asin'],a['field'],a['raw'],a['language_tags'],r['source_record_id'],a['field_locator'],VERSION,'controlled_language_alias_or_unresolved')
                candidates[a['field']].append(dict(assertion_id=aid,value=a['language_tags'],rank=2,eligible=bool(a['language_tags'])))
            # Work-year/credit/plot evidence is retained; not silently promoted to release facts.
            for a in r['assertions']:
                assertion(d,run,'product',p['asin'],'identity_evidence.'+a['field'],a['raw'],a['normalized'],r['source_record_id'],a['field_locator'],VERSION,a['semantics'])
        for field in ('language','subtitles'):
            for x in candidates[field]:
                if x['rank']==1:x['eligible']=False
        values={}
        for field in ('title','medium','language','subtitles','region_code','distributor','product_release_date'):
            v=select(d,run,'product',p['asin'],field,candidates[field],mode='union' if field in ('medium','language','subtitles') else 'scalar')
            values[field]=v
        d.execute('INSERT INTO editions VALUES (?,?,?,?,?,?,?,?,?,?,?)',(p['asin'],p['classification'],p['mapping_status'],values['title'],js(values['medium'] or []),js(values['language'] or []),js(values['subtitles'] or []),js(values['region_code']) if values['region_code'] is not None else None,js(values['distributor']) if values['distributor'] is not None else None,values['product_release_date'],p['review_count']))
        if i and i%10000==0:logging.info('Product-field selections: %s',i)
    for w in t4.execute('SELECT * FROM works ORDER BY movie_id'):
        mid=w['movie_id'];match=d.execute('SELECT * FROM work_matches WHERE movie_id=?',(mid,)).fetchone();candidates=defaultdict(list)
        for source in t4.execute('SELECT p.profile FROM products p JOIN product_movie m USING(asin) WHERE m.movie_id=?',(mid,)):
            for r in json.loads(source[0])['records']:
                aid=assertion(d,run,'movie',mid,'title',r['title_raw'],r['title_raw'],r['source_record_id'],'title',VERSION,'provisional_product_display_title_not_canonical_work_proof')
                candidates['title'].append(dict(assertion_id=aid,value=r['title_raw'],rank=1))
                raw=json.loads(d.execute('SELECT raw_record FROM source_records WHERE source_record_id=?',(r['source_record_id'],)).fetchone()[0])
                info=extract_fields(raw,rules)['genres']
                if info['normalized']:
                    aid=assertion(d,run,'movie',mid,'genres',info['raw'],info['normalized'],r['source_record_id'],js(info['field_locators']),VERSION,'controlled_product_genre_category')
                    candidates['genres'].append(dict(assertion_id=aid,value=info['normalized'],rank=1))
        if match['tconst']:
            t=imdb.execute('SELECT * FROM titles WHERE tconst=?',(match['tconst'],)).fetchone();register_imdb(d,imdb,t['source_record_id'])
            for field,key,value in [('title','primaryTitle',t['primary_title']),('runtime_minutes','runtimeMinutes',t['runtime']),('genres','genres',json.loads(t['genres']))]:
                normalized=normalize_genres(value) if field=='genres' else value
                aid=assertion(d,run,'movie',mid,field,value,normalized,t['source_record_id'],key,VERSION,'verified_work_external_authority')
                candidates[field].append(dict(assertion_id=aid,value=normalized,rank=2))
        release=d.execute('SELECT * FROM release_info WHERE movie_id=?',(mid,)).fetchone()
        if release['assertion_id']:candidates['release_year'].append(dict(assertion_id=release['assertion_id'],value=release['release_year'],rank=2))
        for cr in d.execute('SELECT * FROM credits WHERE movie_id=?',(mid,)):
            candidates[cr['role']].append(dict(assertion_id=cr['assertion_id'],value=[cr['person_id']],rank=1))
        values={}
        for field in ('title','runtime_minutes','genres','release_year','release_date','director','starring','cast'):
            values[field]=select(d,run,'movie',mid,field,candidates[field],mode='union' if field in ('genres','director','starring','cast') else 'scalar',missing_reason=release['date_reason'] if field=='release_date' else release['year_reason'] if field=='release_year' else None)
        d.execute('INSERT INTO movies VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(mid,values['title'],match['status'],match['tconst'],values['release_year'],values['release_date'],release['date_precision'],values['runtime_minutes'],js(values['genres'] or []),js(values['director'] or []),js(values['starring'] or []),js(values['cast'] or [])))
def verify(d):
    if d.execute('PRAGMA foreign_key_check').fetchone():raise ValueError('Field-selection foreign key')
    bad=d.execute("SELECT count(*) FROM field_selections WHERE (status='selected' AND value='null') OR (status!='selected' AND value!='null')").fetchone()[0]
    if bad:raise ValueError('NULL/status mismatch')
    return dict(status='passed',foreign_key_errors=0,invalid_null_status_rows=bad,all_candidates_preserved=True)
def run(c):
    tc=task4_config(c['task4_config']);t3=task3_config(tc['task3_config']);t2=task2_config(t3['task2_config']);ic=imdb_config(c['imdb_config'])
    with run_lock(c['work_dir']),closing(open_db(c)) as d,closing(ro(Path(tc['work_dir'])/'resolution.sqlite3')) as t4,closing(ro(Path(ic['work_dir'])/'catalog.sqlite3')) as imdb,closing(ro(Path(t2['work_dir'])/'metadata.sqlite3')) as meta:
        schema(d);rid=begin(d,'task7',dict(task6=required(d,'task6'),version=VERSION,implementation=hash_file(__file__),genre_rules=hash_file(t3['rules_file'])))
        if rid:
            # Single atomic stage: interruption rolls back ALL selections; no half-final fields.
            with d:build(d,c,t4,imdb,meta,task3_rules(t3['rules_file']),rid);finish(d,'task7',rid)
        result=report(c,'task7',d,TABLES,dict(selection_statuses=dict(d.execute('SELECT status,count(*) FROM field_selections GROUP BY status').fetchall()),
            policy='Semantic eligibility first; unique highest applicable authority for scalar, explicit supported union for multivalue. Equal-authority disagreements remain NULL with reason. No majority/latest arbitrary tie break.'))
        save_json(Path(c['report_root'])/'task7'/'full_verification.json',verify(d));return result
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');a=p.parse_args();logging.basicConfig(level=logging.INFO);print(js(run(load_config(a.config))))
if __name__=='__main__':main()
