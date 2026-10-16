"""Task5: verify work identity before binding people to contextual IMDb credits.

Name-only and initials-only matches never merge people. Ambiguous credits stay
source-context provisional entities. IMDb principals are not complete cast/stars.
"""
from __future__ import annotations
import argparse
from collections import defaultdict
from contextlib import closing
import json
from pathlib import Path
import re
import sys
import unicodedata

sys.path.insert(0,str(Path(__file__).resolve().parent))
from integration_common import *
from resolve_movies import load_config as task4_config
from acquire_imdb import load_config as imdb_config

VERSION='people-context-v1'
TABLES=['work_matches','work_match_candidates','people','person_aliases','credits','person_decisions','person_evaluation_pairs']
def norm_name(raw):
    value=''.join(ch for ch in unicodedata.normalize('NFKD',raw.casefold()) if not unicodedata.combining(ch))
    return ' '.join(re.findall(r'[^\W\d_]+',value))
def full_name(raw):
    parts=norm_name(raw).split();return len(parts)>=2 and sum(len(x)>1 for x in parts)>=2
def compatible(a,b):return full_name(a) and full_name(b) and norm_name(a)==norm_name(b)
def candidate_evidence(profiles,title,directors,actors):
    vals=defaultdict(set);hints=set()
    for p in profiles:
        hints.update(p['title_year_hints'])
        for k,v in p['values'].items():vals[k].update(v)
    source_dirs=vals['directors']|vals['director_mentions']
    source_actors=vals['starring']|vals['actor_context']
    dirs=sorted({n['nconst'] for n in directors if any(compatible(x,n['primary_name']) for x in source_dirs)})
    cast=sorted({n['nconst'] for n in actors if any(compatible(x,n['primary_name']) for x in source_actors)})
    years=vals['work_years']|hints
    year=bool(title['release_year'] is not None and title['release_year'] in years)
    conflict=bool(years and title['release_year'] is not None and all(abs(title['release_year']-x)>1 for x in years))
    runtime=bool(title['runtime'] is not None and any(abs(title['runtime']-x)<=3 for x in vals['runtimes']))
    qualified=not conflict and ((bool(dirs) and (year or runtime or bool(cast))) or len(cast)>=2 and (year or runtime) or len(cast)>=3)
    return dict(qualified=bool(qualified),matched_director_ids=dirs,matched_actor_ids=cast,year_agrees=year,runtime_agrees=runtime,year_conflicts=conflict,
        rule='exact_source_title_plus_contextual_people_and_independent_year_runtime_or_cast',
        source_record_ids=sorted({r['source_record_id'] for p in profiles for r in p['records']}),
        title_source_record_id=title['source_record_id'],source_year_candidates=sorted(years),not_name_only=True)
def schema(d):
    d.executescript('''
    CREATE TABLE IF NOT EXISTS work_matches(movie_id TEXT PRIMARY KEY,tconst TEXT,status TEXT NOT NULL,reason TEXT NOT NULL,evidence TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS work_match_candidates(movie_id TEXT NOT NULL REFERENCES work_matches(movie_id),tconst TEXT NOT NULL,qualified INTEGER NOT NULL CHECK(qualified IN (0,1)),evidence TEXT NOT NULL,PRIMARY KEY(movie_id,tconst));
    CREATE TABLE IF NOT EXISTS people(person_id TEXT PRIMARY KEY,nconst TEXT UNIQUE,canonical_name TEXT NOT NULL,identity_status TEXT NOT NULL,birth_year INTEGER,death_year INTEGER,name_source_record_id TEXT REFERENCES source_records(source_record_id));
    CREATE TABLE IF NOT EXISTS person_aliases(alias_id TEXT PRIMARY KEY,person_id TEXT NOT NULL REFERENCES people(person_id),raw_name TEXT NOT NULL,normalized_name TEXT NOT NULL,movie_id TEXT NOT NULL,role TEXT NOT NULL,source_record_id TEXT NOT NULL REFERENCES source_records(source_record_id),assertion_id TEXT NOT NULL REFERENCES assertions(assertion_id));
    CREATE INDEX IF NOT EXISTS alias_norm ON person_aliases(normalized_name);
    CREATE TABLE IF NOT EXISTS credits(credit_id TEXT PRIMARY KEY,movie_id TEXT NOT NULL,person_id TEXT NOT NULL REFERENCES people(person_id),role TEXT NOT NULL CHECK(role IN ('director','starring','cast')),credit_order INTEGER,raw_name TEXT NOT NULL,source_record_id TEXT NOT NULL REFERENCES source_records(source_record_id),assertion_id TEXT NOT NULL REFERENCES assertions(assertion_id),scope TEXT NOT NULL);
    CREATE INDEX IF NOT EXISTS credit_movie_role ON credits(movie_id,role);
    CREATE INDEX IF NOT EXISTS credit_person ON credits(person_id);
    CREATE TABLE IF NOT EXISTS person_decisions(decision_id TEXT PRIMARY KEY,movie_id TEXT,raw_name TEXT,role TEXT,person_id TEXT REFERENCES people(person_id),status TEXT,reason TEXT,evidence TEXT);
    CREATE TABLE IF NOT EXISTS person_evaluation_pairs(pair_id TEXT PRIMARY KEY,left_alias_id TEXT NOT NULL REFERENCES person_aliases(alias_id),right_alias_id TEXT NOT NULL REFERENCES person_aliases(alias_id),stratum TEXT NOT NULL);
    ''')
def register_imdb(d,imdb,sid):
    if d.execute('SELECT 1 FROM source_records WHERE source_record_id=?',(sid,)).fetchone():return
    r=imdb.execute('SELECT * FROM raw_records WHERE source_record_id=?',(sid,)).fetchone()
    m=json.loads(imdb.execute('SELECT registration FROM sources WHERE file=?',(r['file'],)).fetchone()[0])
    d.execute('INSERT INTO source_records VALUES (?,?,?,?,?,?,?,?,?,?)',(sid,m['source_id'],m['sha256'],r['record_sequence'],r['byte_start'],r['byte_end'],r['raw_sha256'],r['raw_record'],m['url'],m['downloaded_at']))
def register_metadata(d,source,record):
    sid=record['source_record_id']
    if d.execute('SELECT 1 FROM source_records WHERE source_record_id=?',(sid,)).fetchone():return
    r=source.execute('SELECT * FROM records WHERE source_id=? AND record_sequence=?',(record['source_id'],record['record_sequence'])).fetchone()
    if not r:raise ValueError('Missing retained product raw record')
    d.execute('INSERT INTO source_records VALUES (?,?,?,?,?,?,?,?,?,?)',(sid,r['source_id'],r['archive_sha256'],r['record_sequence'],r['byte_start'],r['byte_end'],r['raw_record_sha256'],r['raw_record'],'https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_v2/metaFiles2/meta_Movies_and_TV.json.gz',r['source_downloaded_at']))
def add_credit(d,run,mid,raw,role,order,sid,locator,person,scope,parents=()):
    pid=person['person_id']
    aid=assertion(d,run,'movie',mid,role,raw,pid,sid,locator,VERSION,scope,parents)
    key=digest_json([mid,role,sid,locator,raw,order])
    d.execute('INSERT OR IGNORE INTO credits VALUES (?,?,?,?,?,?,?,?,?)',('credit:'+key,mid,pid,role,order,raw,sid,aid,scope))
    alias='alias:'+digest_json([pid,raw,mid,role,sid])
    d.execute('INSERT OR IGNORE INTO person_aliases VALUES (?,?,?,?,?,?,?,?)',(alias,pid,raw,norm_name(raw),mid,role,sid,aid))
    return alias
def imdb_person(d,imdb,n):
    register_imdb(d,imdb,n['source_record_id']);pid=stable('person:','imdb:'+n['nconst'])
    d.execute('INSERT OR IGNORE INTO people VALUES (?,?,?,?,?,?,?)',(pid,n['nconst'],n['primary_name'],'verified_external_context',n['birth_year'],n['death_year'],n['source_record_id']))
    return dict(person_id=pid)
def build(d,c,t4,imdb,meta,run):
    for w in t4.execute('SELECT * FROM works ORDER BY movie_id'):
        mid=w['movie_id'];profiles=[json.loads(r[0]) for r in t4.execute('SELECT p.profile FROM products p JOIN product_movie m USING(asin) WHERE m.movie_id=? ORDER BY p.asin',(mid,))]
        titles=sorted({v for p in profiles for v in p['titles']});candidate_ids=set()
        for v in titles:candidate_ids.update(r[0] for r in imdb.execute('SELECT tconst FROM title_lookup WHERE normalized_title=?',(v,)))
        candidates=[]
        for tid in sorted(candidate_ids):
            title=dict(imdb.execute('SELECT * FROM titles WHERE tconst=?',(tid,)).fetchone())
            crew=imdb.execute('SELECT * FROM crew WHERE tconst=?',(tid,)).fetchone()
            directors=[dict(n) for key in json.loads(crew['directors']) if (n:=imdb.execute('SELECT * FROM names WHERE nconst=?',(key,)).fetchone())] if crew else []
            actors=[dict(n) for n in imdb.execute("SELECT DISTINCT n.* FROM principals p JOIN names n USING(nconst) WHERE p.tconst=? AND p.category IN ('actor','actress')",(tid,))]
            e=candidate_evidence(profiles,title,directors,actors);candidates.append((tid,e))
        good=[(tid,e) for tid,e in candidates if e['qualified']]
        tid=good[0][0] if len(good)==1 else None
        status='verified_multifield' if tid else 'ambiguous' if len(good)>1 else 'unresolved'
        match=dict(selected_candidate=good[0][1] if tid else None,all_candidate_ids=sorted(candidate_ids),original_work_status=w['status'])
        d.execute('INSERT INTO work_matches VALUES (?,?,?,?,?)',(mid,tid,status,'unique_multifield_candidate' if tid else 'multiple_qualified_candidates' if good else 'insufficient_work_identity_evidence',js(match)))
        for key,e in candidates:d.execute('INSERT INTO work_match_candidates VALUES (?,?,?,?)',(mid,key,int(e['qualified']),js(e)))
        for p in profiles:
            for r in p['records']:register_metadata(d,meta,r)
        credit_people=defaultdict(list)
        if tid:
            title=imdb.execute('SELECT * FROM titles WHERE tconst=?',(tid,)).fetchone();register_imdb(d,imdb,title['source_record_id'])
            match_op=operation(d,run,'match_work_before_person',VERSION,[title['source_record_id']]+good[0][1]['source_record_ids'])
            crew=imdb.execute('SELECT * FROM crew WHERE tconst=?',(tid,)).fetchone()
            if crew:
                register_imdb(d,imdb,crew['source_record_id'])
                for key in json.loads(crew['directors']):
                    n=imdb.execute('SELECT * FROM names WHERE nconst=?',(key,)).fetchone()
                    if n:
                        person=imdb_person(d,imdb,n);credit_people['director'].append((dict(n),person))
                        add_credit(d,run,mid,n['primary_name'],'director',None,crew['source_record_id'],'directors:'+key,person,'IMDb explicit director list; order not asserted',[match_op,n['source_record_id']])
            for cr in imdb.execute("SELECT * FROM principals WHERE tconst=? AND category IN ('actor','actress') ORDER BY ordering",(tid,)):
                n=imdb.execute('SELECT * FROM names WHERE nconst=?',(cr['nconst'],)).fetchone()
                if n:
                    register_imdb(d,imdb,cr['source_record_id']);person=imdb_person(d,imdb,n);credit_people['cast'].append((dict(n),person))
                    add_credit(d,run,mid,n['primary_name'],'cast',cr['ordering'],cr['source_record_id'],'nconst',person,'IMDb principal cast only; not complete cast or starring billing',[match_op,n['source_record_id']])
        for p in profiles:
            for r in p['records']:
                for a in r['assertions']:
                    if a['field'] not in ('directors','starring'):continue
                    role='director' if a['field']=='directors' else 'starring';raw=a['normalized']
                    possible=[(n,per) for n,per in credit_people['director' if role=='director' else 'cast'] if compatible(raw,n['primary_name'])]
                    if len(possible)==1:person=possible[0][1];reason='same_verified_work_role_full_name_unique_external_credit'
                    else:
                        pid=stable('person:','unresolved:'+digest_json([r['source_record_id'],role,raw]));person=dict(person_id=pid)
                        d.execute('INSERT OR IGNORE INTO people VALUES (?,?,?,?,?,?,?)',(pid,None,raw,'provisional_source_context',None,None,r['source_record_id']))
                        reason='ambiguous_or_missing_verified_work_role_person_context'
                    add_credit(d,run,mid,raw,role,None,r['source_record_id'],a['field_locator'],person,'explicit product-source role; original raw prose retained')
                    d.execute('INSERT INTO person_decisions VALUES (?,?,?,?,?,?,?,?)',('pd:'+digest_json([mid,role,r['source_record_id'],a['field_locator'],raw]),mid,raw,role,person['person_id'],'auto_context_bind' if len(possible)==1 else 'unresolved',reason,js(dict(tconst=tid,source_record_id=r['source_record_id'],candidate_nconsts=[n['nconst'] for n,_ in possible]))))
    aliases=defaultdict(list);by_name=defaultdict(list)
    for a in d.execute('SELECT * FROM person_aliases ORDER BY alias_id'):
        aliases[a['person_id']].append(dict(a));by_name[a['normalized_name']].append(dict(a))
    pairs={}
    # Bounded deterministic representatives, not exhaustive pair enumeration.
    for group in aliases.values():
        for a,b in zip(group,group[1:]):pairs[digest_json(sorted([a['alias_id'],b['alias_id']]))]=(a,b,'same_external_context_candidate')
    for group in by_name.values():
        representatives={a['person_id']:a for a in group}
        values=sorted(representatives.values(),key=lambda x:x['alias_id'])
        for a,b in zip(values,values[1:]):pairs[digest_json(sorted([a['alias_id'],b['alias_id']]))]=(a,b,'same_name_different_context_candidate')
    chosen=[]
    for stratum in sorted({v[2] for v in pairs.values()}):
        chosen.extend(sorted([(k,*v) for k,v in pairs.items() if v[2]==stratum],key=lambda x:digest_json(['person-holdout-v1',x[0]]))[:c['evaluation_pairs']//2])
    chosen_ids={x[0] for x in chosen}
    chosen.extend(sorted([(k,*v) for k,v in pairs.items() if k not in chosen_ids],key=lambda x:digest_json(['person-holdout-v1',x[0]]))[:max(0,c['evaluation_pairs']-len(chosen))])
    queue=[]
    for key,a,b,stratum in chosen:
        d.execute('INSERT INTO person_evaluation_pairs VALUES (?,?,?,?)',(key,a['alias_id'],b['alias_id'],stratum))
        queue.append(dict(pair_id=key,stratum=stratum,left={k:v for k,v in a.items() if k!='person_id'},right={k:v for k,v in b.items() if k!='person_id'},label=None,labeled_by=None,labeler_type=None,labeled_at=None,labeling_evidence=None,rationale=None))
    save_json(Path(c['work_dir'])/'person_evaluation_queue.json',dict(partition='independent_human_evaluation',predictions_hidden=True,pairs=queue,development_pairs=[]))
def verify(d):
    if d.execute('PRAGMA foreign_key_check').fetchone():raise ValueError('Foreign keys')
    for r in d.execute('SELECT * FROM work_matches WHERE tconst IS NOT NULL'):
        good=list(d.execute('SELECT * FROM work_match_candidates WHERE movie_id=? AND qualified=1',(r['movie_id'],)))
        if len(good)!=1 or good[0]['tconst']!=r['tconst']:raise ValueError('Ambiguous work incorrectly selected')
    for p in d.execute('SELECT * FROM people WHERE nconst IS NOT NULL'):
        if p['person_id']!=stable('person:','imdb:'+p['nconst']):raise ValueError('Unstable person identity')
    return dict(status='passed_structural_not_human_accuracy',foreign_key_errors=0,work_match_uniqueness=True,person_ids_stable=True)
def run(c):
    t=task4_config(c['task4_config']);ic=imdb_config(c['imdb_config'])
    from classify_products import load_config as task3_config
    from acquire_products import load_config as task2_config
    tc=task2_config(task3_config(t['task3_config'])['task2_config'])
    with run_lock(c['work_dir']),closing(open_db(c)) as d,closing(ro(Path(t['work_dir'])/'resolution.sqlite3')) as t4,closing(ro(Path(ic['work_dir'])/'catalog.sqlite3')) as imdb,closing(ro(Path(tc['work_dir'])/'metadata.sqlite3')) as meta:
        schema(d)
        inputs=dict(version=VERSION,implementation=hash_file(__file__),common_implementation=hash_file(ROOT/'src/transform/integration_common.py'),evaluation_pairs=c['evaluation_pairs'],task4_state=[tuple(r) for r in t4.execute('SELECT * FROM state ORDER BY key')],imdb_state=[tuple(r) for r in imdb.execute('SELECT * FROM state ORDER BY key')])
        rid=begin(d,'task5',inputs)
        if rid:
            with d:build(d,c,t4,imdb,meta,rid);finish(d,'task5',rid)
        result=report(c,'task5',d,TABLES,dict(work_matches=dict(d.execute('SELECT status,count(*) FROM work_matches GROUP BY status').fetchall()),people_status=dict(d.execute('SELECT identity_status,count(*) FROM people GROUP BY identity_status').fetchall()),
            human_evaluation=dict(status='awaiting_independent_human_labels',required_minimum=100,labeled=0,precision=None,recall=None,f1=None,queue_path=str(Path(c['work_dir'])/'person_evaluation_queue.json')),
            limitations=['No human labeling supplied; independent evaluation NOT completed.','Source aliases retained; no assertion that all cast or full starring billing was obtained.','Five alias-merge and five homonym example acceptance requires separate evidence review.']))
        save_json(Path(c['report_root'])/'task5'/'full_verification.json',verify(d));return result
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');a=p.parse_args()
    print(js(run(load_config(a.config))))
if __name__=='__main__':main()
