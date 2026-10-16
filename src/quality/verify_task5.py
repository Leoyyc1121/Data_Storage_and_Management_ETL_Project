"""Replay all contextual external-work matches and retained source hashes."""
import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
from resolve_people import candidate_evidence,verify as structural,ROLE_FIELDS,credited_spelling,credit_name_issue,contextual_people
from resolve_movies import load_config as task4_config
from acquire_imdb import load_config as imdb_config

def verify(c):
    tc=task4_config(c['task4_config']);ic=imdb_config(c['imdb_config']);count=0
    with closing(ro(Path(c['work_dir'])/'integration.sqlite3')) as d,closing(ro(Path(tc['work_dir'])/'resolution.sqlite3')) as t4,closing(ro(Path(ic['work_dir'])/'catalog.sqlite3')) as imdb:
        required(d,'task5');structural(d)
        for row in d.execute('SELECT * FROM source_records'):
            if hashlib.sha256(row['raw_record']).hexdigest()!=row['raw_sha256']:raise ValueError('Source raw hash mismatch')
            if row['byte_end']-row['byte_start']!=len(row['raw_record']):raise ValueError('Source offset mismatch')
        expected_roles=set()
        for w in d.execute('SELECT * FROM work_matches ORDER BY movie_id'):
            profiles=[json.loads(r[0]) for r in t4.execute('SELECT p.profile FROM products p JOIN product_movie m USING(asin) WHERE m.movie_id=?',(w['movie_id'],))]
            for p in profiles:
                for record in p['records']:
                    for a in record['assertions']:
                        if a['field'] in ROLE_FIELDS:
                            source_raw=d.execute('SELECT raw_record FROM source_records WHERE source_record_id=?',(record['source_record_id'],)).fetchone()[0]
                            expected_roles.add((w['movie_id'],ROLE_FIELDS[a['field']],record['source_record_id'],a['field_locator'],credited_spelling(a,source_raw) or a['raw']))
            expected=set()
            for title in {v for p in profiles for v in p['titles']}:expected.update(r[0] for r in imdb.execute('SELECT tconst FROM title_lookup WHERE normalized_title=?',(title,)))
            actual=list(d.execute('SELECT * FROM work_match_candidates WHERE movie_id=?',(w['movie_id'],)))
            if expected!={r['tconst'] for r in actual}:raise ValueError('Missing work candidate')
            qualified=[]
            for r in actual:
                title=imdb.execute('SELECT * FROM titles WHERE tconst=?',(r['tconst'],)).fetchone();crew=imdb.execute('SELECT * FROM crew WHERE tconst=?',(r['tconst'],)).fetchone()
                directors=[dict(n) for key in json.loads(crew['directors']) if (n:=imdb.execute('SELECT * FROM names WHERE nconst=?',(key,)).fetchone())] if crew else []
                actors=[dict(n) for n in imdb.execute("SELECT DISTINCT n.* FROM principals p JOIN names n USING(nconst) WHERE p.tconst=? AND p.category IN ('actor','actress')",(r['tconst'],))]
                e=candidate_evidence(profiles,title,directors,actors)
                if e!=json.loads(r['evidence']) or e['qualified']!=bool(r['qualified']):raise ValueError('Work evidence mismatch')
                if e['qualified']:qualified.append(r['tconst'])
                count+=1
            if w['tconst']!=(qualified[0] if len(qualified)==1 else None):raise ValueError('Wrong unique match selection')
        actual_roles={(r['movie_id'],r['role'],json.loads(r['evidence'])['source_record_id'],json.loads(r['evidence'])['field_locator'],r['raw_name']) for r in d.execute('SELECT * FROM person_decisions')}
        if expected_roles!=actual_roles:raise ValueError('Explicit source role schema contract not completely preserved')
        for decision in d.execute('SELECT * FROM person_decisions'):
            e=json.loads(decision['evidence']);issue=credit_name_issue(decision['raw_name']) if e['original_spelling_resolved'] else 'unresolved_span'
            if issue:
                if decision['person_id'] is not None:raise ValueError('Non-person/compound span created an entity')
                continue
            options=[]
            for cr in d.execute("SELECT DISTINCT p.person_id,p.nconst,p.canonical_name FROM credits c JOIN people p USING(person_id) WHERE c.movie_id=? AND c.role=? AND c.scope LIKE 'IMDb%'",(decision['movie_id'],'director' if decision['role']=='director' else 'cast')):
                options.append((dict(nconst=cr['nconst'],primary_name=cr['canonical_name']),dict(person_id=cr['person_id'])))
            matches=contextual_people(decision['raw_name'],options)
            if len(matches)==1 and (decision['person_id']!=matches[0][1]['person_id'] or decision['status']!='auto_context_bind'):raise ValueError('Contextual alias identity replay mismatch')
            if len(matches)!=1 and decision['status']=='auto_context_bind':raise ValueError('Ambiguous name variants incorrectly merged')
        queue=json.loads((Path(c['work_dir'])/'person_evaluation_queue.json').read_text(encoding='utf-8'))
        ids={r[0] for r in d.execute('SELECT pair_id FROM person_evaluation_pairs')}
        if ids!={p['pair_id'] for p in queue['pairs']}:raise ValueError('Queue mismatch')
        if any(p['label'] is not None for p in queue['pairs']):raise ValueError('Automatic queue must not generate human labels')
        development=set(c.get('development_source_records',[]))
        if any(p[side]['source_record_id'] in development for p in queue['pairs'] for side in ('left','right')):raise ValueError('Development evidence leaked into person holdout')
        result=dict(status='passed_full_evidence_replay_not_human_accuracy',all_candidate_matches_replayed=count,
            raw_source_hashes_verified=d.execute('SELECT count(*) FROM source_records').fetchone()[0],all_explicit_source_role_assertions_preserved=len(actual_roles),human_queue_pairs=len(ids),human_labels_completed=0)
        return result
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');a=p.parse_args();c=load_config(a.config);r=verify(c);save_json(Path(c['report_root'])/'task5'/'full_verification.json',r);print(js(r))
if __name__=='__main__':main()
