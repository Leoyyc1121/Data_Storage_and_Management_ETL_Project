"""Source-backed development examples, explicitly NOT independent human labels."""
from collections import defaultdict
from contextlib import closing
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
from acquire_imdb import load_config as imdb_config
from resolve_people import norm_name
from resolve_movies import load_config as movie_config,pair_id
def run(c):
    ic=imdb_config(c['imdb_config']);tc=movie_config(c['task4_config'])
    pairs=[('6305235074','6305235120'),('6304853653','6305235074'),('B000WTVZGC','B003ZZ7TPG'),('B000EWBNYG','B001O7SWJ4'),('6304856245','6305235074')]
    movie_examples=[]
    with closing(ro(Path(tc['work_dir'])/'resolution.sqlite3')) as d:
        for a,b in pairs:
            r=d.execute('SELECT * FROM pairs WHERE pair_id=?',(pair_id(a,b),)).fetchone()
            if not r or r['final_decision']!='reject':raise ValueError('Requested actual kept-separate development case missing')
            ev=json.loads(r['evidence']);movie_examples.append(dict(asins=sorted([a,b]),rule_id=r['rule_id'],source_record_ids=ev['facts']['left_source_records']+ev['facts']['right_source_records']))
    save_json(ROOT/'reports/task4/kept_separate_case_audit.json',dict(status='five_actual_sequel_pairs_source_inspected_not_human_gold',examples=movie_examples,franchises=3,scope='Five source-supported pair groups from three franchises, not five different franchises; excludes teaching volumes and TV episode examples.'))
    with closing(ro(Path(ic['work_dir'])/'catalog.sqlite3')) as d:
        groups=defaultdict(list)
        for r in d.execute('SELECT * FROM names WHERE birth_year IS NOT NULL'):groups[norm_name(r['primary_name'])].append(dict(r))
        examples=[]
        for name,rows in sorted(groups.items()):
            for left,right in zip(rows,rows[1:]):
                if left['birth_year']==right['birth_year'] or set(json.loads(left['known_for']))&set(json.loads(right['known_for'])):continue
                credits=[]
                for p in [left,right]:credits.append([dict(r) for r in d.execute("SELECT p.tconst,p.category,p.source_record_id,t.release_year FROM principals p JOIN titles t USING(tconst) WHERE p.nconst=? AND p.category IN ('actor','actress','director') ORDER BY p.tconst LIMIT 5",(p['nconst'],))])
                if not all(credits):continue
                examples.append(dict(left=left,right=right,credits=credits,decision='keep_distinct_external_context_identities',reason='same_normalized_name_but_distinct_external_ID_birth_year_and_nonoverlapping_credit_context'))
                break
            if len(examples)>=5:break
    path=Path(c['work_dir'])/'person_kept_separate_examples.json';save_json(path,dict(partition='development_source_examples_not_human_evaluation',examples=examples))
    report=dict(status='source_context_examples_available' if len(examples)>=5 else 'insufficient_cases',count=len(examples),local_evidence_packet=str(path.resolve()),sha256=hash_file(path),
        scope='Same-name disambiguation examples from retained candidate catalog, not all loaded target people or independently human-confirmed homonyms. Birth/year/ID/credit context reviewed by code; no name-only merge.',human_gold=False)
    save_json(ROOT/'reports/task5/kept_separate_case_audit.json',report);return report
if __name__=='__main__':print(js(run(load_config())))
