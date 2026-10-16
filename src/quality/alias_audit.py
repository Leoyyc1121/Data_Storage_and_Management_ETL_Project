"""Audit substantive context-supported person aliases; raw examples stay local."""
from collections import defaultdict
from contextlib import closing
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/transform'));sys.path.insert(0,str(ROOT/'src/quality'))
from integration_common import *
from resolve_people import name_relation

def audit(c):
    groups=defaultdict(list)
    with closing(ro(Path(c['work_dir'])/'integration.sqlite3')) as d:
        for row in d.execute("SELECT a.*,p.nconst,p.canonical_name,w.tconst FROM person_aliases a JOIN people p USING(person_id) JOIN work_matches w USING(movie_id) WHERE p.nconst IS NOT NULL AND w.status='verified_multifield'"):
            relation=name_relation(row['raw_name'],row['canonical_name'])
            if relation!='contextual_middle_name_or_surname_expansion':continue
            decision=d.execute("SELECT * FROM person_decisions WHERE person_id=? AND movie_id=? AND raw_name=? AND status='auto_context_bind' AND json_extract(evidence,'$.source_record_id')=?",(row['person_id'],row['movie_id'],row['raw_name'],row['source_record_id'])).fetchone()
            if not decision:raise ValueError('Alias lacks independently matched work/role decision')
            e=json.loads(decision['evidence'])
            if len(e['candidate_nconsts'])!=1 or e['candidate_nconsts'][0]!=row['nconst']:raise ValueError('Alias candidate is not unique')
            external=[dict(x) for x in d.execute("SELECT c.source_record_id,c.assertion_id,p.name_source_record_id FROM credits c JOIN people p USING(person_id) WHERE c.person_id=? AND c.movie_id=? AND c.scope LIKE 'IMDb%'",(row['person_id'],row['movie_id']))]
            if not external:raise ValueError('Alias lacks actual external work credit')
            groups[row['person_id']].append(dict(row,relation=relation,decision=dict(decision),external_credits=external))
        excluded=d.execute('SELECT count(*) FROM person_decisions WHERE person_id IS NULL').fetchone()[0]
        queue=json.loads((Path(c['work_dir'])/'person_evaluation_queue.json').read_text(encoding='utf-8'))
    packet=Path(c['work_dir'])/'substantive_alias_evidence.json'
    save_json(packet,dict(scope='source_supported_rule_cases_NOT_HUMAN_gold',groups=dict(groups)))
    result=dict(status='passed_source_supported_examples_not_human_accuracy' if len(groups)>=5 else 'insufficient_real_alias_cases',
        substantive_person_groups=len(groups),required=5,alias_assertions=sum(map(len,groups.values())),
        excluded_nonperson_or_unresolved_spans=excluded,case_packet=dict(path=str(packet),sha256=hash_file(packet),bytes=packet.stat().st_size),
        case_person_ids=sorted(groups),evaluation_queue_pairs=len(queue['pairs']),development_source_records=len(c.get('development_source_records',[])),
        semantics='Nontrivial middle-name or appended-surname variants, independently verified same work/role, unique external nconst. No fuzzy name-only, initials-only, spelling/punctuation-only groups counted. Cases are rule-development evidence, not independent human evaluation.')
    save_json(Path(c['report_root'])/'task5'/'substantive_alias_audit.json',result)
    if c.get('person_evaluation_registry') and c.get('scope','full')=='full':
        from prepare_person_evaluation import prepare
        result['independent_evaluation_candidates']=prepare(c,sorted(groups))
    return result
if __name__=='__main__':print(js(audit(load_config())))
