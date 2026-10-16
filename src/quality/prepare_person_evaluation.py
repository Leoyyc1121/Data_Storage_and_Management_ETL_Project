"""Reserve both identity-like and confusing-different-context person pairs.

No gold labels or predictions are emitted. The sealed integration DB is read
only; this separate assessment artifact can be human-labeled later.
"""
from collections import defaultdict,Counter
from contextlib import closing
from itertools import combinations
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *

def reserve(aliases,development_sources=(),development_people=(),limit=120):
    excluded_sources=set(development_sources);excluded_people=set(development_people)
    rows=[dict(x) for x in aliases if x['source_record_id'] not in excluded_sources and x['person_id'] not in excluded_people]
    by_person=defaultdict(list);by_surname=defaultdict(dict);by_initials=defaultdict(dict)
    for a in sorted(rows,key=lambda x:x['alias_id']):
        by_person[a['person_id']].append(a);tokens=a['normalized_name'].split()
        if len(tokens)>=2:
            by_surname[tokens[-1]].setdefault(a['person_id'],a)
            by_initials[(tokens[0][0],tokens[-1][0])].setdefault(a['person_id'],a)
    buckets=defaultdict(dict)
    def add(a,b,stratum):
        pid='person-pair:'+digest_json(sorted([a['alias_id'],b['alias_id']]))
        buckets[stratum][pid]=(a,b)
    for group in by_person.values():
        for a,b in zip(group,group[1:]):
            if a['movie_id']!=b['movie_id']:add(a,b,'cross_work_identity_candidate')
    for groups,stratum in [(by_surname,'similar_name_different_context_candidate'),(by_initials,'shared_initials_different_context_candidate')]:
        for group in groups.values():
            values=list(group.values())[:50]
            for a,b in combinations(values,2):add(a,b,stratum)
    chosen=[];used=set();quotas={'cross_work_identity_candidate':limit//2,'similar_name_different_context_candidate':limit//4,'shared_initials_different_context_candidate':limit-limit//2-limit//4}
    for stratum,quota in quotas.items():
        for pid,(a,b) in sorted(buckets[stratum].items(),key=lambda x:digest_json(['person-independent-v2',x[0]])):
            if pid in used:continue
            chosen.append((pid,a,b,stratum));used.add(pid)
            if sum(x[3]==stratum for x in chosen)>=quota:break
    return [dict(pair_id=pid,stratum=stratum,left={k:v for k,v in a.items() if k!='person_id'},right={k:v for k,v in b.items() if k!='person_id'},label=None,labeled_by=None,labeler_type=None,labeled_at=None,labeling_evidence=None,rationale=None) for pid,a,b,stratum in chosen]

def prepare(c,development_people=()):
    with closing(ro(Path(c['work_dir'])/'integration.sqlite3')) as d:
        rows=[dict(r) for r in d.execute('SELECT a.*,p.nconst,p.birth_year,p.death_year,p.identity_status FROM person_aliases a JOIN people p USING(person_id)')]
        fingerprint=required(d,'task5')
    pairs=reserve(rows,c.get('development_source_records',[]),development_people,c.get('evaluation_pairs',120))
    strata=dict(Counter(p['stratum'] for p in pairs))
    if len(pairs)<100 or len(strata)<2:raise ValueError('Insufficient independent mixed candidate pool; do not substitute a positive-only set')
    path=ROOT/c['person_evaluation_registry']
    content=dict(partition='reserved_independent_human_evaluation_v2',predictions_hidden=True,integration_task5_fingerprint=fingerprint,development_sources=list(c.get('development_source_records',[])),development_people=sorted(development_people),pairs=pairs,
        limitations='Candidate strata are not gold classes. Human evidence, both gold classes and difficult-case coverage still need verification before metrics. No AI-generated labels.')
    if path.exists():
        old=json.loads(path.read_text(encoding='utf-8'))
        if old!=content:raise ValueError('Reserved evaluation evidence changed; new registry version required')
    else:save_json(path,content)
    result=dict(status='mixed_independent_candidates_reserved_NOT_HUMAN_labeled',pairs=len(pairs),candidate_strata=strata,human_labeled_pairs=0,
        registry=dict(path=str(path),sha256=hash_file(path)),integration_task5_fingerprint=fingerprint,
        excluded_development_sources=len(c.get('development_source_records',[])),excluded_development_people=len(development_people),
        scope='Separate read-only evaluation registry; supersedes the positive-only in-DB v7 candidate queue for future person evaluation. Candidate buckets are NOT manually established gold classes.')
    save_json(Path(c['report_root'])/'task5/evaluation_queue_audit.json',result);return result
if __name__=='__main__':
    c=load_config();a=json.loads((Path(c['report_root'])/'task5/substantive_alias_audit.json').read_text(encoding='utf-8'));print(js(prepare(c,a['case_person_ids'])))
