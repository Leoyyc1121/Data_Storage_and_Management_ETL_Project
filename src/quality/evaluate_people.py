"""Evaluate FINAL person mappings using separately supplied independent HUMAN labels."""
import argparse
from contextlib import closing
from datetime import datetime
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
def evaluate(c,path):
    labels=json.loads(Path(path).read_text(encoding='utf-8'))['pairs']
    complete=[p for p in labels if p.get('label') in ('same_person','different_person')]
    if len(complete)<100 or len({p['pair_id'] for p in complete})!=len(complete):raise ValueError('At least100 distinct independently HUMAN labeled pairs required')
    if {p['label'] for p in complete}!={'same_person','different_person'}:raise ValueError('Both classes required')
    from verify_task5 import verify
    verify(c);tp=fp=fn=tn=unresolved=0
    registry=None
    if c.get('person_evaluation_registry'):
        registry_path=ROOT/c['person_evaluation_registry']
        registry_audit=json.loads((Path(c['report_root'])/'task5/evaluation_queue_audit.json').read_text(encoding='utf-8'))
        if hash_file(registry_path)!=registry_audit['registry']['sha256']:raise ValueError('Reserved evaluation registry changed')
        registry=json.loads(registry_path.read_text(encoding='utf-8'))
        registry_pairs={r['pair_id']:r for r in registry['pairs']}
    with closing(ro(Path(c['work_dir'])/'integration.sqlite3')) as d:
        if registry and registry['integration_task5_fingerprint']!=required(d,'task5'):raise ValueError('Reserved evaluation refers to a different model version')
        for p in complete:
            if p.get('labeler_type')!='human' or not all(p.get(k) for k in ('labeled_by','labeled_at','labeling_evidence','rationale')):raise ValueError('Human provenance required; AI/synthetic labels not accepted')
            if datetime.fromisoformat(p['labeled_at']).tzinfo is None:raise ValueError('Timezone required')
            reserved=registry_pairs.get(p['pair_id']) if registry else None
            pair=dict(left_alias_id=reserved['left']['alias_id'],right_alias_id=reserved['right']['alias_id']) if reserved else None if registry else d.execute('SELECT * FROM person_evaluation_pairs WHERE pair_id=?',(p['pair_id'],)).fetchone()
            if not pair:raise ValueError('Not reserved holdout pair')
            people=[d.execute('SELECT p.* FROM person_aliases a JOIN people p USING(person_id) WHERE a.alias_id=?',(pair[k],)).fetchone() for k in ('left_alias_id','right_alias_id')]
            prediction=people[0]['person_id']==people[1]['person_id'];gold=p['label']=='same_person'
            tp+=prediction and gold;fp+=prediction and not gold;fn+=not prediction and gold;tn+=not prediction and not gold
            unresolved+=any(x['identity_status']=='provisional_source_context' for x in people)
    precision=tp/(tp+fp) if tp+fp else None;recall=tp/(tp+fn) if tp+fn else None
    f1=2*precision*recall/(precision+recall) if precision is not None and recall is not None and precision+recall else 0.0 if precision==0 and recall==0 else None
    return dict(status='evaluated_user_supplied_human_declarations',pairs=len(complete),tp=tp,fp=fp,fn=fn,tn=tn,precision=precision,recall=recall,f1=f1,unresolved_coverage=unresolved/len(complete),scope='Reserved pair set only, not whole-dataset accuracy; declarations cannot prove human authorship.')
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');p.add_argument('--labels',type=Path,required=True);a=p.parse_args();c=load_config(a.config);r=evaluate(c,a.labels);save_json(Path(c['report_root'])/'task5'/'evaluation_results.json',r);print(js(r))
if __name__=='__main__':main()
