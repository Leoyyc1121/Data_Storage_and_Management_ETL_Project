"""Replay final field choices and selected/unselected source-candidate membership."""
import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
from select_fields import choose,verify as structural
def verify(c):
    with closing(ro(Path(c['work_dir'])/'integration.sqlite3')) as d:
        required(d,'task7');structural(d);n=0
        for s in d.execute("SELECT * FROM field_selections WHERE status!='missing'"):
            candidates=[]
            for a in d.execute('SELECT a.*,m.status AS member_status FROM assertions a JOIN selection_members m USING(assertion_id) WHERE m.selection_id=?',(s['selection_id'],)):
                rank=2 if a['source_record_id'].startswith('imdb:') or a['reason']=='controlled_language_alias_or_unresolved' else 1
                eligible=not (s['entity_type']=='product' and s['field'] in ('language','subtitles') and a['reason']!='controlled_language_alias_or_unresolved')
                candidates.append(dict(assertion_id=a['assertion_id'],value=json.loads(a['normalized_value']),rank=rank,eligible=eligible))
            mode='union' if s['field'] in ('medium','language','subtitles','genres','director','starring','cast') else 'scalar'
            value,status,reason,selected=choose(candidates,mode)
            actual={r[0] for r in d.execute("SELECT assertion_id FROM selection_members WHERE selection_id=? AND status='selected'",(s['selection_id'],))}
            if js(value)!=s['value'] or status!=s['status'] or reason!=s['reason'] or selected!=actual:raise ValueError('Field choice replay mismatch: '+s['selection_id'])
            n+=1
        source_n=0
        for r in d.execute('SELECT raw_record,raw_sha256,byte_start,byte_end FROM source_records'):
            if hashlib.sha256(r['raw_record']).hexdigest()!=r['raw_sha256'] or len(r['raw_record'])!=r['byte_end']-r['byte_start']:raise ValueError('Retained raw evidence damaged')
            source_n+=1
        result=dict(status='passed_full_selection_replay',nonmissing_fields_replayed=n,all_field_states_checked=d.execute('SELECT count(*) FROM field_selections').fetchone()[0],all_retained_raw_records_hash_verified=source_n,foreign_key_errors=0)
        return result
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');a=p.parse_args();c=load_config(a.config);r=verify(c);save_json(Path(c['report_root'])/'task7/full_verification.json',r);print(js(r))
if __name__=='__main__':main()
