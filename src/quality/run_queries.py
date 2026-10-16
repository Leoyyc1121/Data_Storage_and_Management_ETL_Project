"""Execute every documented query read-only; publish counts, not IMDb-derived rows."""
import argparse
from contextlib import closing
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
PARAMETERS={k:None for k in ('movie_id','person_id','field_name','source_review_id','asin','source_record_id')}
def verification_filename(bindings,query_numbers=None):
    return 'query_selection_verification.json' if query_numbers or any(v is not None for v in bindings.values()) else 'query_verification.json'
def statements(path):
    import sqlite3
    sql=''
    for line in Path(path).read_text(encoding='utf-8').splitlines():
        if line.lstrip().startswith('--'):continue
        sql+=line+'\n'
        if sqlite3.complete_statement(sql):
            if sql.strip():yield sql
            sql=''
    if sql.strip():raise ValueError('Incomplete SQL statement')
def run(c,show_rows=False,parameters=None,query_numbers=None):
    result=[]
    bindings=dict(PARAMETERS)
    if parameters:
        if set(parameters)-set(bindings):raise ValueError('Unknown query parameter')
        bindings.update(parameters)
    bindings['field_name']={'canonical_title':'title','directors':'director'}.get(bindings['field_name'],bindings['field_name'])
    with closing(ro(Path(c['target_dir'])/'etl.sqlite3')) as d:
        d.execute('PRAGMA cache_size=-524288');d.execute('PRAGMA mmap_size=1073741824')
        for i,sql in enumerate(statements(ROOT/'sql/demo_queries.sql'),1):
            if query_numbers and i not in query_numbers:continue
            rows=d.execute(sql,bindings).fetchall();result.append(dict(query=i,status='passed',returned_rows=len(rows)))
            if show_rows:print(js(dict(query=i,rows=[dict(r) for r in rows])))
    save_json(Path(c['report_root'])/'final'/verification_filename(bindings,query_numbers),dict(status='passed',queries=result,scope='Listed queries actually executed read-only; filtered/selected invocations do not overwrite full eight-query acceptance proof. Row content local only due to source terms.'));return result
def main():
    if hasattr(sys.stdout,'reconfigure'):sys.stdout.reconfigure(encoding='utf-8')
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');p.add_argument('--show-rows',action='store_true');p.add_argument('--query',type=int,nargs='+',choices=range(1,9))
    for key in PARAMETERS:p.add_argument('--'+key.replace('_','-'))
    a=p.parse_args();print(js(run(load_config(a.config),a.show_rows,{k:getattr(a,k) for k in PARAMETERS},a.query)))
if __name__=='__main__':main()
