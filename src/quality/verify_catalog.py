"""Re-extract every retained IMDb TSV row and compare all supported typed fields."""
import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'));sys.path.insert(0,str(ROOT/'src/extract'))
from integration_common import *
from import_imdb import number,array,missing,HEADERS
from acquire_imdb import load_config as imdb_config
def verify(path):
    count=0
    with closing(ro(path)) as d:
        d.execute('PRAGMA cache_size=-131072')
        for r in d.execute('SELECT * FROM raw_records'):
            raw=r['raw_record'];v=raw.decode('utf-8').rstrip('\r\n').split('\t')
            if len(v)!=len(HEADERS[r['file']]) or len(raw)!=r['byte_end']-r['byte_start'] or hashlib.sha256(raw).hexdigest()!=r['raw_sha256']:raise ValueError('Raw TSV evidence mismatch')
            if r['file']=='title.basics.tsv.gz':
                t=d.execute('SELECT * FROM titles WHERE tconst=?',(v[0],)).fetchone()
                expected=dict(tconst=v[0],title_type=v[1],primary_title=v[2],original_title=v[3],release_year=number(v[5]),runtime=number(v[7]),genres=array(v[8]))
            elif r['file']=='title.crew.tsv.gz':
                t=d.execute('SELECT * FROM crew WHERE tconst=?',(v[0],)).fetchone();expected=dict(tconst=v[0],directors=array(v[1]),writers=array(v[2]))
            elif r['file']=='title.principals.tsv.gz':
                t=d.execute('SELECT * FROM principals WHERE tconst=? AND ordering=?',(v[0],int(v[1]))).fetchone();expected=dict(tconst=v[0],ordering=int(v[1]),nconst=v[2],category=v[3],job=missing(v[4]),characters=missing(v[5]))
            else:
                t=d.execute('SELECT * FROM names WHERE nconst=?',(v[0],)).fetchone();expected=dict(nconst=v[0],primary_name=v[1],birth_year=number(v[2]),death_year=number(v[3]),professions=array(v[4]),known_for=array(v[5]))
            if t is None or t['source_record_id']!=r['source_record_id']:raise ValueError('Missing typed raw-row relation')
            for key,value in expected.items():
                actual=json.loads(t[key]) if isinstance(value,list) else t[key]
                if actual!=value:raise ValueError('Typed catalog field mismatch: '+key)
            count+=1
        if d.execute('PRAGMA foreign_key_check').fetchone():raise ValueError('Catalog FK')
    return dict(status='passed_complete_retained_raw_TSV_field_replay',raw_records_compared=count,foreign_key_errors=0,scope='Retained candidate subset only; full archive scan/CRC evidence separately registered. Not identity matching accuracy.')
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/imdb.example.json');a=p.parse_args();c=imdb_config(a.config);r=verify(Path(c['work_dir'])/'catalog.sqlite3');save_json(Path(c['report_dir'])/'full_verification.json',r);print(js(r))
if __name__=='__main__':main()
