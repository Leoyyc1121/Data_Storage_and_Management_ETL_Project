"""Task8: bounded-batch, restartable raw-review preservation and exact dedup mapping.

Only byte-identical full records within the same product share a logical review.
Cross-edition user/time/text repeats are candidates, never automatic deletions.
"""
import argparse
from collections import Counter
from contextlib import closing
from html import unescape
import json
import logging
import math
from pathlib import Path
import re
import sqlite3
import sys
import unicodedata
import uuid
import pyarrow.parquet as pq
sys.path.insert(0,str(Path(__file__).resolve().parent))
from integration_common import *

VERSION='preserve-exact-product-review-v1'
FIELDS=['source_review_id','source_file_sha256','raw_record_sha256','record_sequence','product_id','reviewer_id','profile_name','helpfulness_raw','score_raw','review_time_raw','summary','review_text','helpful_votes','total_votes','helpfulness_ratio','score','review_time_unix','review_time_utc','quality_issues','encoding_issue_fields','raw_record_base64','byte_start','byte_end','start_line','end_line','run_id','parser_version','rule_version']
COLUMNS=FIELDS+['summary_clean','review_text_clean','logical_review_id','clean_rule_version','clean_run_id']
JSON_FIELDS={'quality_issues','encoding_issue_fields'}
def clean_text(value):
    if value is None:return None
    return ' '.join(unicodedata.normalize('NFKC',unescape(value)).split())
def logical_id(row):
    # Full raw-record hash includes ASIN and every field; no text-only/user-only dedup.
    return stable('lr:',row['product_id']+':'+row['raw_record_sha256'])
def values(row,run):
    data=[js(row[k]) if k in JSON_FIELDS else row[k] for k in FIELDS]
    return data+[clean_text(row['summary']),clean_text(row['review_text']),logical_id(row),VERSION,run]
def open_reviews(c):
    root=review_root(c);root.mkdir(parents=True,exist_ok=True)
    d=sqlite3.connect(root/'reviews.sqlite3');d.row_factory=sqlite3.Row
    d.execute('PRAGMA foreign_keys=ON');d.execute('PRAGMA journal_mode=WAL');d.execute('PRAGMA synchronous=FULL')
    definitions=[]
    integer={'record_sequence','helpful_votes','total_votes','review_time_unix','byte_start','byte_end','start_line','end_line'}
    for key in COLUMNS:
        kind='INTEGER' if key in integer else 'REAL' if key in ('score','helpfulness_ratio') else 'TEXT'
        suffix=' PRIMARY KEY' if key=='record_sequence' else ' NOT NULL UNIQUE' if key=='source_review_id' else ' REFERENCES review_products(asin)' if key=='product_id' else ' REFERENCES reviewers(reviewer_id)' if key=='reviewer_id' else ' NOT NULL REFERENCES logical_reviews(logical_review_id)' if key=='logical_review_id' else ''
        definitions.append('"'+key+'" '+kind+suffix)
    d.executescript('''
    CREATE TABLE IF NOT EXISTS review_state(key TEXT PRIMARY KEY,payload TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS review_runs(run_id TEXT PRIMARY KEY,started_at TEXT,finished_at TEXT,status TEXT);
    CREATE TABLE IF NOT EXISTS review_products(asin TEXT PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS reviewers(reviewer_id TEXT PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS logical_reviews(logical_review_id TEXT PRIMARY KEY,asin TEXT NOT NULL REFERENCES review_products(asin),raw_record_sha256 TEXT NOT NULL,representative_sequence INTEGER NOT NULL,UNIQUE(asin,raw_record_sha256));
    CREATE TABLE IF NOT EXISTS cross_edition_candidates(candidate_id TEXT PRIMARY KEY,left_sequence INTEGER NOT NULL,right_sequence INTEGER NOT NULL,status TEXT NOT NULL,reason TEXT NOT NULL);
    ''')
    d.execute('CREATE TABLE IF NOT EXISTS raw_reviews('+','.join(definitions)+')')
    return d
def review_root(c):return Path(c['work_dir'])/c.get('review_version','review_work')
def get(d,key,default=None):
    r=d.execute('SELECT payload FROM review_state WHERE key=?',(key,)).fetchone();return json.loads(r[0]) if r else default
def set_state(d,key,value):d.execute('INSERT OR REPLACE INTO review_state VALUES (?,?)',(key,js(value)))
def registration(c):
    manifest=json.loads((ROOT/'reports/task1/full_v2_output_manifest.json').read_text(encoding='utf-8'))
    root=Path(c['source_reviews'])
    actual={str(p.relative_to(root)).replace('\\','/') for p in (root/'reviews').glob('*.parquet')}
    if actual!={f['path'] for f in manifest['files']}:raise ValueError('Source Parquet manifest set mismatch')
    return manifest,dict(source_sha256=manifest['source_sha256'],files=[{k:f[k] for k in ('path','rows','sha256')} for f in manifest['files']],version=VERSION,implementation=hash_file(__file__),batch_size=c['batch_size'])
def insert_batch(d,rows,run):
    d.executemany('INSERT OR IGNORE INTO review_products VALUES (?)',[(x,) for x in {r['product_id'] for r in rows}])
    d.executemany('INSERT OR IGNORE INTO reviewers VALUES (?)',[(x,) for x in {r['reviewer_id'] for r in rows if r['reviewer_id']}])
    pending={}
    for r in rows:
        lid=logical_id(r)
        inserted=d.execute('INSERT OR IGNORE INTO logical_reviews VALUES (?,?,?,?)',(lid,r['product_id'],r['raw_record_sha256'],r['record_sequence'])).rowcount
        if not inserted:
            previous=pending.get(lid)
            if previous is None:previous=d.execute('SELECT r.* FROM raw_reviews r JOIN logical_reviews l ON r.record_sequence=l.representative_sequence WHERE l.logical_review_id=?',(lid,)).fetchone()
            if previous is None:raise ValueError('Broken exact duplicate representative')
            for k in ('product_id','reviewer_id','profile_name','helpfulness_raw','score_raw','review_time_raw','summary','review_text','raw_record_base64'):
                if previous[k]!=r[k]:raise ValueError('Raw-hash collision or corrupted duplicate: not merged')
        pending.setdefault(lid,r)
    d.executemany('INSERT INTO raw_reviews VALUES ('+','.join('?' for _ in COLUMNS)+')',[values(r,run) for r in rows])
def summarize(c,d,manifest):
    raw=d.execute('SELECT count(*) FROM raw_reviews').fetchone()[0];logical=d.execute('SELECT count(*) FROM logical_reviews').fetchone()[0]
    issues=Counter()
    for q,n in d.execute('SELECT quality_issues,count(*) FROM raw_reviews GROUP BY quality_issues'):
        for issue in json.loads(q):issues[issue]+=n
    h=__import__('hashlib').sha256()
    for r in d.execute('SELECT record_sequence,source_review_id,raw_record_sha256,logical_review_id FROM raw_reviews ORDER BY record_sequence'):h.update(js(list(r)).encode());h.update(b'\n')
    result=dict(status='complete',observed_at=utc_now(),raw_reviews=raw,logical_reviews=logical,exact_same_product_duplicate_records=raw-logical,deleted_raw_reviews=0,
        distinct_products=d.execute('SELECT count(*) FROM review_products').fetchone()[0],distinct_reviewers=d.execute('SELECT count(*) FROM reviewers').fetchone()[0],quality_issue_counts=dict(issues),
        identity_mapping_sha256=h.hexdigest(),database_path=str((review_root(c)/'reviews.sqlite3').resolve()),
        source_sha256=manifest['source_sha256'],source_files=len(manifest['files']),
        rating_policy='One valid score per logical byte-identical-within-product review; cross-product/edition candidates kept separate; product-only compilation reviews. Movie aggregation requires single_work mapping and reports identity status, raw count, logical count and valid-score denominator.',
        cross_edition_policy='Candidate blocking by same reviewer/time/normalized text within final mapped work, never auto-delete. Task9 materializes this scoped candidate queue.',
        preservation='Every source review and raw value retained; cleaned text is separate; unresolved UTF8 text remains NULL with original bytes and locators. Ratios only when 0<=helpful<=total and total>0.')
    save_json(Path(c['report_root'])/'task8/full_statistics.json',result);return result
def run(c,stop_after_batches=None):
    manifest,inputs=registration(c);fingerprint=digest_json(inputs);batches=0;root=review_root(c)
    with run_lock(root),closing(open_reviews(c)) as d:
        old=get(d,'input')
        if old and old!=fingerprint:raise ValueError('Review source/rules changed; new version directory required')
        run=uuid.uuid4().hex
        with d:
            set_state(d,'input',fingerprint);d.execute('INSERT INTO review_runs VALUES (?,?,?,?)',(run,utc_now(),None,'running'))
        if not get(d,'complete',False):
            cursor=get(d,'last_sequence',0)
            try:
                for f in manifest['files']:
                    path=Path(c['source_reviews'])/f['path']
                    if hash_file(path)!=f['sha256']:raise ValueError('Source shard changed: '+f['path'])
                    parquet=pq.ParquetFile(path)
                    if parquet.metadata.num_rows!=f['rows']:raise ValueError('Source shard count mismatch')
                    for batch in parquet.iter_batches(batch_size=c['batch_size'],columns=FIELDS):
                        rows=[r for r in batch.to_pylist() if r['record_sequence']>cursor]
                        if not rows:continue
                        if rows[0]['record_sequence']!=cursor+1:raise ValueError('Noncontiguous source records')
                        with d:
                            insert_batch(d,rows,run);cursor=rows[-1]['record_sequence'];set_state(d,'last_sequence',cursor)
                        batches+=1;logging.info('Committed reviews: %s',cursor)
                        if stop_after_batches and batches>=stop_after_batches:raise KeyboardInterrupt
                with d:set_state(d,'complete',True)
            except BaseException as exc:
                with d:d.execute('UPDATE review_runs SET finished_at=?,status=? WHERE run_id=?',(utc_now(),'interrupted' if isinstance(exc,KeyboardInterrupt) else 'failed',run))
                raise
        with d:d.execute("UPDATE review_runs SET finished_at=?,status='complete' WHERE run_id=?",(utc_now(),run))
        return summarize(c,d,manifest)
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');p.add_argument('--stop-after-batches',type=int);a=p.parse_args();c=load_config(a.config)
    logdir=Path(c['report_root'])/'task8';logdir.mkdir(parents=True,exist_ok=True)
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s',handlers=[logging.StreamHandler(),logging.FileHandler(logdir/'execution.log',encoding='utf-8')])
    try:print(js(run(c,a.stop_after_batches)))
    except KeyboardInterrupt:raise SystemExit(130)
if __name__=='__main__':main()
