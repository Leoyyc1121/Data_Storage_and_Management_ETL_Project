"""Verify every preserved review, identity mapping and normalization against Task1 shards."""
import argparse
from collections import Counter
from contextlib import closing
import hashlib
import json
from pathlib import Path
import sys
import pyarrow.parquet as pq
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
import clean_reviews as cr
def verify(c):
    manifest,_=cr.registration(c);total=0;logical_hash=hashlib.sha256();issues=Counter()
    with closing(ro(cr.review_root(c)/'reviews.sqlite3')) as d:
        d.execute('PRAGMA cache_size=-786432');d.execute('PRAGMA mmap_size=1073741824')
        if not cr.get(d,'complete',False):raise ValueError('Incomplete review cleaning')
        if d.execute('PRAGMA foreign_key_check').fetchone():raise ValueError('Review mapping FK')
        rows=iter(d.execute('SELECT * FROM raw_reviews ORDER BY record_sequence'))
        for f in manifest['files']:
            path=Path(c['source_reviews'])/f['path']
            if hash_file(path)!=f['sha256']:raise ValueError('Changed source Parquet')
            for batch in pq.ParquetFile(path).iter_batches(batch_size=c['batch_size'],columns=cr.FIELDS):
                for source in batch.to_pylist():
                    actual=next(rows,None)
                    if actual is None:raise ValueError('Missing preserved raw review')
                    for field in cr.FIELDS:
                        expected=js(source[field]) if field in cr.JSON_FIELDS else source[field]
                        if actual[field]!=expected:raise ValueError(f'Raw field mismatch {source["record_sequence"]}:{field}')
                    if actual['summary_clean']!=cr.clean_text(source['summary']) or actual['review_text_clean']!=cr.clean_text(source['review_text']) or actual['logical_review_id']!=cr.logical_id(source):raise ValueError('Derived review mismatch')
                    total+=1
                    logical_hash.update(js([actual[k] for k in ('record_sequence','source_review_id','raw_record_sha256','logical_review_id')]).encode());logical_hash.update(b'\n')
                    issues.update(source['quality_issues'])
        if next(rows,None) is not None:raise ValueError('Extra target review')
        bad=d.execute('SELECT count(*) FROM logical_reviews l LEFT JOIN raw_reviews r ON l.representative_sequence=r.record_sequence WHERE r.record_sequence IS NULL OR r.logical_review_id!=l.logical_review_id OR r.product_id!=l.asin OR r.raw_record_sha256!=l.raw_record_sha256').fetchone()[0]
        if bad or total!=manifest['records']:raise ValueError('Logical representative/count mismatch')
        return dict(status='passed_full_raw_and_clean_field_replay',all_raw_reviews_compared=total,source_shards_hash_checked=len(manifest['files']),identity_mapping_sha256=logical_hash.hexdigest(),quality_issue_counts=dict(issues),foreign_key_errors=0,invalid_logical_representatives=bad)
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/reviews.example.json');a=p.parse_args();c=load_config(a.config);r=verify(c);save_json(Path(c['report_root'])/'task8/full_verification.json',r);print(js(r))
if __name__=='__main__':main()
