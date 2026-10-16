"""Export current committed Task1 shards for downstream same-source reproduction.

New parse run IDs can change Parquet binary hashes; never reuse an old machine's
output manifest after legitimately regenerating the same original review file.
"""
import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
from acquire_products import save_json
from register_source import hash_file,utc_now
ROOT=Path(__file__).resolve().parents[2]
def export(run_dir,output):
    run_dir=Path(run_dir)
    with closing(sqlite3.connect(run_dir/'checkpoint.sqlite3')) as d:
        state=json.loads(d.execute('SELECT payload FROM state WHERE id=1').fetchone()[0])
        if state['status']!='complete' or state['max_records'] is not None:raise ValueError('Full completed Task1 required')
        files=[]
        for (payload,) in d.execute('SELECT payload FROM batches ORDER BY batch_id'):
            batch=json.loads(payload)
            for f in batch['files']:
                relative=f['path'].replace('\\','/')
                if not relative.startswith('reviews/'):continue
                path=run_dir/relative
                if hash_file(path)!=f['sha256']:raise ValueError('Changed source shard')
                files.append(dict(batch_id=batch.get('batch_id'),path=relative,rows=f['rows'],file_size_bytes=path.stat().st_size,sha256=f['sha256']))
        if sum(f['rows'] for f in files)!=state['valid_records']:raise ValueError('Manifest row reconciliation')
        result=dict(manifest_version='1',verified_at=utc_now(),dataset='Task1 structured reviews',run_id=state['run_id'],parser_version=state['parser_version'],rule_version=state['rule_version'],source_sha256=state['source_sha256'],storage_location=str(run_dir.resolve()),file_count=len(files),records=state['valid_records'],quarantined_records=state['quarantined_records'],parquet_size_bytes=sum(f['file_size_bytes'] for f in files),files=files,
            distribution='Local storage only; files not hosted on GitHub.',recovery_instructions='Register current source path; parse/verify Task1; run this exporter, then downstream ETL. Same source/versions preserves record IDs, not necessarily binary Parquet hashes across new run IDs.')
        save_json(output,result);return result
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-dir',type=Path,default=ROOT/'data/staging/task1/full_v2');p.add_argument('--output',type=Path,default=ROOT/'reports/task1/full_v2_output_manifest.json');a=p.parse_args();r=export(a.run_dir,a.output);print(json.dumps(dict(status='complete',files=r['file_count'],records=r['records'])))
if __name__=='__main__':main()
