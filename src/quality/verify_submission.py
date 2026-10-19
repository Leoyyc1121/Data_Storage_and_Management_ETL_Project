"""Inventory submission files separately from required, local-only runtime data."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/extract'))
from acquire_products import save_json, hash_file, utc_now

SELF_OUTPUTS={'reports/final/submission_manifest.json','reports/final/submission_verification.json'}
LOCAL_ONLY_REPORTS={'reports/task1/full.log','reports/task1/sample_20k.log','reports/task1/sample_recovery_20k.log'}
ROOT_FILES={'.gitignore','.gitattributes','README.md','requirements.txt','项目实施记录.md'}
DIRECTORIES={'config':{'.json'},'src':{'.py'},'tests':{'.py'},'docs':{'.md','.mmd'},'sql':{'.sql'},'reports':{'.json','.md','.log'}}
TOKENS=[re.compile(p) for p in (r'ghp_[A-Za-z0-9]{30,}',r'github_pat_[A-Za-z0-9_]{30,}',r'AKIA[0-9A-Z]{16}',r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----')]


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def collect(root):
    root=Path(root).resolve();files=[]
    for p in root.iterdir():
        if p.is_file():
            if p.name not in ROOT_FILES:raise ValueError('Unexpected root file: '+p.name)
            files.append(p)
        elif p.name not in {*DIRECTORIES,'data','.git','.venv'}:
            raise ValueError('Unexpected root directory: '+p.name)
    for folder,suffixes in DIRECTORIES.items():
        for p in (root/folder).rglob('*'):
            if not p.is_file() or '__pycache__' in p.parts:continue
            if p.suffix not in suffixes:raise ValueError('Unexpected submission file: '+str(p.relative_to(root)))
            if p.relative_to(root).as_posix() not in SELF_OUTPUTS|LOCAL_ONLY_REPORTS:files.append(p)
    keep=root/'data/raw/.gitkeep'
    if keep.exists():files.append(keep)
    return sorted(files,key=lambda p:p.relative_to(root).as_posix())


def inspect_files(root,files):
    items=[]
    for p in files:
        rel=p.relative_to(root).as_posix()
        if p.is_symlink() or p.stat().st_size>5*1024*1024:raise ValueError('Symlink or large file in submission: '+rel)
        content=p.read_text(encoding='utf-8-sig')
        if any(pattern.search(content) for pattern in TOKENS):raise ValueError('Possible credential in '+rel)
        if p.suffix=='.json':json.loads(content)
        if p.suffix=='.md':
            for target in re.findall(r'\]\(([^)]+)\)',content):
                target=target.strip('<>').split('#',1)[0]
                if not target or target.startswith(('https://','http://','mailto:')):continue
                if not (p.parent/target).exists():raise ValueError('Missing documentation link: '+rel+' -> '+target)
        items.append(dict(path=rel,bytes=p.stat().st_size,sha256=hash_file(p)))
    return items


def local_artifacts(hash_large=False):
    t1=read(ROOT/'reports/task1/source_manifest.reported.json')['source_file']
    target=read(ROOT/'reports/task9/load_statistics.json')
    reviews=read(ROOT/'reports/task8/output_manifest.json')
    t2=read(ROOT/'reports/task2/full_statistics.json')
    fixed=read(ROOT/'reports/final/offline_sample_manifest.json')
    registered=[('review_source',t1['path'],t1['file_size_bytes'],t1['sha256'],'reports/task1/source_manifest.reported.json'),
        ('final_target',target['target_path'],target['file_size_bytes'],target['sha256'],'reports/task9/load_statistics.json'),
        ('cleaned_reviews',reviews['database_path'],reviews['file_size_bytes'],reviews['sha256'],'reports/task8/output_manifest.json')]
    for label,v in [('product_manifest',t2['product_manifest']),('research_metadata',t2['third_party_metadata']['output'])]:
        registered.append((label,v['path'],v['file_size_bytes'],v['sha256'],'reports/task2/full_statistics.json'))
    registered += [('fixed_raw_packet',fixed['packet']['path'],fixed['packet']['bytes'],fixed['packet']['sha256'],'reports/final/offline_sample_manifest.json'),
        ('fixed_candidate_catalog',fixed['catalog']['path'],None,fixed['catalog']['sha256'],'reports/final/offline_sample_manifest.json')]
    rows=[]
    for label,location,size,digest,evidence in registered:
        path=(ROOT/location).resolve()
        if not path.is_file() or size is not None and path.stat().st_size!=size:raise ValueError('Missing/changed local artifact: '+label)
        rehashed=hash_large or path.stat().st_size<64*1024*1024
        if rehashed and hash_file(path)!=digest:raise ValueError('Local artifact SHA mismatch: '+label)
        rows.append(dict(kind=label,path=str(path),bytes=path.stat().st_size,registered_sha256=digest,
            checksum_status='recomputed_and_matched' if rehashed else 'registered_previous_verification_not_rehashed_this_run',registration=evidence,public_git=False))
    required=['data/staging/task1/full_v2/checkpoint.sqlite3','data/staging/task2/full/acquisition.sqlite3',
        'data/staging/task2/full/metadata.sqlite3','data/staging/task3/full_v2/classification.sqlite3',
        'data/integration/task4/full_v3/resolution.sqlite3','data/integration/final_v7/integration.sqlite3',
        'data/acceptance/fixed_v2/run_v1/target/etl.sqlite3','data/evaluation/people_v2/reserved_pairs.json',
        'data/evaluation/annotation_v1/movies.labels.json','data/evaluation/annotation_v1/people.labels.json']
    for rel in required:
        if not (ROOT/rel).is_file():raise ValueError('Required runtime material missing: '+rel)
    return rows,required


def verify(hash_large=False):
    artifacts,required=local_artifacts(hash_large)
    summary=read(ROOT/'reports/task2/scrapy_retest_summary.json')
    for evidence in summary['evidence']:
        if hash_file(evidence['archived_path'])!=evidence['sha256']:raise ValueError('Archived experiment evidence changed')
    # Inventory last: hashing large artifacts can take minutes, during which an
    # independent test run may finish and update its aggregate report.
    items=inspect_files(ROOT,collect(ROOT))
    manifest=dict(observed_at=utc_now(),status='passed_submission_boundary_and_registered_local_artifacts',
        files=items,files_count=len(items),submission_bytes=sum(x['bytes'] for x in items),
        content_fingerprint=hashlib.sha256(json.dumps(items,sort_keys=True).encode()).hexdigest(),
        self_hash_exclusions=sorted(SELF_OUTPUTS),local_only_artifacts=artifacts,required_local_runtime_files=required,
        recovery_instructions='docs/交付与恢复.md; README.md; reports/final/cleanup_manifest.json',
        source_distribution='Large data, licensed source/derived content, annotations, offline evidence and environment are not included in Git.',
        limitations='Boundary, known credential-pattern, link, JSON and recorded artifact checks only; NOT human matching accuracy or a new replay of all source fields.')
    save_json(ROOT/'reports/final/submission_manifest.json',manifest)
    result=dict(observed_at=utc_now(),status='passed',submitted_content_files=len(items),submission_bytes=manifest['submission_bytes'],
        no_large_or_restricted_data_in_submission=True,json_and_markdown_links_valid=True,known_credential_patterns_found=0,
        registered_local_artifacts_checked=len(artifacts),large_artifacts_rehashed=hash_large,
        archived_scrapy_evidence_files_hash_checked=len(summary['evidence']),
        scope=manifest['limitations'],self_hash_exclusions=sorted(SELF_OUTPUTS))
    save_json(ROOT/'reports/final/submission_verification.json',result)
    return result


def main():
    if hasattr(sys.stdout,'reconfigure'):sys.stdout.reconfigure(encoding='utf-8')
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--hash-large',action='store_true',help='Also rehash the large raw/target/review/catalog files; otherwise retain earlier registered hashes explicitly')
    a=p.parse_args();print(json.dumps(verify(a.hash_large),ensure_ascii=False,indent=2))


if __name__=='__main__':main()
