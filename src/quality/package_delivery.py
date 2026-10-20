"""Build a checked code/report ZIP; licensed local data are never bundled."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
from zipfile import ZipFile,ZIP_DEFLATED

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/quality'))
import verify_submission as submission
import finalize_annotations as annotations
from integration_common import save_json,hash_file,utc_now


def acceptance():
    read=lambda name:json.loads((ROOT/name).read_text(encoding='utf-8-sig'))
    tests=read('reports/final/test_results.json');full=read('reports/final/full_verification.json');queries=read('reports/final/query_verification.json')
    pipeline=read('reports/final/movie_merge_pipeline_acceptance.json');isolated=read('reports/final/isolated_reproduction.json')
    course=read('reports/final/requirements_audit.json');source=read('reports/final/source_annotations.json')
    directory=Path(source['evidence']['path']).parent;source_check=annotations.verify(directory)
    checks=dict(engineering_tests=tests['status']=='passed' and not tests['failures'] and not tests['errors'],
        registered_full_target_integrity=full['status']=='passed_integrity_not_human_accuracy' and all(full[k]==0 for k in ['foreign_key_errors','product_review_count_mismatches','unknown_lineage_parent_nodes','selected_fields_without_source']),
        five_actual_work_groups=pipeline['actual_full_target_work_groups']>=5,
        executable_queries=queries['status']=='passed' and len(queries['queries'])>=5,
        actual_idempotency_and_fixed_recovery=pipeline['same_input_all_fifteen_counts_equal'] and pipeline['offline_actual_exit130_resume_repeat'],
        registered_isolated_rebuild=isolated['counts_equal_original_reconstruction'] and isolated['actual_target_action']=='complete',
        source_annotations=source_check['status']=='passed_source_annotation_integrity' and all(v['total']==120 for v in source_check['packets'].values()))
    if not all(checks.values()):raise ValueError('Delivery engineering checks failed: '+str(checks))
    report=dict(observed_at=utc_now(),status='ready_under_user_scope_with_disclosed_limitations',checks=checks,
        scope_change='User explicitly requests source-evidence annotation instead of the original independent-human labeling requirement; no annotation name/time fields are required.',
        independent_human_evaluation=False,strict_assignment_acceptance=course['strict_assignment_acceptance'],
        engineering_tests=tests['tests_run'],source_annotation_counts=source_check['packets'],
        disclosed_limitations=['Not independent HUMAN evaluation; original course requirement remains unsatisfied.',
            '53 source pairs abstain for insufficient evidence, not forced guesses.',
            'Live HTML success remains zero; permitted alternative sources and access failures retained.',
            'Historical original compressed checksum/exact download time remain unavailable; homonym evidence has declared scope limitations.',
            'ZIP contains code, configuration, report, SQL, tests and aggregate evidence. Large/third-party raw or derived data, pair annotations and environment remain local with registered checksums and restoration instructions.'],
        replay_scope='This packaging verifies current local annotation sources and existing engineering evidence. It does not claim a new full 35 GB ETL rebuild or a new independent accuracy measurement.')
    save_json(ROOT/'reports/final/delivery_acceptance.json',report);return report


def package(output):
    output=Path(output).resolve()
    if output.suffix.lower()!='.zip' or output.exists() or not output.parent.is_dir():raise ValueError('Choose a new .zip file in an existing directory')
    if output.is_relative_to(ROOT):raise ValueError('Delivery ZIP must be outside the repository')
    receipt=output.with_suffix('.receipt.json')
    if receipt.exists():raise ValueError('Receipt exists; preserve the old delivery')
    result=acceptance();submission.verify(False)
    manifest=json.loads((ROOT/'reports/final/submission_manifest.json').read_text(encoding='utf-8'))
    files=[ROOT/item['path'] for item in manifest['files']]+[ROOT/name for name in sorted(submission.SELF_OUTPUTS)]
    if len(files)!=len(set(files)):raise ValueError('Duplicate archive entries')
    for item in manifest['files']:
        if hash_file(ROOT/item['path'])!=item['sha256']:raise ValueError('File changed after inventory: '+item['path'])
    expected={p.relative_to(ROOT).as_posix():hash_file(p) for p in files}
    with ZipFile(output,'x',compression=ZIP_DEFLATED,compresslevel=9) as archive:
        for path in sorted(files):archive.write(path,arcname='MovieReviewsETL/'+path.relative_to(ROOT).as_posix())
    with ZipFile(output) as archive:
        if archive.testzip() is not None:raise ValueError('Archive CRC failed')
        actual={name.removeprefix('MovieReviewsETL/'):hashlib.sha256(archive.read(name)).hexdigest() for name in archive.namelist()}
        if actual!=expected:raise ValueError('Archive content differs from frozen inventory')
    summary=dict(observed_at=utc_now(),status='passed_archive_crc_and_all_member_sha256',zip_path=str(output),
        bytes=output.stat().st_size,sha256=hash_file(output),files=len(expected),
        submission_content_fingerprint=manifest['content_fingerprint'],strict_assignment_acceptance=False,
        user_scope_acceptance=result['status'],licensed_local_data_included=False,
        local_target='data/target/final_v8/etl.sqlite3',local_annotations='data/evaluation/source_annotations_v1',
        receipt_scope='Sidecar outside the ZIP prevents recursive self-hash. Does not certify independent-human annotation or course grading.')
    save_json(receipt,summary);return summary


def main():
    if hasattr(sys.stdout,'reconfigure'):sys.stdout.reconfigure(encoding='utf-8')
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();print(json.dumps(package(args.output),ensure_ascii=False,indent=2))


if __name__=='__main__':main()
