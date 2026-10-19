"""Evidence-backed final assignment audit; missing human evidence cannot pass."""
import argparse
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
from human_evaluation import load_labels
def read(path):return json.loads((ROOT/path).read_text(encoding='utf-8'))
def current_evaluation(path,classes,expected_model):
    report=ROOT/path
    if not report.exists():return None
    try:
        value=read(path)
        if value.get('status') not in {'evaluated_on_declared_independent_human_labels','evaluated_user_supplied_human_declarations'} or value.get('input_fingerprint')!=expected_model:return None
        labels=Path(value['labels_path'])
        if hash_file(labels)!=value['labels_sha256']:return None
        _,complete,progress=load_labels(labels,classes)
        if len(complete)!=value.get('labeled_pairs',value.get('pairs')) or progress['gold_class_counts']!=value['annotation_progress']['gold_class_counts']:return None
        return value
    except (ValueError,KeyError,OSError,TypeError):return None
def audit():
    t1=read('reports/task1/source_manifest.reported.json');t4=read('reports/task4/full_statistics.json');t5=read('reports/task5/full_statistics.json');t8=read('reports/task8/full_statistics.json')
    final=read('reports/final/full_verification.json');queries=read('reports/final/query_verification.json');offline=read('reports/final/offline_acceptance.json')
    recovery=read('reports/task9/idempotency_recovery.json')
    aliases=read('reports/task5/substantive_alias_audit.json')
    development_sources=aliases['development_source_records']
    completeness=read('reports/final/field_completeness.json')
    identified=completeness['levels']['identified_movie']['denominator']
    identified_products=completeness['identification']['multifield_identified_products']
    handoff=read('reports/final/human_evaluation_handoff.json') if (ROOT/'reports/final/human_evaluation_handoff.json').exists() else None
    movie_evaluation=current_evaluation('reports/task4/evaluation_results.json',{'same_work','different_work'},handoff['packets']['movies']['evaluation_binding']['model_fingerprint']) if handoff else None
    person_evaluation=current_evaluation('reports/task5/evaluation_results.json',{'same_person','different_person'},handoff['packets']['people']['evaluation_binding']['model_fingerprint']) if handoff else None
    recovery_passed=recovery['status']=='passed' and recovery['same_input_repeat']['before_counts']==recovery['same_input_repeat']['after_counts']
    checks=[
        dict(requirement='Full review/product/user processing and reconciliation',status='passed' if final['counts']['raw_reviews']==7911684 and final['counts']['editions']==253059 and final['counts']['reviewers']==889176 else 'failed',evidence='reports/final/full_verification.json'),
        dict(requirement='Original compressed source SHA256 and exact original download time',status='partial',reason='No registered compressed-original checksum; download date known, exact original time NULL. Do not invent historical evidence.',evidence='reports/task1/source_manifest.reported.json'),
        dict(requirement='Full ASIN manifest and permitted acquisition with failures/coverage',status='passed_with_access_limit',reason='253059 statuses retained; HTML successes0, UCSD metadata73813 ASIN. Restriction evidence and alternative source scope disclosed.',evidence='reports/task2/full_verification.json'),
        dict(requirement='Content categories and all original review mappings',status='passed_engineering',reason='All products processed; unresolved classification not presented as verified movie census.',evidence='reports/task3/full_verification.json'),
        dict(requirement='At least5 actual cross-edition movie merge groups',status='passed' if t4['multifield_work_groups']>=5 else 'not_met',actual=t4['multifield_work_groups'],required=5,evidence='reports/task4/full_statistics.json'),
        dict(requirement='Five source-supported kept-separate movie groups',status='passed_source_examples_not_human_gold',reason='Five source-supported sequel pair groups from3 franchises. Assignment does not require five different franchises. No teaching/TV volumes or claim of HUMAN gold accuracy.',evidence='reports/task4/kept_separate_case_audit.json'),
        dict(requirement='Five substantive alias merge groups and five true-homonym examples',status='source_alias_cases_passed_homonym_scope_partial' if aliases['substantive_person_groups']>=5 else 'not_met',actual_substantive_alias_groups=aliases['substantive_person_groups'],reason='Contextual source-supported aliases audited; five source-context separation examples are from candidate catalog, not all final target people or independently human-confirmed.',evidence='reports/task5/substantive_alias_audit.json;reports/task5/kept_separate_case_audit.json'),
        dict(requirement='At least100 independently HUMAN labeled movie pairs and balanced classes',status='passed_declared_human_sample' if movie_evaluation else 'not_met',actual=movie_evaluation['labeled_pairs'] if movie_evaluation else t4['evaluation']['manually_labeled_pairs'],required=100,evidence='reports/task4/evaluation_results.json' if movie_evaluation else 'reports/task4/evaluation_status.json'),
        dict(requirement='At least100 independently HUMAN labeled person pairs and balanced classes',status='passed_declared_human_sample' if person_evaluation else 'not_met',actual=person_evaluation['pairs'] if person_evaluation else t5['human_evaluation']['labeled'],required=100,evidence='reports/task5/evaluation_results.json' if person_evaluation else 'reports/task5/full_statistics.json'),
        dict(requirement='Measured matching Precision Recall F1 errors and manual/unresolved coverage',status='reported_on_declared_human_sample' if movie_evaluation and person_evaluation else 'not_met',reason='Metrics remain NULL until legitimate labels supplied; structural replay and synthetic tests are not accuracy. Current evaluation reports must bind to unchanged labels/model.',evidence='src/quality/evaluate_task4.py;src/quality/evaluate_people.py'),
        dict(requirement='Identity-first release semantics precision and source selection',status='passed_engineering_with_missing_values',reason=str(final['date_semantics']['years'])+' release years; sourced work_review_observations stores earliest valid review separately, no invented full dates/regions/review upper bounds.',evidence='reports/task6/full_verification.json;sql/target_views.sql'),
        dict(requirement='All field candidates and sourced scalar/multivalue selections',status='passed_engineering',evidence='reports/task7/full_verification.json'),
        dict(requirement='Raw-review retention exact duplicate mapping and score denominator',status='passed_engineering',actual_raw=t8['raw_reviews'],actual_logical=t8['logical_reviews'],exact_duplicate_mappings=t8['exact_same_product_duplicate_records'],evidence='reports/task8/full_verification.json'),
        dict(requirement='Field-level source/operation/selection lineage and required reverse queries',status='passed_engineering',reason='Includes all four required directions, now person credits/alias history plus complete bound-entity results without thirty-row truncation.',evidence='reports/final/full_verification.json;sql/demo_queries.sql'),
        dict(requirement='Movie field coverage denominator and full product identification coverage',status='passed_corrected',reason=str(identified)+' independently multifield-identified works for movie information;'+str(final['counts']['movies'])+' all/provisional work records separately;'+str(identified_products)+'/'+str(final['counts']['editions'])+' product identification. Conditional coverage is not population identification.',evidence='reports/final/field_completeness.json'),
        dict(requirement='Person evaluation pool supports both identity-like and confusing distinct-context candidates',status='prepared_not_labeled',actual_pairs=read('reports/task5/evaluation_queue_audit.json')['pairs'],reason='60 cross-work,30 similar-name,30 initials;'+str(development_sources)+' development sources and5 alias case people excluded. Candidate buckets are NOT gold classes.',evidence='reports/task5/evaluation_queue_audit.json'),
        dict(requirement='Offline fixed raw-evidence sample covers ALL required difficult cases',status='source_case_categories_passed_not_human_gold' if read('reports/final/offline_sample_manifest.json').get('substantive_alias_groups',0)>=5 else 'partial',reason='Current fixed evidence includes actual contextual person aliases; source-case demonstrations do not substitute for independent HUMAN evaluation.',evidence='reports/final/offline_sample_manifest.json'),
        dict(requirement='Idempotent target load interruption recovery and versioned recomputation',status='passed_engineering' if recovery_passed else 'failed',evidence='reports/task9/idempotency_recovery.json;reports/final/offline_acceptance.json;reports/pipeline/last_execution.json'),
        dict(requirement='At least5 executable result queries',status='passed' if queries['status']=='passed' and len(queries['queries'])>=5 else 'failed',actual=len(queries['queries']),required=5,evidence='reports/final/query_verification.json'),
        dict(requirement='Report implementation model dictionary instructions AI use and no restricted raw data in Git',status='passed_artifacts_subject_to_final_git_audit',evidence='README.md;项目实施记录.md;reports/项目报告.md;reports/final/data_dictionary.json')]
    if handoff:checks.append(dict(requirement='Blinded editable human packets and explicit disagreement protocol',status='prepared_not_labeled' if not movie_evaluation or not person_evaluation else 'prepared_and_declared_evaluated',movie_pairs=handoff['packets']['movies']['reserved_pairs'],person_pairs=handoff['packets']['people']['reserved_pairs'],evidence='reports/final/human_evaluation_handoff.json;docs/人工评估与剩余验收.md',scope='Preparation is not completion of HUMAN evaluation.'))
    checks.append(dict(requirement='Submission boundary and recoverable archival of redundant versions',status='documented',evidence='docs/交付与恢复.md;reports/final/cleanup_manifest.json;reports/final/submission_verification.json'))
    result=dict(observed_at=utc_now(),status='not_fully_met',strict_assignment_acceptance=False,engineering_integrity=final['status'],checks=checks,
        required_user_or_permitted_new_evidence=(['Independent human movie and person labels (>=100 each, both classes, evidence, disagreements).'] if not movie_evaluation or not person_evaluation else [])+(['At least'+str(5-t4['multifield_work_groups'])+' additional genuinely supported cross-edition groups.'] if t4['multifield_work_groups']<5 else [])+['Review source-supported same-name/different-person examples and missing historical original-download evidence.'],
        nonfabrication='No AI-generated facts, fabricated page success, synthetic HUMAN labels or perfect-accuracy claims. Missing historical acquisition evidence remains unknown.')
    save_json(ROOT/'reports/final/requirements_audit.json',result);return result
def main():
    if hasattr(sys.stdout,'reconfigure'):sys.stdout.reconfigure(encoding='utf-8')
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--strict',action='store_true');a=p.parse_args();r=audit();print(js(r))
    if a.strict and not r['strict_assignment_acceptance']:raise SystemExit(1)
if __name__=='__main__':main()
