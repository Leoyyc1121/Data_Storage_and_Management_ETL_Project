"""Versioned source-evidence annotations, explicitly NOT independent human gold.

No annotation author, annotation timestamp, or human confirmation is required.
Raw source evidence and pair-level output remain local, outside public Git.
"""
import argparse
from collections import Counter
from contextlib import closing
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/quality'))
import prepare_review_drafts as draft
from integration_common import ro,save_json,hash_file,utc_now
from evaluate_task4 import metrics

FORBIDDEN={'labeler_type','labeled_by','labeled_at','reviewed','reviews','adjudication'}
ROW_KEYS=('number','pair_id','label','rationale','labeling_evidence','left','right')


def annotation_packet(kind,source,binding):
    items=[]
    for original in source['items']:
        item={key:deepcopy(original[key]) for key in ROW_KEYS}
        if item['label'] not in draft.KINDS[kind]:raise ValueError('Unknown source annotation label')
        items.append(item)
    return dict(format_version='source-evidence-annotations-v1',entity_kind=kind,
        status='completed_with_evidence_based_abstentions',method='assistant_source_evidence_annotation',
        independent_human_evaluation=False,source_evidence=binding,
        scope='Reference judgments based on retained raw sources. Unresolved is an explicit abstention, not a missing form. No human authorship or independent accuracy claim.',items=items)


def evidence_check(evidence):
    count=0
    for pairs in evidence['pairs'].values():
        for pair in pairs:
            for side in ('left','right'):
                for source in pair[side]['sources']:
                    if hashlib.sha256(source['raw_text'].encode('utf-8')).hexdigest()!=source['raw_sha256']:
                        raise ValueError('Retained raw source checksum mismatch')
                    count+=1
    return count


def validate(packet,pairs):
    kind=packet['entity_kind']
    if packet.get('format_version')!='source-evidence-annotations-v1' or packet.get('independent_human_evaluation') is not False or packet.get('method')!='assistant_source_evidence_annotation':
        raise ValueError('Source annotations must not claim independent human evaluation')
    if FORBIDDEN&set(packet):raise ValueError('Human metadata not part of this annotation schema')
    expected={p['number']:p for p in pairs};items=packet['items']
    if len(items)!=len(expected) or {i['number'] for i in items}!=set(expected):raise ValueError('Duplicate/missing annotation numbers')
    for item in items:
        if set(item)!=set(ROW_KEYS) or item['pair_id']!=expected[item['number']]['pair_id']:raise ValueError('Annotation schema/pair identity changed')
        if item['label'] not in draft.KINDS[kind] or not isinstance(item['rationale'],str) or len(item['rationale'].strip())<12:raise ValueError('Supported label and reason required')
        sources=[]
        for side in ('left','right'):
            source=expected[item['number']][side]
            if item[side]!=source['packet_context']:raise ValueError('Original side context changed')
            sources.extend(source['sources'])
        for source in sources:
            if source['source_record_id'] not in item['labeling_evidence'] or source['raw_sha256'] not in item['labeling_evidence']:
                raise ValueError('Missing raw source reference/checksum')
    return dict(Counter(i['label'] for i in items))


def build(directory,source_directory):
    directory=draft.location(directory);source_directory=draft.location(source_directory)
    if directory.exists():raise ValueError('Use a new version directory; never overwrite annotations')
    evidence=draft.read(source_directory/'evidence.json');evidence_check(evidence)
    packets={}
    for kind in draft.KINDS:
        source=draft.read(source_directory/(kind+'.review.json'))
        if any(i.get('reviewed') is not False or any(i.get(k) is not None for k in draft.HUMAN_FIELDS) for i in source['items']):
            raise ValueError('Actual human edits exist; preserve and handle them separately')
        packets[kind]=source
    directory.mkdir(parents=True)
    save_json(directory/'evidence.json',evidence)
    binding=dict(path='evidence.json',sha256=hash_file(directory/'evidence.json'))
    reports={}
    for kind,source in packets.items():
        packet=annotation_packet(kind,source,binding);counts=validate(packet,evidence['pairs'][kind])
        path=directory/(kind+'.annotations.json');save_json(path,packet)
        rendered=draft.render(packet,evidence['pairs'][kind]).splitlines()
        first=next(i for i,line in enumerate(rendered) if line.startswith('## '))
        body='\n'.join('标注：'+line[len('候选：'):] if line.startswith('候选：') else line for line in rendered[first:])
        markdown=directory/(kind+'.annotations.md')
        markdown.write_text('# '+('电影' if kind=='movies' else '人物')+'来源证据标注\n\n'
            '按保留来源形成的参考判断；并非独立人工金标准。无需填写姓名或标注时间。unresolved 表示证据不足，不能为凑数强判。完整原件位于 evidence.json。\n\n'+body,encoding='utf-8')
        reports[kind]=dict(pairs=len(packet['items']),label_counts=counts,determinate=len(packet['items'])-counts.get('unresolved',0),
            annotations=dict(path=str(path),sha256=hash_file(path)),evidence_cards=dict(path=str(markdown),sha256=hash_file(markdown)))
    report=dict(observed_at=utc_now(),status='completed_source_evidence_annotations',protocol='source-evidence-annotations-v1',
        packets=reports,annotation_author_and_time_required=False,human_metadata_fields_present=False,
        independent_human_evaluation=False,independent_accuracy_metrics=None,
        evidence=dict(path=str(directory/'evidence.json'),sha256=binding['sha256']),
        input_drafts={kind:dict(path=str(source_directory/(kind+'.review.json')),sha256=hash_file(source_directory/(kind+'.review.json'))) for kind in draft.KINDS},
        scope='User-selected source-assisted annotation deliverable. Does not satisfy the original independent HUMAN evaluation requirement. No changes to production ETL mappings.')
    save_json(ROOT/'reports/final/source_annotations.json',report)
    return report


def verify(directory):
    directory=draft.location(directory);evidence=draft.read(directory/'evidence.json');raw_count=evidence_check(evidence);packets={}
    registration=draft.read(ROOT/'reports/final/source_annotations.json')
    if hash_file(directory/'evidence.json')!=registration['evidence']['sha256']:raise ValueError('Registered evidence changed')
    for kind in draft.KINDS:
        registered=registration['packets'][kind]
        for suffix,key in [('annotations.json','annotations'),('annotations.md','evidence_cards')]:
            if hash_file(directory/(kind+'.'+suffix))!=registered[key]['sha256']:raise ValueError('Registered annotation changed; preserve a new version')
        packet=draft.read(directory/(kind+'.annotations.json'))
        if packet['source_evidence']!=dict(path='evidence.json',sha256=hash_file(directory/'evidence.json')):raise ValueError('Evidence binding changed')
        counts=validate(packet,evidence['pairs'][kind]);packets[kind]=dict(total=len(packet['items']),label_counts=counts)
    return dict(observed_at=utc_now(),status='passed_source_annotation_integrity',packets=packets,raw_record_hashes_checked=raw_count,
        annotation_author_and_time_fields_absent=True,independent_human_evaluation=False,scope='Integrity, complete annotation states and traceable raw evidence; not independently validated accuracy.')


def compare(directory):
    verification=verify(directory);directory=draft.location(directory)
    config=draft.read(ROOT/'config/integration.example.json');target=ROOT/config['target_dir']/'etl.sqlite3'
    target_registration=draft.read(ROOT/'reports/task9/load_statistics.json')
    if target.stat().st_size!=target_registration['file_size_bytes']:raise ValueError('Target size changed')
    result={};details=[]
    with closing(ro(target)) as db:
        states={r['key']:json.loads(r['value']) for r in db.execute('SELECT * FROM target_state')}
        if states.get('ready') is not True or states.get('input')!=target_registration['input_fingerprint']:raise ValueError('Published target fingerprint changed')
        for kind in draft.KINDS:
            rows=draft.read(directory/(kind+'.annotations.json'))['items'];reference=[];predicted=[];counts=Counter();provisional=0
            for item in rows:
                if item['label']=='unresolved':counts['reference_unresolved']+=1;continue
                values=[]
                for side in ('left','right'):
                    context=item[side]
                    if kind=='movies':
                        found=db.execute("SELECT movie_id FROM product_movie WHERE asin=? AND relationship='single_work'",(context['asin'],)).fetchall()
                        values.append(found[0][0] if len(found)==1 else None)
                    else:
                        found=db.execute('SELECT p.person_id,p.identity_status FROM person_aliases a JOIN people p USING(person_id) WHERE a.alias_id=?',(context['alias_id'],)).fetchone()
                        values.append(found[0] if found else None)
                        provisional+=bool(found and found[1]=='provisional_source_context')
                if None in values:
                    counts['model_unmapped_or_non_single_work']+=1;continue
                same=values[0]==values[1];truth=item['label']==('same_work' if kind=='movies' else 'same_person')
                reference.append(truth);predicted.append(same)
                details.append(dict(kind=kind,pair_id=item['pair_id'],source_label=item['label'],predicted_same=same,agrees=same==truth))
            result[kind]=dict(total_pairs=len(rows),reference_determinate=len(rows)-counts['reference_unresolved'],
                reference_unresolved=counts['reference_unresolved'],model_unmapped_or_non_single_work=counts['model_unmapped_or_non_single_work'],
                compared_pairs=len(reference),comparison_coverage=len(reference)/len(rows) if rows else None,
                provisional_person_sides=provisional,reference_agreement=metrics(reference,predicted))
    save_json(directory/'model_comparison.details.json',dict(comparisons=details))
    report=dict(observed_at=utc_now(),status='compared_to_source_assisted_reference_not_independent_accuracy',packets=result,
        independent_human_evaluation=False,scope='Diagnostic agreement on source-assisted judgments only. Shared source identifiers, repeated original credits and source overlap can inflate agreement; not independent accuracy or full-dataset performance. Unmapped/non-single-work cases are explicit model abstentions, not true negatives.',
        target_registration='reports/task9/load_statistics.json',target_input_fingerprint=target_registration['input_fingerprint'],
        annotation_hashes={kind:hash_file(directory/(kind+'.annotations.json')) for kind in draft.KINDS},annotation_verification=verification['status'])
    save_json(ROOT/'reports/final/source_annotation_agreement.json',report);return report


def main():
    if hasattr(sys.stdout,'reconfigure'):sys.stdout.reconfigure(encoding='utf-8')
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('action',choices=['build','verify','compare'])
    parser.add_argument('--directory',type=Path,default=ROOT/'data/evaluation/source_annotations_v1')
    parser.add_argument('--source-directory',type=Path,default=ROOT/'data/evaluation/review_draft_v1')
    args=parser.parse_args()
    if args.action=='build':result=build(args.directory,args.source_directory)
    elif args.action=='compare':result=compare(args.directory)
    else:
        result=verify(args.directory);save_json(ROOT/'reports/final/source_annotation_verification.json',result)
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
