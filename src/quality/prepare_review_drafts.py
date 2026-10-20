"""Source-only candidate review packets, separate from untouched HUMAN holdouts.

Candidate decisions are supplied separately after inspecting source evidence.
No production grouping/person predictions are read and no HUMAN identity is
generated. Local source excerpts stay under the ignored data/evaluation tree.
"""
import argparse
from collections import Counter
from contextlib import closing
from copy import deepcopy
import json
import html
from pathlib import Path
import re
import sys
import unicodedata

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import ro,save_json,hash_file,utc_now

KINDS={'movies':{'same_work','different_work','unresolved'},'people':{'same_person','different_person','unresolved'}}
HUMAN_FIELDS=('labeler_type','labeled_by','labeled_at')


def read(path):return json.loads(Path(path).read_text(encoding='utf-8-sig'))


def location(value):
    path=Path(value).resolve()
    path.relative_to((ROOT/'data/evaluation').resolve())
    if path==(ROOT/'data/evaluation').resolve():raise ValueError('Dedicated review directory required')
    return path


def raw_source(db,sid):
    row=db.execute('SELECT * FROM source_records WHERE source_record_id=?',(sid,)).fetchone()
    if row is None:raise ValueError('Missing retained source '+sid)
    value=dict(row);raw=value.pop('raw_record')
    import hashlib
    if hashlib.sha256(raw).hexdigest()!=value['raw_sha256']:raise ValueError('Raw evidence checksum mismatch')
    value['raw_text']=raw.decode('utf-8')
    return value


def context(item,db,catalog):
    ids=[item['source_record_id']] if 'source_record_id' in item else [r['source_record_id'] for r in item['records']]
    sources=[]
    for sid in dict.fromkeys(ids):
        source=raw_source(db,sid);text=source['raw_text']
        if source['source_id'].startswith('imdb:'):
            columns=text.rstrip('\r\n').split('\t');tid=columns[0]
            title=catalog.execute('SELECT * FROM titles WHERE tconst=?',(tid,)).fetchone()
            source['published_title']=dict(title) if title else None
            people=[]
            # This is an original credited nconst, not the ETL person_id or a
            # selected cross-product identity prediction.
            for nid in sorted(set(re.findall(r'\bnm\d+\b',text))):
                person=catalog.execute('SELECT * FROM names WHERE nconst=?',(nid,)).fetchone()
                if person:people.append(dict(person))
            source['published_credit_names']=people
            source['column_order']={'imdb:title.crew.tsv.gz':['tconst','directors','writers'],
                'imdb:title.principals.tsv.gz':['tconst','ordering','nconst','category','job','characters']}.get(source['source_id'])
        else:
            source['product_record']=json.loads(text)
        sources.append(source)
    return dict(packet_context=deepcopy(item),sources=sources)


def prepare(directory):
    directory=location(directory)
    if (directory/'evidence.json').exists():raise ValueError('Preserve existing evidence; choose another directory')
    c=read(ROOT/'config/integration.example.json');imdb=read(ROOT/c['imdb_config'])
    handoff=read(ROOT/'reports/final/human_evaluation_handoff.json');packets={};bindings={}
    with closing(ro(ROOT/c['target_dir']/'etl.sqlite3')) as db,closing(ro(ROOT/imdb['work_dir']/'catalog.sqlite3')) as catalog:
        for kind in KINDS:
            path=Path(handoff['packets'][kind]['path']);packet=read(path)
            if hash_file(path)!=handoff['packets'][kind]['sha256']:raise ValueError('Original annotation packet changed; do not overwrite actual labels')
            if any(p.get('label') is not None or p.get('reviews') or any(p.get(k) is not None for k in HUMAN_FIELDS) for p in packet['pairs']):raise ValueError('Human annotations already present; preserve their work')
            bindings[kind]=dict(path=str(path),sha256=hash_file(path),evaluation_binding=packet['evaluation_binding'])
            packets[kind]=[dict(number=i,pair_id=p['pair_id'],left=context(p['left'],db,catalog),right=context(p['right'],db,catalog)) for i,p in enumerate(packet['pairs'],1)]
    result=dict(status='source_evidence_for_pending_review',observed_at=utc_now(),original_packets=bindings,pairs=packets,
        source_scope='Retained raw metadata/TSV, original published names and titles only. Production work/person predictions are not consulted. Not HUMAN labels.')
    save_json(directory/'evidence.json',result)
    return dict(status=result['status'],pairs={k:len(v) for k,v in packets.items()},path=str(directory/'evidence.json'))


def show(directory,kind,start,count,chars):
    data=read(location(directory)/'evidence.json')
    for pair in data['pairs'][kind][start-1:start-1+count]:
        print('\nPAIR',pair['number'],pair['pair_id'])
        for side in ('left','right'):
            item=pair[side];packet=item['packet_context']
            print(side,packet.get('raw_name',packet.get('raw_titles')),packet.get('role',''))
            seen=set()
            for source in item['sources']:
                if source['raw_sha256'] in seen:continue
                seen.add(source['raw_sha256'])
                if 'product_record' in source:
                    raw=source['product_record'];description=raw.get('description',[])
                    if isinstance(description,str):description=[description]
                    paragraphs=[' '.join(html.unescape(re.sub(r'<[^>]*>',' ',str(x))).split()) for x in description]
                    prose=' | '.join(x for x in paragraphs if len(x.split())>=8)
                    print(json.dumps(dict(source_id=source['source_record_id'],title=raw.get('title'),
                        description=prose[:chars],description_chars=len(prose),details=raw.get('details'),brand=raw.get('brand')),ensure_ascii=False))
                else:
                    title=source.get('published_title')
                    print(json.dumps(dict(source_id=source['source_record_id'],raw_credit=source['raw_text'].strip(),
                        title={k:title[k] for k in ('tconst','primary_title','release_year')} if title else None,
                        names=[{k:n[k] for k in ('nconst','primary_name','birth_year')} for n in source.get('published_credit_names',[])]),ensure_ascii=False))


def validate_decisions(decisions,source_pairs,kind):
    if not isinstance(decisions,list) or len(decisions)!=len(source_pairs):raise ValueError('One judgment per source pair required')
    by_number={d['number']:d for d in decisions}
    if set(by_number)!={p['number'] for p in source_pairs} or len(by_number)!=len(decisions):raise ValueError('Duplicate/missing pair numbers')
    for p in source_pairs:
        decision=by_number[p['number']]
        if decision['label'] not in KINDS[kind] or not isinstance(decision.get('reason'),str) or len(decision['reason'].strip())<12:
            raise ValueError('A supported candidate label and concrete reasoning required')
    return by_number


def person_candidates(pairs):
    """Reference-credit suggestions, never independent human ground truth."""
    def normalized(name):
        return ''.join(c for c in unicodedata.normalize('NFKC',name).casefold() if c.isalnum())
    def profile(item):
        packet=item['packet_context'];found={}
        for source in item['sources']:
            columns=source['raw_text'].strip().split('\t')
            if source['source_id']=='imdb:title.principals.tsv.gz' and len(columns)>=4:
                eligible={columns[2]}
            elif source['source_id']=='imdb:title.crew.tsv.gz' and len(columns)>=3 and packet['role']=='director':
                eligible=set(columns[1].split(','))
            else:continue
            for name in source.get('published_credit_names',[]):
                if name['nconst'] in eligible and normalized(name['primary_name'])==normalized(packet['raw_name']):
                    title=source.get('published_title') or {}
                    found[name['nconst']]=dict(name,work=title.get('primary_title'),year=title.get('release_year'))
        return next(iter(found.values())) if len(found)==1 else None
    results=[]
    for pair in pairs:
        left,right=profile(pair['left']),profile(pair['right'])
        if left and right:
            def describe(value):
                return f"{value['primary_name']}（原始人物号 {value['nconst']}，出生年 {value['birth_year'] if value['birth_year'] is not None else '未提供'}；署名作品 {value['work']} / {value['year']}）"
            if left['nconst']==right['nconst']:
                label='same_person';reason=f"左侧 {describe(left)}；右侧 {describe(right)}。原始信用行的人物号、姓名一致，候选为同一人物；不同角色/片名不代表不同人物。"
                if pair['left']['sources'][0]['raw_sha256']==pair['right']['sources'][0]['raw_sha256']:
                    reason+=' 两侧重用了同一条信用原件，不是两份独立证据。'
            elif normalized(left['primary_name'])!=normalized(right['primary_name']):
                label='different_person';reason=f"左侧 {describe(left)}；右侧 {describe(right)}。原始档案分别署名，完整姓名及人物号均不同，候选为不同人物；不能因姓氏或首字母相似合并。"
            else:
                label='unresolved';reason='完整姓名相同但原始人物号不同，可能存在同名者或来源重复档案，现有材料不能独立裁决。'
        else:
            label='unresolved';reason=f"左侧姓名 {pair['left']['packet_context']['raw_name']}，右侧姓名 {pair['right']['packet_context']['raw_name']}；至少一侧缺少可直接核对的唯一原始人物信用，不能只凭姓名猜测身份。"
        results.append(dict(number=pair['number'],label=label,reason=reason))
    return results


def review_packet(kind,source_pairs,decisions,binding):
    mapping=validate_decisions(decisions,source_pairs,kind);items=[]
    for p in source_pairs:
        choice=mapping[p['number']]
        refs=[s['source_record_id']+'; sha256='+s['raw_sha256'] for side in ('left','right') for s in p[side]['sources']]
        items.append(dict(number=p['number'],pair_id=p['pair_id'],label=choice['label'],
            rationale=choice['reason'],reviewed=False,labeler_type=None,labeled_by=None,labeled_at=None,
            labeling_evidence='\n'.join(refs),left=deepcopy(p['left']['packet_context']),right=deepcopy(p['right']['packet_context'])))
    return dict(format_version='source-review-draft-v1',entity_kind=kind,status='pending_individual_human_verification',
        original_annotation_packet=binding,annotation_protocol='candidate_visible_review_not_independent_blind_annotation',
        instructions='Inspect raw sources, change label/reason if needed, set reviewed=true and fill actual HUMAN identity/time only after checking. This draft is not the original independent-human evaluation packet.',items=items)


def render(packet,source_pairs):
    sources_by_number={p['number']:p for p in source_pairs}
    out=['# '+('电影' if packet['entity_kind']=='movies' else '人物')+'逐对核查材料','',
        '待核查草稿。正式人工评估包没有改动；候选判断不是已完成人工标签。逐对核查后在同名 review.json 中填写 reviewed、labeler_type、labeled_by、labeled_at，必要时修改 label、rationale 和 labeling_evidence。','']
    for item in packet['items']:
        out += [f"## {item['number']:03d} · {item['pair_id']}",'',f"候选：`{item['label']}`",'',item['rationale'],'']
        for side,label in [('left','左侧'),('right','右侧')]:
            out += ['### '+label,'']
            context=sources_by_number[item['number']][side]
            out += ['```json',json.dumps(context['packet_context'],ensure_ascii=False,indent=2),'```','']
            for s in context['sources']:
                out += [f"原件：`{s['source_record_id']}`；来源：`{s['source_id']}`；记录序号：{s['record_sequence']}",'',f"SHA-256：`{s['raw_sha256']}`",'']
                if 'product_record' in s:
                    raw=s['product_record'];descriptions=raw.get('description',[])
                    if isinstance(descriptions,str):descriptions=[descriptions]
                    out += ['商品原件正文（只去除 HTML 排版；字节原件保留在 evidence.json）：','','```text',
                        'title: '+str(raw.get('title')),'brand: '+str(raw.get('brand')),
                        'details: '+json.dumps(raw.get('details'),ensure_ascii=False),
                        *[' '.join(re.sub(r'<[^>]*>',' ',html.unescape(str(x))).split()) for x in descriptions],'```','']
                else:out += ['原始 TSV 信用行：','','```text',s['raw_text'].strip(),'```','']
                if s.get('published_title'):out += ['原始档案题名/年份：','', '```json',json.dumps(s['published_title'],ensure_ascii=False,indent=2),'```','']
                if s.get('published_credit_names'):out += ['原始档案信用姓名（不是系统人物合并结果）：','','```json',json.dumps(s['published_credit_names'],ensure_ascii=False,indent=2),'```','']
    return '\n'.join(out)


def assemble(directory,decisions_path):
    directory=location(directory);evidence=read(directory/'evidence.json');decisions=read(decisions_path)
    if 'people' not in decisions:
        choices=person_candidates(evidence['pairs']['people'])
        overrides=decisions.get('people_overrides',[])
        numbers={d['number'] for d in overrides}
        if len(numbers)!=len(overrides) or not numbers.issubset({p['number'] for p in choices}):
            raise ValueError('Duplicate/unknown person override number')
        mapping={d['number']:d for d in overrides}
        decisions['people']=[mapping.get(d['number'],d) for d in choices]
    files={}
    for kind in KINDS:
        binding=evidence['original_packets'][kind]
        if hash_file(binding['path'])!=binding['sha256']:raise ValueError('Original HUMAN packet changed')
        packet=review_packet(kind,evidence['pairs'][kind],decisions[kind],binding)
        for suffix in ('review.json','review.md'):
            if (directory/(kind+'.'+suffix)).exists():raise ValueError('Never overwrite a review file; choose a new directory')
        files[kind]=packet
    reports={}
    for kind,packet in files.items():
        path=directory/(kind+'.review.json');save_json(path,packet)
        # Generated evidence material, not an edit to repository source files.
        markdown=directory/(kind+'.review.md');markdown.write_text(render(packet,evidence['pairs'][kind]),encoding='utf-8')
        reports[kind]=dict(pairs=len(packet['items']),candidate_counts=dict(Counter(i['label'] for i in packet['items'])),
            human_completed=0,all_human_identity_time_fields_blank=all(all(i[k] is None for k in HUMAN_FIELDS) and i['reviewed'] is False for i in packet['items']),
            json=dict(path=str(path),sha256=hash_file(path)),evidence_markdown=dict(path=str(markdown),sha256=hash_file(markdown)))
    report=dict(observed_at=utc_now(),status='candidate_drafts_ready_NOT_completed_HUMAN_annotation',prepared_by='Codex project assistant',
        packets=reports,original_blind_packets_preserved=evidence['original_packets'],human_labels_generated=0,
        independent_human_requirement_satisfied=False,metrics=dict(precision=None,recall=None,f1=None),
        scope='Source-reviewed candidates for actual human checking. Generated evidence and per-pair judgments stay local. Candidate-visible review must not be reported as independent blind annotation.')
    save_json(ROOT/'reports/final/review_draft_handoff.json',report)
    return report


def check(directory):
    from human_evaluation import final_judgment
    directory=location(directory);evidence=read(directory/'evidence.json');results={}
    for kind in KINDS:
        packet=read(directory/(kind+'.review.json'))
        if packet.get('format_version')!='source-review-draft-v1' or packet.get('entity_kind')!=kind:raise ValueError('Unexpected draft format')
        expected={p['number']:p['pair_id'] for p in evidence['pairs'][kind]}
        items=packet['items'];actual={i['number']:i['pair_id'] for i in items}
        if actual!=expected or len(actual)!=len(items):raise ValueError('Draft pair identity/coverage changed')
        counts=Counter();problems=[]
        for item in items:
            if item.get('label') not in KINDS[kind]:
                problems.append(dict(number=item['number'],error='A valid candidate/final label is required'));continue
            if item.get('reviewed') is False:
                counts['pending']+=1
                if any(item.get(k) is not None for k in HUMAN_FIELDS):problems.append(dict(number=item['number'],error='Identity/time filled but reviewed is still false'))
                continue
            try:
                if item.get('reviewed') is not True:raise ValueError('reviewed must be a JSON boolean')
                judgment,_=final_judgment(item,KINDS[kind]-{'unresolved'})
                if judgment is None:raise ValueError('Completed review needs a label')
                counts['human_unresolved' if judgment['label']=='unresolved' else 'human_determinate']+=1
            except (ValueError,TypeError,KeyError) as error:problems.append(dict(number=item['number'],error=str(error)))
        results[kind]=dict(total=len(items),pending=counts['pending'],human_determinate=counts['human_determinate'],human_unresolved=counts['human_unresolved'],problems=problems)
    return dict(status='review_progress_only_not_independent_evaluation',packets=results,independent_human_requirement_satisfied=False)


def main():
    if hasattr(sys.stdout,'reconfigure'):sys.stdout.reconfigure(encoding='utf-8')
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('action',choices=['evidence','show','assemble','check'])
    p.add_argument('--directory',type=Path,default=ROOT/'data/evaluation/review_draft_v1');p.add_argument('--kind',choices=list(KINDS),default='movies')
    p.add_argument('--start',type=int,default=1);p.add_argument('--count',type=int,default=20);p.add_argument('--chars',type=int,default=1400);p.add_argument('--decisions',type=Path)
    a=p.parse_args()
    if a.action=='show':show(a.directory,a.kind,a.start,a.count,a.chars);return
    if a.action=='check':result=check(a.directory)
    else:result=prepare(a.directory) if a.action=='evidence' else assemble(a.directory,a.decisions or a.directory/'decisions.json')
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
