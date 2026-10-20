"""Synthetic draft checks; no actual project HUMAN annotations are generated."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/quality'))
import prepare_review_drafts as drafts
from human_evaluation import load_labels


def side(name='Synthetic Example',nid='nm9990001',birth=1900,role='cast',source_id='imdb:title.principals.tsv.gz'):
    text=f'tt9990001\t1\t{nid}\tactor\t\\N\t["Fixture character"]'
    return dict(packet_context=dict(raw_name=name,role=role),sources=[dict(
        source_id=source_id,source_record_id='fixture-source',raw_sha256=hashlib.sha256(text.encode()).hexdigest(),
        record_sequence=1,raw_text=text,published_title=dict(primary_title='Synthetic fixture',release_year=2000),
        published_credit_names=[dict(nconst=nid,primary_name=name,birth_year=birth)])])


def pair():return dict(number=1,pair_id='synthetic-pair',left=side(),right=side())
def decision():return [dict(number=1,label='same_person',reason='Synthetic source-supported candidate fixture, never a human judgment.')]


class ReviewDraftTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.directory=self.root/'data/evaluation/review'
        self.directory.mkdir(parents=True)

    def test_original_pair_not_mutated(self):
        source=pair();before=deepcopy(source)
        drafts.review_packet('people',[source],decision(),{})
        self.assertEqual(source,before)

    def test_candidates_have_no_human_name_or_time(self):
        value=drafts.review_packet('people',[pair()],decision(),{})['items'][0]
        self.assertIs(value['reviewed'],False)
        for field in drafts.HUMAN_FIELDS:self.assertIsNone(value[field])
        self.assertIn('sha256=',value['labeling_evidence'])

    def test_draft_not_accepted_as_formal_human_packet(self):
        path=self.root/'draft.json';path.write_text(json.dumps(drafts.review_packet('people',[pair()],decision(),{})),encoding='utf-8')
        with self.assertRaises(ValueError):load_labels(path,{'same_person','different_person'},1)

    def test_all_pair_numbers_required(self):
        with self.assertRaises(ValueError):drafts.validate_decisions([], [pair()], 'people')
        wrong=decision();wrong[0]['number']=2
        with self.assertRaises(ValueError):drafts.validate_decisions(wrong,[pair()],'people')

    def test_duplicate_numbers_rejected(self):
        second=dict(pair(),number=2)
        with self.assertRaises(ValueError):drafts.validate_decisions(decision()*2,[pair(),second],'people')

    def test_label_and_reason_checked(self):
        for change in [dict(label='unknown'),dict(reason='short'),dict(reason=None)]:
            invalid=decision();invalid[0].update(change)
            with self.assertRaises(ValueError):drafts.validate_decisions(invalid,[pair()],'people')

    def test_original_credit_same_identity(self):
        self.assertEqual(drafts.person_candidates([pair()])[0]['label'],'same_person')
        self.assertIn('不是两份独立证据',drafts.person_candidates([pair()])[0]['reason'])

    def test_original_credit_distinct_full_names(self):
        p=pair();p['right']=side('Other Fixture','nm9990002',1950)
        candidate=drafts.person_candidates([p])[0]
        self.assertEqual(candidate['label'],'different_person')
        self.assertIn('nm9990001',candidate['reason']);self.assertIn('nm9990002',candidate['reason'])

    def test_same_name_distinct_ids_not_automatically_different(self):
        p=pair();p['right']=side(nid='nm9990002')
        self.assertEqual(drafts.person_candidates([p])[0]['label'],'unresolved')

    def test_missing_source_identity_stays_pending(self):
        p=pair();p['right']['sources'][0]['published_credit_names']=[]
        self.assertEqual(drafts.person_candidates([p])[0]['label'],'unresolved')

    def test_writer_not_misread_as_director(self):
        p=pair();source=p['left']['sources'][0]
        source['source_id']='imdb:title.crew.tsv.gz';p['left']['packet_context']['role']='director'
        source['raw_text']='tt9990001\tnm9990002\tnm9990001'
        self.assertEqual(drafts.person_candidates([p])[0]['label'],'unresolved')

    def test_reference_name_must_match_packet_name(self):
        p=pair();p['left']['packet_context']['raw_name']='Unrelated Fixture'
        self.assertEqual(drafts.person_candidates([p])[0]['label'],'unresolved')

    def test_raw_source_hash_checked(self):
        db=sqlite3.connect(':memory:');self.addCleanup(db.close);db.row_factory=sqlite3.Row
        db.execute('CREATE TABLE source_records(source_record_id TEXT,raw_record BLOB,raw_sha256 TEXT)')
        db.execute('INSERT INTO source_records VALUES(?,?,?)',('fixture',b'original','wrong'))
        with self.assertRaises(ValueError):drafts.raw_source(db,'fixture')
        with self.assertRaises(ValueError):drafts.raw_source(db,'missing')

    def test_location_cannot_point_outside_evaluation(self):
        with patch.object(drafts,'ROOT',self.root):
            self.assertEqual(drafts.location(self.directory),self.directory)
            with self.assertRaises(ValueError):drafts.location(self.root/'data')
            with self.assertRaises(ValueError):drafts.location(self.root/'data/evaluation')

    def test_evidence_export_never_overwrites(self):
        (self.directory/'evidence.json').write_text('{}',encoding='utf-8')
        with patch.object(drafts,'ROOT',self.root):
            with self.assertRaises(ValueError):drafts.prepare(self.directory)

    def setup_check(self):
        evidence=dict(pairs={kind:[pair()] for kind in drafts.KINDS})
        (self.directory/'evidence.json').write_text(json.dumps(evidence),encoding='utf-8')
        packets={}
        for kind in drafts.KINDS:
            choices=decision()
            if kind=='movies':choices[0]['label']='same_work'
            packet=drafts.review_packet(kind,[pair()],choices,{})
            packets[kind]=packet
            (self.directory/(kind+'.review.json')).write_text(json.dumps(packet),encoding='utf-8')
        return packets

    def test_initial_check_counts_zero_completed(self):
        self.setup_check()
        with patch.object(drafts,'ROOT',self.root):result=drafts.check(self.directory)
        for packet in result['packets'].values():
            self.assertEqual(packet['pending'],1);self.assertEqual(packet['human_determinate'],0)
            self.assertEqual(packet['problems'],[])
        self.assertIs(result['independent_human_requirement_satisfied'],False)

    def test_reviewed_without_identity_cannot_count_as_human(self):
        packets=self.setup_check();packets['people']['items'][0]['reviewed']=True
        (self.directory/'people.review.json').write_text(json.dumps(packets['people']),encoding='utf-8')
        with patch.object(drafts,'ROOT',self.root):result=drafts.check(self.directory)['packets']['people']
        self.assertEqual(result['human_determinate'],0);self.assertEqual(len(result['problems']),1)

    def test_null_label_with_identity_cannot_count(self):
        packets=self.setup_check();item=packets['people']['items'][0]
        item.update(reviewed=True,label=None,labeler_type='human',labeled_by='Synthetic test only',labeled_at='2026-10-19T00:00:00+08:00')
        (self.directory/'people.review.json').write_text(json.dumps(packets['people']),encoding='utf-8')
        with patch.object(drafts,'ROOT',self.root):result=drafts.check(self.directory)['packets']['people']
        self.assertEqual(result['human_determinate'],0);self.assertEqual(len(result['problems']),1)

    def test_markdown_shows_source_but_has_no_per_row_ai_badge(self):
        p=pair();packet=drafts.review_packet('people',[p],decision(),{})
        value=drafts.render(packet,[p])
        self.assertIn('fixture-source',value);self.assertIn('Synthetic fixture',value)
        self.assertNotIn('AI辅助',value)

    def test_assemble_preserves_original_and_refuses_replacement(self):
        bindings={}
        for kind in drafts.KINDS:
            path=self.root/(kind+'.original.json');path.write_text('{"pairs":[]}',encoding='utf-8')
            bindings[kind]=dict(path=str(path),sha256=drafts.hash_file(path))
        evidence=dict(pairs={kind:[pair()] for kind in drafts.KINDS},original_packets=bindings)
        (self.directory/'evidence.json').write_text(json.dumps(evidence),encoding='utf-8')
        choices=dict(people=decision(),movies=[dict(decision()[0],label='same_work')])
        decisions=self.directory/'decisions.json';decisions.write_text(json.dumps(choices),encoding='utf-8')
        with patch.object(drafts,'ROOT',self.root):
            result=drafts.assemble(self.directory,decisions)
            digest=drafts.hash_file(self.directory/'people.review.json')
            with self.assertRaises(ValueError):drafts.assemble(self.directory,decisions)
        self.assertEqual(drafts.hash_file(self.directory/'people.review.json'),digest)
        for binding in bindings.values():self.assertEqual(drafts.hash_file(binding['path']),binding['sha256'])
        self.assertEqual(result['human_labels_generated'],0)

    def test_completed_unresolved_still_not_a_determinate_label(self):
        packets=self.setup_check();item=packets['people']['items'][0]
        item.update(reviewed=True,label='unresolved',labeler_type='human',labeled_by='Synthetic unit fixture only',labeled_at='2026-10-19T00:00:00+08:00')
        (self.directory/'people.review.json').write_text(json.dumps(packets['people']),encoding='utf-8')
        with patch.object(drafts,'ROOT',self.root):result=drafts.check(self.directory)['packets']['people']
        self.assertEqual(result['human_determinate'],0);self.assertEqual(result['human_unresolved'],1)
        self.assertEqual(result['problems'],[])


if __name__=='__main__':unittest.main()
