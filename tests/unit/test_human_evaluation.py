"""Synthetic evaluator tests, NOT actual human annotation or measured accuracy."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/quality'))
from human_evaluation import final_judgment, load_labels, verify_binding, coverage
from prepare_human_evaluation import blinded_packet


def sample(label='same_person',name='Synthetic test reviewer'):
    return dict(label=label,labeler_type='human',labeled_by=name,labeled_at='2026-10-19T00:00:00+08:00',labeling_evidence='Synthetic unit-test evidence only',rationale='Synthetic validator fixture, never a project human label')


class HumanEvaluationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name)/'labels.json'

    def load(self,rows,minimum=2):
        self.path.write_text(json.dumps(dict(pairs=rows)),encoding='utf-8')
        return load_labels(self.path,{'same_person','different_person'},minimum)

    def test_direct_legacy_human_judgment_supported(self):
        row=dict(pair_id='a',**sample())
        self.assertEqual(final_judgment(row,{'same_person','different_person'}),(row,False))

    def test_pending_rows_do_not_become_gold(self):
        _,done,progress=self.load([dict(pair_id='a',**sample()),dict(pair_id='b',**sample('different_person')),dict(pair_id='c',label=None)])
        self.assertEqual(len(done),2);self.assertEqual(progress['not_yet_reviewed_pairs'],1)

    def test_unresolved_human_review_counted_separately(self):
        _,done,progress=self.load([dict(pair_id='a',**sample()),dict(pair_id='b',**sample('different_person')),dict(pair_id='c',**sample('unresolved'))])
        self.assertEqual(len(done),2);self.assertEqual(progress['unresolved_human_pairs'],1)
        self.assertEqual(coverage(progress,4)['manual_review_coverage'],0.75)
        self.assertEqual(coverage(progress,4)['human_unresolved_rate'],1/3)

    def test_empty_labels_cannot_produce_metrics(self):
        with self.assertRaises(ValueError):self.load([dict(pair_id='a',label=None)])

    def test_both_gold_classes_required(self):
        with self.assertRaises(ValueError):self.load([dict(pair_id='a',**sample()),dict(pair_id='b',**sample())])

    def test_ai_or_missing_evidence_rejected(self):
        for field,value in [('labeler_type','AI'),('labeling_evidence',''),('rationale',' '),('labeled_at','2026-10-19T00:00:00')]:
            row=dict(pair_id='a',**sample());row[field]=value
            with self.assertRaises(ValueError):final_judgment(row,{'same_person','different_person'})

    def test_unknown_label_not_silently_discarded(self):
        with self.assertRaises(ValueError):self.load([dict(pair_id='a',**sample('maybe'))])

    def test_duplicate_pending_ids_rejected(self):
        with self.assertRaises(ValueError):self.load([dict(pair_id='a',label=None),dict(pair_id='a',label=None)])

    def test_agreeing_independent_reviews(self):
        row=dict(pair_id='a',label=None,reviews=[sample(name='Fixture reviewer A'),sample(name='Fixture reviewer B')])
        value,dispute=final_judgment(row,{'same_person','different_person'})
        self.assertEqual(value['label'],'same_person');self.assertFalse(dispute)

    def test_same_reviewer_not_double_annotation(self):
        with self.assertRaises(ValueError):final_judgment(dict(reviews=[sample(),sample()]),{'same_person','different_person'})

    def test_disagreement_without_adjudication_rejected(self):
        row=dict(reviews=[sample(),sample('different_person','Fixture reviewer B')])
        with self.assertRaises(ValueError):final_judgment(row,{'same_person','different_person'})

    def test_disagreement_human_resolution_retained(self):
        row=dict(label=None,reviews=[sample(),sample('different_person','Fixture reviewer B')],adjudication=sample('different_person','Fixture adjudicator'))
        value,dispute=final_judgment(row,{'same_person','different_person'})
        self.assertTrue(dispute);self.assertEqual(value['label'],'different_person')

    def test_final_label_cannot_override_reviews(self):
        with self.assertRaises(ValueError):final_judgment(dict(label='different_person',reviews=[sample()]),{'same_person','different_person'})

    def test_registry_and_model_bindings(self):
        p=dict(evaluation_binding=dict(registry_sha256='a',model_fingerprint='b'))
        verify_binding(p,'a','b')
        with self.assertRaises(ValueError):verify_binding(p,'other','b')

    def test_blinding_removes_model_ids_and_leaves_labels_empty(self):
        side=dict(alias_id='a',raw_name='Synthetic Doe',nconst='synthetic-n',person_id='prediction',identity_status='prediction',birth_year=1900,death_year=1999,classification='prediction',source_record_id='synthetic-source')
        source=dict(pairs=[dict(pair_id='p',stratum='identity-like',left=side,right=deepcopy(side),label='same_person')])
        value=blinded_packet(source,'person','hash','model')
        row=value['pairs'][0]
        self.assertIsNone(row['label']);self.assertEqual(row['reviews'],[])
        self.assertNotIn('stratum',row)
        for key in ('person_id','nconst','identity_status','birth_year','death_year','classification'):self.assertNotIn(key,row['left'])
        self.assertEqual(row['left']['source_record_id'],'synthetic-source')
        self.assertEqual(source['pairs'][0]['left']['nconst'],'synthetic-n')


if __name__=='__main__':unittest.main()
