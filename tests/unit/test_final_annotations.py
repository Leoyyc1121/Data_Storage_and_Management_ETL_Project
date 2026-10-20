"""Synthetic checks only; fixtures are not actual project reference labels."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZipFile

ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/quality'))
import finalize_annotations as final
import package_delivery as delivery
from test_review_drafts import pair,decision


class FinalAnnotationTests(unittest.TestCase):
    def packet(self):
        source=final.draft.review_packet('people',[pair()],decision(),{})
        return final.annotation_packet('people',source,dict(path='evidence.json',sha256='fixture'))

    def test_author_and_time_not_in_schema(self):
        value=self.packet()
        self.assertFalse(set(value)&final.FORBIDDEN)
        self.assertFalse(set(value['items'][0])&final.FORBIDDEN)
        self.assertIs(value['independent_human_evaluation'],False)
        self.assertEqual(final.validate(value,[pair()]),{'same_person':1})

    def test_source_schema_has_stable_key_order(self):
        self.assertEqual(tuple(self.packet()['items'][0]),final.ROW_KEYS)

    def test_original_draft_not_modified(self):
        source=final.draft.review_packet('people',[pair()],decision(),{});before=deepcopy(source)
        final.annotation_packet('people',source,{})
        self.assertEqual(source,before)

    def test_unknown_or_human_origin_cannot_pass(self):
        for changes in [dict(independent_human_evaluation=True),dict(method='independent_human'),dict(labeled_by='Synthetic')]:
            value=self.packet();value.update(changes)
            with self.assertRaises(ValueError):final.validate(value,[pair()])

    def test_wrong_label_missing_reason_or_added_human_field_rejected(self):
        for changes in [dict(label='unknown'),dict(rationale='short'),dict(labeled_at=None)]:
            value=self.packet();value['items'][0].update(changes)
            with self.assertRaises(ValueError):final.validate(value,[pair()])

    def test_source_context_and_reference_required(self):
        for field,value in [('left',{}),('labeling_evidence','missing'),('pair_id','wrong')]:
            packet=self.packet();packet['items'][0][field]=value
            with self.assertRaises(ValueError):final.validate(packet,[pair()])

    def test_duplicate_number_not_accepted(self):
        value=self.packet();value['items']*=2
        with self.assertRaises(ValueError):final.validate(value,[pair()])

    def test_abstention_is_a_completed_annotation_state(self):
        value=self.packet();value['items'][0]['label']='unresolved'
        self.assertEqual(final.validate(value,[pair()]),{'unresolved':1})

    def test_source_byte_hash_checked(self):
        value=dict(pairs=dict(people=[pair()]))
        self.assertEqual(final.evidence_check(value),2)
        value['pairs']['people'][0]['left']['sources'][0]['raw_text']='changed'
        with self.assertRaises(ValueError):final.evidence_check(value)

    def test_build_verify_compare_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'data/evaluation/source';source.mkdir(parents=True)
            pairs={kind:[pair()] for kind in final.draft.KINDS}
            for kind,rows in pairs.items():
                for side in ('left','right'):
                    rows[0][side]['packet_context'].update(asin='fixture-'+side,alias_id='fixture-'+side)
                choices=decision()
                if kind=='movies':choices[0]['label']='same_work'
                packet=final.draft.review_packet(kind,rows,choices,{})
                (source/(kind+'.review.json')).write_text(json.dumps(packet),encoding='utf-8')
            (source/'evidence.json').write_text(json.dumps(dict(pairs=pairs)),encoding='utf-8')
            output=root/'data/evaluation/final'
            with patch.object(final,'ROOT',root),patch.object(final.draft,'ROOT',root):
                final.build(output,source)
                result=final.verify(output)
                self.assertEqual(result['raw_record_hashes_checked'],4)
                with self.assertRaises(ValueError):final.build(output,source)
                (root/'config').mkdir();(root/'data/target').mkdir()
                (root/'config/integration.example.json').write_text('{"target_dir":"data/target"}',encoding='utf-8')
                target=root/'data/target/etl.sqlite3'
                with sqlite3.connect(target) as db:
                    db.executescript('CREATE TABLE target_state(key TEXT,value TEXT);CREATE TABLE product_movie(asin TEXT,movie_id TEXT,relationship TEXT);CREATE TABLE person_aliases(alias_id TEXT,person_id TEXT);CREATE TABLE people(person_id TEXT,identity_status TEXT);')
                    db.executemany('INSERT INTO target_state VALUES(?,?)',[('ready','true'),('input','"fixture-fingerprint"')])
                    db.executemany('INSERT INTO person_aliases VALUES(?,?)',[('fixture-left','fixture-person'),('fixture-right','fixture-person')])
                    db.execute("INSERT INTO people VALUES('fixture-person','verified_fixture')")
                db.close()
                (root/'reports/task9').mkdir()
                (root/'reports/task9/load_statistics.json').write_text(json.dumps(dict(file_size_bytes=target.stat().st_size,input_fingerprint='fixture-fingerprint')),encoding='utf-8')
                agreement=final.compare(output)
                self.assertEqual(agreement['packets']['movies']['model_unmapped_or_non_single_work'],1)
                self.assertEqual(agreement['packets']['movies']['reference_agreement']['tn'],0)
                self.assertIsNone(agreement['packets']['movies']['reference_agreement']['precision'])
                self.assertEqual(agreement['packets']['people']['reference_agreement']['tp'],1)
                changed=json.loads((output/'people.annotations.json').read_text(encoding='utf-8'))
                changed['items'][0]['label']='different_person'
                (output/'people.annotations.json').write_text(json.dumps(changed),encoding='utf-8')
                with self.assertRaises(ValueError):final.verify(output)

    def test_existing_human_edits_are_not_repurposed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'data/evaluation/source';source.mkdir(parents=True)
            evidence=dict(pairs=dict(people=[pair()],movies=[pair()]))
            (source/'evidence.json').write_text(json.dumps(evidence),encoding='utf-8')
            packet=final.draft.review_packet('people',[pair()],decision(),{})
            packet['items'][0]['labeled_by']='Synthetic actual-edit sentinel'
            (source/'movies.review.json').write_text(json.dumps(packet),encoding='utf-8')
            with patch.object(final,'ROOT',root),patch.object(final.draft,'ROOT',root):
                with self.assertRaises(ValueError):final.build(root/'data/evaluation/final',source)


class DeliveryPackageTests(unittest.TestCase):
    def test_output_inside_repository_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            with patch.object(delivery,'ROOT',root):
                with self.assertRaises(ValueError):delivery.package(root/'delivery.zip')

    def test_existing_output_not_overwritten(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);output=root/'delivery.zip';output.write_bytes(b'keep original')
            with self.assertRaises(ValueError):delivery.package(output)
            self.assertEqual(output.read_bytes(),b'keep original')

    def test_archive_all_member_checksums(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)/'workspace';(root/'reports/final').mkdir(parents=True)
            content=root/'README.md';content.write_text('Synthetic delivery fixture',encoding='utf-8')
            manifest=dict(files=[dict(path='README.md',sha256=hashlib.sha256(content.read_bytes()).hexdigest())],content_fingerprint='fixture')
            (root/'reports/final/submission_manifest.json').write_text(json.dumps(manifest),encoding='utf-8')
            (root/'reports/final/submission_verification.json').write_text('{"status":"passed"}',encoding='utf-8')
            output=root.parent/'delivery.zip'
            with patch.object(delivery,'ROOT',root),patch.object(delivery,'acceptance',return_value=dict(status='fixture')),patch.object(delivery.submission,'verify'):
                receipt=delivery.package(output)
            self.assertEqual(receipt['files'],3);self.assertFalse(receipt['licensed_local_data_included'])
            with ZipFile(output) as archive:
                self.assertIsNone(archive.testzip());self.assertEqual(len(archive.namelist()),3)
            self.assertTrue(output.with_suffix('.receipt.json').is_file())


if __name__=='__main__':unittest.main()
