import json
from contextlib import closing
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src/quality'))
from verify_target import ensure_publication_report
from integration_common import digest_json,hash_file


class TargetPublicationTests(unittest.TestCase):
    def fixture(self,root,ready=True):
        target=root/'target';target.mkdir()
        inputs={'version':'fixture'};fingerprint=digest_json(inputs)
        (target/'load_registration.json').write_text(json.dumps(dict(inputs=inputs,input_fingerprint=fingerprint)),encoding='utf-8')
        with closing(sqlite3.connect(target/'etl.sqlite3')) as d:
            d.execute('CREATE TABLE target_state(key TEXT PRIMARY KEY,value TEXT)')
            d.executemany('INSERT INTO target_state VALUES (?,?)',[('ready',json.dumps(ready)),('input',json.dumps(fingerprint))])
            d.commit()
        return dict(target_dir=str(target),report_root=str(root/'reports'))

    def test_missing_summary_recovers_from_ready_registered_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=self.fixture(Path(tmp));r=ensure_publication_report(c)
            self.assertTrue(r['report_recovered_after_atomic_publication'])
            self.assertEqual(r['sha256'],hash_file(Path(c['target_dir'])/'etl.sqlite3'))
            self.assertEqual(ensure_publication_report(c),r)

    def test_incomplete_target_cannot_publish_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=self.fixture(Path(tmp),False)
            with self.assertRaises(ValueError):ensure_publication_report(c)

    def test_invalid_registration_cannot_publish_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=self.fixture(Path(tmp));p=Path(c['target_dir'])/'load_registration.json'
            r=json.loads(p.read_text());r['inputs']['version']='changed';p.write_text(json.dumps(r))
            with self.assertRaises(ValueError):ensure_publication_report(c)


if __name__=='__main__':unittest.main()
