import sys
from pathlib import Path
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src/transform'))
from enrich_release import release_record
class ReleaseTests(unittest.TestCase):
    def test_year_only(self):
        r=release_record(dict(status='verified_multifield',tconst='tt1'),dict(release_year=2001,source_record_id='r1'))
        self.assertEqual(r['release_year'],2001);self.assertIsNone(r['release_date']);self.assertEqual(r['date_precision'],'year')
    def test_no_identity_no_enrichment(self):
        r=release_record(dict(status='unresolved',tconst=None),dict(release_year=2001,source_record_id='r1'))
        self.assertIsNone(r['release_year']);self.assertIsNone(r['source_record_id'])
    def test_no_review_bound_assumption(self):
        r=release_record(dict(status='verified_multifield',tconst='tt1'),dict(release_year=2001,source_record_id='r1'))
        self.assertIsNone(r['inferred_upper_bound']);self.assertIsNone(r['region'])
    def test_missing_source_year(self):
        r=release_record(dict(status='verified_multifield',tconst='tt1'),dict(release_year=None,source_record_id='r1'))
        self.assertEqual(r['date_precision'],'unknown');self.assertEqual(r['year_reason'],'missing_source_year')
if __name__=='__main__':unittest.main()
