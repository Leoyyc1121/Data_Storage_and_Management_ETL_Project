import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src/transform'))
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src/extract'))
import clean_reviews as cr
from parse_reviews import parse_record
def row(seq=1,asin='A',text='Review'):
    raw=(f'product/productId: {asin}\nreview/userId: U\nreview/profileName: User\nreview/helpfulness: 0/0\nreview/score: 5.0\nreview/time: 1262304000\nreview/summary: Summary\nreview/text: {text}\n').encode()
    return parse_record(raw,seq,'a'*64,(0,len(raw),1,9),'run')
class ReviewTests(unittest.TestCase):
    def test_zero_zero_ratio_null(self):self.assertIsNone(row()['helpfulness_ratio'])
    def test_cleanup_preserves_case(self):self.assertEqual(cr.clean_text(' A&nbsp;  B\nC '),'A B C')
    def test_unknown_text_not_guessed(self):self.assertIsNone(cr.clean_text(None))
    def test_no_cross_product_dedup(self):self.assertNotEqual(cr.logical_id(row(1,'A')),cr.logical_id(row(2,'B')))
    def test_exact_same_batch_duplicate(self):
        with tempfile.TemporaryDirectory() as temp:
            d=cr.open_reviews({'work_dir':temp})
            with d:cr.insert_batch(d,[row(1),row(2)],'r')
            self.assertEqual(d.execute('select count(*) from raw_reviews').fetchone()[0],2)
            self.assertEqual(d.execute('select count(*) from logical_reviews').fetchone()[0],1);d.close()
    def test_exact_prior_batch_duplicate(self):
        with tempfile.TemporaryDirectory() as temp:
            d=cr.open_reviews({'work_dir':temp})
            with d:cr.insert_batch(d,[row(1)],'r1')
            with d:cr.insert_batch(d,[row(2)],'r2')
            self.assertEqual(d.execute('select count(*) from logical_reviews').fetchone()[0],1);d.close()
    def test_similar_not_exact_kept(self):
        self.assertNotEqual(cr.logical_id(row(1,text='Text')),cr.logical_id(row(2,text='text')))
    def test_collision_fails_not_merges(self):
        with tempfile.TemporaryDirectory() as temp:
            d=cr.open_reviews({'work_dir':temp});a=row(1);b=row(2,text='Different');b['raw_record_sha256']=a['raw_record_sha256']
            with self.assertRaises(ValueError):
                with d:cr.insert_batch(d,[a,b],'r')
            self.assertEqual(d.execute('select count(*) from raw_reviews').fetchone()[0],0);d.close()
if __name__=='__main__':unittest.main()
