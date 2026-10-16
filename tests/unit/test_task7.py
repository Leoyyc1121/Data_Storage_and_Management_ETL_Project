from contextlib import closing
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src/transform'))
import select_fields as sf
from integration_common import open_db,begin
class SelectionTests(unittest.TestCase):
    def test_controlled_genre_aliases(self):self.assertEqual(sf.normalize_genres(['Drama','Sci-Fi','Film-Noir']),['drama','film_noir','science_fiction'])
    def test_conflict_null(self):
        v,s,_,ids=sf.choose([dict(assertion_id='a',value='A',rank=1),dict(assertion_id='b',value='B',rank=1)])
        self.assertIsNone(v);self.assertEqual(s,'unresolved');self.assertFalse(ids)
    def test_authority_semantics(self):
        v,_,_,ids=sf.choose([dict(assertion_id='a',value=1999,rank=1),dict(assertion_id='b',value=2001,rank=2)])
        self.assertEqual(v,2001);self.assertEqual(ids,{'b'})
    def test_ineligible_not_selected(self):
        v,s,_,_=sf.choose([dict(assertion_id='a',value='English guessed',rank=100,eligible=False)])
        self.assertIsNone(v);self.assertEqual(s,'missing')
    def test_union_membership(self):
        v,_,_,ids=sf.choose([dict(assertion_id='a',value=['en','fr'],rank=1),dict(assertion_id='b',value=['en'],rank=2)],'union')
        self.assertEqual(v,['en','fr']);self.assertEqual(ids,{'a','b'})
    def test_empty_product_build_constraints(self):
        with tempfile.TemporaryDirectory() as temp,closing(open_db({'work_dir':temp})) as d,closing(sqlite3.connect(':memory:')) as t4,closing(sqlite3.connect(':memory:')) as empty:
            d.row_factory=t4.row_factory=sqlite3.Row;sf.schema(d);rid=begin(d,'test',{})
            t4.executescript('CREATE TABLE products(asin TEXT,classification TEXT,review_count INTEGER,profile TEXT);CREATE TABLE product_status(asin TEXT,mapping_status TEXT);CREATE TABLE works(movie_id TEXT);')
            t4.execute('INSERT INTO products VALUES (?,?,?,?)',('A','awaiting_confirmation',1,json.dumps(dict(records=[]))))
            t4.execute('INSERT INTO product_status VALUES (?,?)',('A','pending_classification'))
            with d:sf.build(d,{},t4,empty,empty,{},rid)
            self.assertEqual(d.execute('select count(*) from editions').fetchone()[0],1)
            self.assertEqual(d.execute('select count(*) from field_selections').fetchone()[0],7);sf.verify(d)
if __name__=='__main__':unittest.main()
