"""Synthetic engineering fixtures; never independent human evaluation labels."""
import gzip
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'))
sys.path.insert(0,str(ROOT/'src/extract'))
import import_imdb
import resolve_people as rp
import integration_common as common

class NamesTests(unittest.TestCase):
    def test_role_field_contract(self):
        self.assertEqual(rp.ROLE_FIELDS,{'directors':'director','credited_stars':'starring','credited_cast':'cast'})
    def test_original_credited_spelling(self):
        self.assertEqual(rp.credited_spelling(dict(normalized='jane doe',raw='Starring Jane Doe, John Smith')),'Jane Doe')
    def test_untruncated_source_name_recovery(self):
        a=dict(normalized='jane doe',raw='truncated excerpt',field_locator='description[0]#line[0]')
        self.assertEqual(rp.credited_spelling(a,json.dumps(dict(description=['Director: Jane Doe'])).encode()),'Jane Doe')
    def test_compound_name_not_fabricated_person(self):
        a=dict(normalized='jane doe john smith',raw='Starring Jane Doe & John Smith',field_locator='description[0]')
        self.assertIsNone(rp.credited_spelling(a,json.dumps(dict(description=[a['raw']])).encode()))
    def test_initials_never_identity(self):
        self.assertFalse(rp.compatible('J Smith','J. Smith'))
        self.assertFalse(rp.compatible('J Smith','John Smith'))
    def test_diacritic_format_alias(self):self.assertTrue(rp.compatible('José Núñez','Jose Nunez'))
    def test_same_name_needs_context(self):
        self.assertTrue(rp.compatible('John Smith','John Smith'))
        self.assertNotEqual(common.stable('person:','imdb:nm1'),common.stable('person:','imdb:nm2'))
    def evidence(self,values=None,year=None):
        p=dict(values=values or {},title_year_hints=[],records=[dict(source_record_id='raw')])
        t=dict(release_year=year,runtime=90,source_record_id='title')
        return p,t
    def test_title_only_never_qualified(self):
        p,t=self.evidence();self.assertFalse(rp.candidate_evidence([p],t,[],[])['qualified'])
    def test_year_only_never_qualified(self):
        p,t=self.evidence({'work_years':[2001]},2001);self.assertFalse(rp.candidate_evidence([p],t,[],[])['qualified'])
    def test_director_year_qualified(self):
        p,t=self.evidence({'work_years':[2001],'directors':['John Smith']},2001)
        self.assertTrue(rp.candidate_evidence([p],t,[dict(nconst='n1',primary_name='John Smith')],[])['qualified'])
    def test_year_conflict_blocks(self):
        p,t=self.evidence({'work_years':[2001],'directors':['John Smith'],'runtimes':[90]},2010)
        self.assertFalse(rp.candidate_evidence([p],t,[dict(nconst='n1',primary_name='John Smith')],[])['qualified'])
    def test_two_actors_no_other_evidence_not_qualified(self):
        p,t=self.evidence({'actor_context':['John Smith','Jane Doe']})
        actors=[dict(nconst='n1',primary_name='John Smith'),dict(nconst='n2',primary_name='Jane Doe')]
        self.assertFalse(rp.candidate_evidence([p],t,[],actors)['qualified'])

class ImportTests(unittest.TestCase):
    def test_raw_offsets_null_and_crc(self):
        with tempfile.TemporaryDirectory() as temp:
            c={'work_dir':temp};d=import_imdb.open_db(c)
            file='title.basics.tsv.gz';path=Path(temp)/file
            content=('\t'.join(import_imdb.HEADERS[file])+'\n'+'tt1\tmovie\tA Test\tA Test\t0\t\\N\t\\N\t90\tDrama\n'+'tt2\ttvSeries\tA Test\tA Test\t0\t2000\t\\N\t\\N\t\\N\n').encode()
            with gzip.open(path,'wb') as f:f.write(content)
            r=dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            s=import_imdb.import_file(d,c,file,r,{'a test'})
            self.assertTrue(s['gzip_crc_verified']);self.assertEqual(s['read'],2);self.assertEqual(s['kept'],1)
            self.assertIsNone(d.execute('select release_year from titles').fetchone()[0])
            row=d.execute('select * from raw_records').fetchone()
            self.assertEqual(content[row['byte_start']:row['byte_end']],row['raw_record'])
            self.assertEqual(import_imdb.import_file(d,c,file,r,{'a test'}),s);d.close()
    def test_resume_committed_transaction(self):
        with tempfile.TemporaryDirectory() as temp:
            c={'work_dir':temp};d=import_imdb.open_db(c);file='title.basics.tsv.gz';path=Path(temp)/file
            with gzip.open(path,'wb') as f:
                f.write(('\t'.join(import_imdb.HEADERS[file])+'\n').encode())
                for i in range(50001):f.write(f'tt{i}\tmovie\tA Test\tA Test\t0\t2000\t\\N\t90\tDrama\n'.encode())
            r=dict(path=str(path),sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            with self.assertRaises(KeyboardInterrupt):import_imdb.import_file(d,c,file,r,{'a test'},1)
            self.assertEqual(d.execute('select count(*) from titles').fetchone()[0],50000)
            s=import_imdb.import_file(d,c,file,r,{'a test'});self.assertEqual(s['kept'],50001)
            self.assertEqual(d.execute('select count(*) from titles').fetchone()[0],50001);d.close()
    def test_changed_schema_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            d=import_imdb.open_db({'work_dir':temp});p=Path(temp)/'x.gz'
            with gzip.open(p,'wb') as f:f.write(b'changed\n')
            with self.assertRaises(ValueError):import_imdb.import_file(d,{},'title.basics.tsv.gz',dict(path=str(p),sha256='x'),set())
            d.close()

class StageTests(unittest.TestCase):
    def test_version_guard_and_no_duplicate_stage(self):
        with tempfile.TemporaryDirectory() as temp:
            d=common.open_db({'work_dir':temp});rid=common.begin(d,'test',{'version':'v1'})
            with d:common.finish(d,'test',rid)
            self.assertIsNone(common.begin(d,'test',{'version':'v1'}))
            with self.assertRaises(ValueError):common.begin(d,'test',{'version':'v2'})
            d.close()

if __name__=='__main__':unittest.main()
