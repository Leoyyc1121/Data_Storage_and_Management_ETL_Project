"""Synthetic regression checks for requirement-driven corrections, not human gold."""
from pathlib import Path
import sqlite3
import sys
import unittest
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/transform'));sys.path.insert(0,str(ROOT/'src/quality'))
from resolve_people import name_relation,contextual_people,credit_name_issue
from field_completeness import coverage
from run_queries import statements,PARAMETERS
from prepare_person_evaluation import reserve

class RecheckTests(unittest.TestCase):
    def test_middle_name_contextual_candidate(self):self.assertEqual(name_relation('Jane Doe','Jane Louise Doe'),'contextual_middle_name_or_surname_expansion')
    def test_appended_surname_contextual_candidate(self):self.assertEqual(name_relation('Jane Doe','Jane Doe Smith'),'contextual_middle_name_or_surname_expansion')
    def test_middle_initial_contextual_candidate(self):self.assertIsNotNone(name_relation('Jane L Doe','Jane Louise Doe'))
    def test_conflicting_middle_initial_not_candidate(self):self.assertIsNone(name_relation('Jane Q Doe','Jane Louise Doe'))
    def test_initials_not_identity_candidate(self):self.assertIsNone(name_relation('J Doe','Jane Doe'))
    def test_typo_not_silently_auto_resolved(self):self.assertIsNone(name_relation('Jone Doe','Jane Doe'))
    def test_two_external_ids_stay_ambiguous(self):
        options=[({'primary_name':n},{'person_id':p}) for n,p in [('Jane L Doe','p1'),('Jane M Doe','p2')]]
        self.assertEqual(len(contextual_people('Jane Doe',options)),2)
    def test_repeated_same_external_person_not_ambiguity(self):
        pair=({'primary_name':'Jane Doe'},{'person_id':'p1'})
        self.assertEqual(len(contextual_people('Jane Doe',[pair,pair])),1)
    def test_awards_formats_prose_not_people(self):
        for n in ['Academy Award','Dolby Digital','Traditional Chinese','Jane Doe Produced','A COLLECTION OF MOVIES','JANE DOEJohn SMITH']:
            self.assertIsNotNone(credit_name_issue(n),n)
        self.assertIsNone(credit_name_issue('Jane Louise Doe'))
    def test_identified_coverage_denominator_excludes_provisional(self):
        d=sqlite3.connect(':memory:');d.executescript("CREATE TABLE movies(movie_id,identity_status);CREATE TABLE field_selections(entity_type,entity_id,field,status);INSERT INTO movies VALUES('m1','verified_multifield'),('m2','unresolved');INSERT INTO field_selections VALUES('movie','m1','release_year','selected'),('movie','m2','release_year','missing');")
        r=coverage(d,'movie',['release_year'],"SELECT movie_id FROM movies WHERE identity_status='verified_multifield'")
        self.assertEqual(r['denominator'],1);self.assertEqual(r['fields']['release_year']['selected_fraction'],1);d.close()
    def test_bound_person_query_does_not_truncate_at_thirty(self):
        sql=list(statements(ROOT/'sql/demo_queries.sql'))[8]
        self.assertIn('ELSE -1',sql);self.assertIn('person_decisions',sql);self.assertIn('source_record_id',sql)
    def test_earliest_review_view_is_observation_not_release(self):
        d=sqlite3.connect(':memory:');d.executescript('CREATE TABLE raw_reviews(product_id,logical_review_id,source_review_id,review_time_utc,review_time_raw,source_file_sha256,raw_record_sha256,record_sequence,byte_start,byte_end,clean_rule_version,clean_run_id);CREATE TABLE product_movie(asin,movie_id,relationship);CREATE TABLE lineage_edges(parent_kind,parent_id);')
        d.executescript((ROOT/'sql/target_views.sql').read_text(encoding='utf-8'))
        d.execute("INSERT INTO product_movie VALUES('a','m','single_work')")
        for seq,time in [(1,'2000'),(2,'1000')]:d.execute('INSERT INTO raw_reviews VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',('a','l'+str(seq),'r'+str(seq),time,time,'hash','raw',seq,0,10,'v','run'))
        r=d.execute('SELECT earliest_review_time,source_review_id,inferred_release_bound FROM work_review_observations').fetchone()
        self.assertEqual(r,('1000','r2',None));d.close()
    def test_movie_scores_same_policy_without_scanning_unmapped_products(self):
        d=sqlite3.connect(':memory:');d.executescript('CREATE TABLE raw_reviews(product_id,logical_review_id,record_sequence INTEGER PRIMARY KEY,score);CREATE TABLE logical_reviews(logical_review_id,asin,representative_sequence);CREATE TABLE product_movie(asin,movie_id,relationship);CREATE TABLE movies(movie_id,identity_status);CREATE TABLE lineage_edges(parent_kind,parent_id);')
        d.executescript((ROOT/'sql/target_views.sql').read_text(encoding='utf-8'))
        d.executescript("INSERT INTO movies VALUES('m','verified_multifield');INSERT INTO product_movie VALUES('a','m','single_work'),('b','m','single_work'),('c','m','contains');INSERT INTO raw_reviews VALUES('a','l1',1,5),('a','l1',2,5),('b','l2',3,3),('b','l3',4,NULL),('c','l4',5,1);INSERT INTO logical_reviews VALUES('l1','a',1),('l2','b',3),('l3','b',4),('l4','c',5);")
        r=d.execute('SELECT raw_review_count,logical_review_count,valid_score_denominator,mean_score FROM movie_review_statistics').fetchone()
        self.assertEqual(r,(4,3,2,4.0));d.close()
    def test_evaluation_pool_has_mixed_candidates_no_labels_or_prediction_ids(self):
        aliases=[dict(alias_id=f'a{p}{w}',person_id=f'p{p}',source_record_id=f's{p}{w}',movie_id=f'm{w}',normalized_name=n,raw_name=n,role='cast') for p,n in enumerate(['jane doe','jack doe','john dane','jill dunn','james doe','jess dane']) for w in range(2)]
        pairs=reserve(aliases,limit=8)
        self.assertEqual(len(pairs),8);self.assertGreater(len({p['stratum'] for p in pairs}),1)
        self.assertTrue(all(p['label'] is None and 'person_id' not in p['left'] for p in pairs))
    def test_evaluation_pool_excludes_development_sources_and_people(self):
        aliases=[dict(alias_id=f'a{p}{w}',person_id=f'p{p}',source_record_id=f's{p}{w}',movie_id=f'm{w}',normalized_name='jane doe',raw_name='Jane Doe',role='cast') for p in range(5) for w in range(2)]
        pairs=reserve(aliases,['s20'],['p1'],8)
        for pair in pairs:
            for side in ('left','right'):
                self.assertNotEqual(pair[side]['source_record_id'],'s20');self.assertFalse(pair[side]['alias_id'].startswith('a1'))
if __name__=='__main__':unittest.main()
