from pathlib import Path
import sqlite3
import sys
import unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/quality'));sys.path.insert(0,str(ROOT/'src/load'))
from run_queries import statements,PARAMETERS,verification_filename
from build_target import state,set_state,edge
class TargetTests(unittest.TestCase):
    def test_selected_queries_preserve_full_acceptance_report(self):self.assertEqual(verification_filename(PARAMETERS,[6]),'query_selection_verification.json')
    def test_default_full_query_report(self):self.assertEqual(verification_filename(PARAMETERS),'query_verification.json')
    def test_query_scripts_executable_statement_boundaries(self):self.assertEqual(len(list(statements(ROOT/'sql/demo_queries.sql'))),10)
    def test_target_phase_state(self):
        d=sqlite3.connect(':memory:');self.assertIsNone(state(d,'ready'))
        d.execute('CREATE TABLE target_state(key TEXT PRIMARY KEY,value TEXT)');set_state(d,'ready',True);self.assertIs(state(d,'ready'),True);d.close()
    def test_graph_preserves_multiple_sources_and_idempotence(self):
        d=sqlite3.connect(':memory:');d.execute('CREATE TABLE lineage_edges(child_kind,child_id,parent_kind,parent_id,role,PRIMARY KEY(child_kind,child_id,parent_kind,parent_id,role))')
        edge(d,'assertion','a','source','s1','raw');edge(d,'assertion','a','source','s2','raw');edge(d,'assertion','a','source','s1','raw')
        self.assertEqual(d.execute('SELECT count(*) FROM lineage_edges').fetchone()[0],2);d.close()
if __name__=='__main__':unittest.main()
