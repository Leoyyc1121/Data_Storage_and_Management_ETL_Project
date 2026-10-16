from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from src.pipeline.run import order_jobs,descendants
class PipelineTests(unittest.TestCase):
    def test_topological_dependencies(self):
        jobs=[dict(id='b',depends_on=['a']),dict(id='a',depends_on=[])]
        self.assertEqual([x['id'] for x in order_jobs(jobs)],['a','b'])
    def test_cycle_rejected(self):
        with self.assertRaises(ValueError):order_jobs([dict(id='a',depends_on=['b']),dict(id='b',depends_on=['a'])])
    def test_missing_dependency_rejected(self):
        with self.assertRaises(ValueError):order_jobs([dict(id='a',depends_on=['missing'])])
    def test_recompute_descendants(self):
        jobs=[dict(id='a',depends_on=[]),dict(id='b',depends_on=['a']),dict(id='c',depends_on=['b']),dict(id='other',depends_on=[])]
        self.assertEqual(descendants(jobs,'b'),{'b','c'})
if __name__=='__main__':unittest.main()
