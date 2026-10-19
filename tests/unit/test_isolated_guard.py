import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'src/quality'))
from verify_isolated_reproduction import GUARDED_RUN


class IsolationGuardTests(unittest.TestCase):
    def run_guard(self,tail):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);isolated=root/'isolated';isolated.mkdir()
            bootstrap=GUARDED_RUN.split('sys.addaudithook(audit)',1)[0]
            return subprocess.run([sys.executable,'-B','-c',bootstrap+'sys.addaudithook(audit)\n'+tail],
                cwd=isolated,env=dict(os.environ,ETL_ORIGINAL_PROJECT=str(root)),capture_output=True,text=True)

    def test_sqlite_attach_outside_isolated_root_is_denied(self):
        r=self.run_guard("d=sqlite3.connect(':memory:'); d.execute('ATTACH DATABASE ? AS other',(str(original/'outside.sqlite3'),))")
        self.assertNotEqual(r.returncode,0)
        self.assertIn('not authorized',r.stderr)

    def test_sqlite_attach_inside_isolated_root_is_allowed(self):
        r=self.run_guard("d=sqlite3.connect(':memory:'); d.execute('ATTACH DATABASE ? AS other',(str(isolated/'inside.sqlite3'),)); d.close()")
        self.assertEqual(r.returncode,0,r.stderr)

    def test_network_connect_is_blocked_before_request(self):
        r=self.run_guard("import socket; s=socket.socket(); s.connect(('127.0.0.1',9))")
        self.assertNotEqual(r.returncode,0)
        self.assertIn('Network access blocked',r.stderr)


if __name__=='__main__':unittest.main()
