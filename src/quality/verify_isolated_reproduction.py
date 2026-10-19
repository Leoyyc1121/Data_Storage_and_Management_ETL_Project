"""Rebuild fixed evidence in an isolated project copy, sharing only Python deps."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import save_json,hash_file,utc_now

GUARDED_RUN=r'''
import os,re,runpy,sys,sqlite3
from pathlib import Path
from urllib.parse import urlsplit,unquote
isolated=Path.cwd().resolve()
original=Path(os.environ['ETL_ORIGINAL_PROJECT']).resolve()
connections=[]
attachments=[]
def inside(path,root):
    try:path.relative_to(root);return True
    except ValueError:return False
def path_of(value):
    value=os.fspath(value)
    if value.startswith('file:'):
        value=unquote(urlsplit(value).path)
        if len(value)>2 and value[0]=='/' and value[2]==':':value=value[1:]
    return Path(value).resolve()
def audit(event,args):
    if event in ('socket.connect','socket.getaddrinfo'):
        raise RuntimeError('Network access blocked during isolated reproduction')
    if event=='sqlite3.connect':
        if args[0]==':memory:':return
        p=path_of(args[0])
        if not inside(p,isolated):raise RuntimeError('Nonisolated SQLite access blocked: '+str(p))
        connections.append(str(p))
    elif event=='open' and args and isinstance(args[0],(str,os.PathLike)):
        p=path_of(args[0])
        if inside(p,original/'data') and not inside(p,isolated):
            raise RuntimeError('Original project data access blocked: '+str(p))

def authorize(action,first,second,database,trigger):
    if action==sqlite3.SQLITE_ATTACH:
        if not first or first!=':memory:' and not inside(path_of(first),isolated):
            return sqlite3.SQLITE_DENY
        attachments.append(first)
    return sqlite3.SQLITE_OK

original_connect=sqlite3.connect
class GuardedConnection(sqlite3.Connection):
    def execute(self,sql,parameters=(),/):
        # SQLite's authorizer runs before ? values are bound. Expand only this
        # exact parameterized ATTACH form with SQLite string-literal escaping,
        # so its native authorizer can inspect the actual path. Other unknown
        # parameterized ATTACH forms remain denied, not silently allowed.
        if re.fullmatch(r'\s*ATTACH\s+(?:DATABASE\s+)?\?\s+AS\s+[A-Za-z_]\w*\s*;?\s*',sql,re.I):
            if len(parameters)!=1:raise ValueError('Exactly one ATTACH path required')
            value=os.fspath(parameters[0])
            sql=sql.replace('?',"'"+value.replace("'","''")+"'",1)
            parameters=()
        return super().execute(sql,parameters)

def guarded_connect(*args,**kwargs):
    if 'factory' in kwargs and kwargs['factory'] is not GuardedConnection:
        raise ValueError('Custom SQLite connection factory forbidden in isolated replay')
    kwargs['factory']=GuardedConnection
    connection=original_connect(*args,**kwargs)
    connection.set_authorizer(authorize)
    return connection
sqlite3.connect=guarded_connect
sqlite3.dbapi2.connect=guarded_connect
sys.addaudithook(audit)
sys.argv=['offline_acceptance','run','--sample-dir','data/acceptance/fixed_v3','--run-name','isolated_run']
runpy.run_module('src.quality.offline_acceptance',run_name='__main__')
print('ISOLATED_SQLITE_CONNECTIONS',len(connections))
print('ISOLATED_SQLITE_ATTACHMENTS',len(attachments))
'''


def verify(directory=None):
    dest=Path(directory).resolve() if directory else ROOT/'data/acceptance/isolated_project_v3'
    if dest.exists():
        raise ValueError('Preserve existing isolated reproduction; select a new version instead of overwriting')
    dest.mkdir(parents=True)
    # No original staging, integration, target, environment or pipeline state.
    for name in ['src','config','sql','reports']:
        shutil.copytree(ROOT/name,dest/name,ignore=shutil.ignore_patterns('__pycache__'))
    package=dest/'data/acceptance/fixed_v3';package.mkdir(parents=True)
    for name in ['evidence.json','catalog.sqlite3']:
        shutil.copy2(ROOT/'data/acceptance/fixed_v3'/name,package/name)
        if hash_file(package/name)!=hash_file(ROOT/'data/acceptance/fixed_v3'/name):
            raise ValueError('Isolated input copy checksum mismatch')
    log=ROOT/'reports/final/movie5-isolated-reproduction.log'
    with log.open('w',encoding='utf-8') as out:
        result=subprocess.run([sys.executable,'-B','-c',GUARDED_RUN],cwd=dest,stdout=out,stderr=subprocess.STDOUT,
            env=dict(os.environ,ETL_ORIGINAL_PROJECT=str(ROOT),PYTHONDONTWRITEBYTECODE='1',PYTHONUTF8='1',PYTHONIOENCODING='utf-8'))
    if result.returncode!=0:raise ValueError('Isolated reconstruction failed; inspect '+str(log))
    actual=json.loads((dest/'reports/final/offline_acceptance.json').read_text(encoding='utf-8'))
    expected=json.loads((ROOT/'reports/final/offline_acceptance.json').read_text(encoding='utf-8'))
    if actual['counts']!=expected['counts'] or actual['packet_sha256']!=expected['packet_sha256']:
        raise ValueError('Isolated source packet or logical counts differ')
    if actual['target_action']!='complete':raise ValueError('Isolated run reused a pre-existing target')
    report=dict(observed_at=utc_now(),status='passed_isolated_project_and_empty_output_reconstruction',
        copied_inputs=['registered raw evidence packet','retained IMDb catalog with raw TSV evidence'],
        preexisting_integration_or_target_databases_copied=False,original_project_sqlite_access_blocked=True,
        sqlite_attach_paths_authorized=True,network_connect_and_dns_blocked=True,
        original_project_data_file_access_blocked=True,source_packet_sha256=actual['packet_sha256'],counts=actual['counts'],
        counts_equal_original_reconstruction=True,actual_target_action=actual['target_action'],network_requests=0,
        query_count=len(actual['queries']),isolation_directory=str(dest),log=dict(path=str(log),sha256=hash_file(log)),
        limitation='Python file/connect audit plus SQLite ATTACH authorizer, not an OS security sandbox. Existing Python dependency runtime is shared; NOT an independently installed environment or a second-machine full-dataset test. No HUMAN labels generated.')
    save_json(ROOT/'reports/final/isolated_reproduction.json',report)
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--directory',type=Path);args=parser.parse_args()
    print(json.dumps(verify(args.directory),ensure_ascii=False,indent=2))
