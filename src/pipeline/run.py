"""Parameterized local ETL DAG tool: dependencies, logs, stop/retry and stage recovery.

Not a shell script collection: validates acyclic job definitions, schedules only
satisfied dependencies, records each execution, and blocks descendants on error.
Actual data stages own immutable input fingerprints and committed batch cursors.
"""
import argparse
from contextlib import closing
import json
import logging
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import uuid
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
def order_jobs(jobs):
    by={j['id']:j for j in jobs}
    if len(by)!=len(jobs):raise ValueError('Duplicate job ID')
    done=[];remaining=set(by)
    while remaining:
        ready=sorted(j for j in remaining if set(by[j]['depends_on'])<=set(done))
        if not ready:raise ValueError('Cycle or unknown dependency')
        for j in ready:done.append(j);remaining.remove(j)
    return [by[j] for j in done]
def descendants(jobs,start):
    chosen={start}
    if start not in {j['id'] for j in jobs}:raise ValueError('Unknown starting job')
    changed=True
    while changed:
        old=len(chosen)
        chosen.update(j['id'] for j in jobs if set(j['depends_on'])&chosen);changed=len(chosen)>old
    return chosen
def run(path,from_job=None,offline=False,plan=False,stop_after_jobs=None):
    c=json.loads(Path(path).read_text(encoding='utf-8-sig'));jobs=order_jobs(c['jobs']);selected=descendants(jobs,from_job) if from_job else {j['id'] for j in jobs}
    jobs=[j for j in jobs if j['id'] in selected]
    if plan:return dict(status='plan_only',jobs=[dict(id=j['id'],depends_on=j['depends_on'],network=j.get('network',False)) for j in jobs],note='--from-job checks existing upstream data via each stage verifier, it does not pretend skipped upstream jobs executed.')
    root=ROOT/c['state_dir'];root.mkdir(parents=True,exist_ok=True);reports=ROOT/c['report_dir'];reports.mkdir(parents=True,exist_ok=True)
    rid=uuid.uuid4().hex;results=[]
    with run_lock(root),closing(sqlite3.connect(root/'pipeline.sqlite3')) as d:
        d.executescript('CREATE TABLE IF NOT EXISTS executions(run_id TEXT,job_id TEXT,started_at TEXT,finished_at TEXT,status TEXT,exit_code INTEGER,log_path TEXT,definition_hash TEXT,PRIMARY KEY(run_id,job_id));')
        finished=set();failed=set()
        for job in jobs:
            jid=job['id'];log=reports/(rid+'-'+jid+'.log')
            if set(job['depends_on'])&failed:
                failed.add(jid);results.append(dict(job=jid,status='blocked_upstream_failure'));continue
            if offline and job.get('network',False):
                failed.add(jid);results.append(dict(job=jid,status='blocked_network_disabled',reason='Offline mode does not invent source acquisition; use fixed offline acceptance runner or --from-job task5 after restoring registered raw sources.'));continue
            started=utc_now()
            with d:d.execute('INSERT INTO executions VALUES (?,?,?,?,?,?,?,?)',(rid,jid,started,None,'running',None,str(log.resolve()),digest_json(job)))
            logging.info('Execute job %s',jid)
            # This generated runtime log is not a source-code edit; never a shell command.
            with log.open('w',encoding='utf-8') as output:
                # Existing extraction programs use sibling imports. Execute the
                # registered repository script path, not a shell or an import
                # mode that silently depends on a caller's PYTHONPATH.
                script=(ROOT/Path(*job['module'].split('.'))).with_suffix('.py').resolve()
                script.relative_to(ROOT/'src')
                child_env=dict(os.environ,PYTHONUTF8='1',PYTHONIOENCODING='utf-8')
                process=subprocess.run([sys.executable,str(script),*job.get('args',[])],cwd=ROOT,stdout=output,stderr=subprocess.STDOUT,env=child_env)
            status='complete' if process.returncode==0 else 'interrupted' if process.returncode==130 else 'failed'
            with d:d.execute('UPDATE executions SET finished_at=?,status=?,exit_code=? WHERE run_id=? AND job_id=?',(utc_now(),status,process.returncode,rid,jid))
            results.append(dict(job=jid,status=status,exit_code=process.returncode,log_path=str(log.resolve())))
            (finished if status=='complete' else failed).add(jid)
            if stop_after_jobs and len(results)>=stop_after_jobs:break
        result=dict(run_id=rid,status='failed' if failed else 'interrupted_at_job_boundary' if len(results)<len(jobs) else 'complete',definition_version=c['version'],definition_sha256=hash_file(path),results=results,
            recovery='Rerun same command. Each stage rechecks fingerprint and resumes own committed batches/complete phases. --from-job selects affected descendants after source/rule change; use NEW version directories and preserve old source/decisions. No automatic threshold or access-policy overrides.')
        save_json(reports/'last_execution.json',result);return result
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/pipeline.example.json');p.add_argument('--from-job');p.add_argument('--offline',action='store_true');p.add_argument('--plan',action='store_true');p.add_argument('--stop-after-jobs',type=int);a=p.parse_args();logging.basicConfig(level=logging.INFO)
    r=run(a.config,a.from_job,a.offline,a.plan,a.stop_after_jobs);print(js(r))
    if r['status']=='failed':raise SystemExit(1)
if __name__=='__main__':main()
