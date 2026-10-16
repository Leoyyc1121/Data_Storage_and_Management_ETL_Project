"""Shared versioned integration storage and explicit field-level provenance."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import sqlite3
import sys
import uuid

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/extract'))
sys.path.insert(0,str(ROOT/'src/transform'))
from acquire_products import digest_json,save_json,run_lock
from register_source import hash_file,utc_now

NS=uuid.UUID('782ed897-77ac-5659-a824-49b281b1a2c9')
def stable(prefix,value):return prefix+uuid.uuid5(NS,value).hex
def js(value):return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'))
def load_config(path=ROOT/'config/integration.example.json'):
    c=json.loads(Path(path).read_text(encoding='utf-8-sig'))
    for key in ('task4_config','imdb_config','work_dir','report_root','source_reviews','target_dir'):
        c[key]=str((ROOT/c[key]).resolve())
    if not isinstance(c['batch_size'],int) or isinstance(c['batch_size'],bool) or c['batch_size']<=0:raise ValueError('Invalid batch_size')
    if Path(c['work_dir']) in (ROOT,Path(c['source_reviews']),Path(c['target_dir'])):raise ValueError('Separate integration directory required')
    return c
def ro(path):
    d=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True);d.row_factory=sqlite3.Row;return d
def open_db(c):
    Path(c['work_dir']).mkdir(parents=True,exist_ok=True)
    d=sqlite3.connect(Path(c['work_dir'])/'integration.sqlite3');d.row_factory=sqlite3.Row
    d.execute('PRAGMA foreign_keys=ON');d.execute('PRAGMA journal_mode=WAL')
    d.executescript('''
    CREATE TABLE IF NOT EXISTS etl_runs(run_id TEXT PRIMARY KEY,stage TEXT NOT NULL,started_at TEXT NOT NULL,finished_at TEXT,status TEXT NOT NULL,input_fingerprint TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS stage_state(stage TEXT PRIMARY KEY,input_fingerprint TEXT NOT NULL,status TEXT NOT NULL,run_id TEXT REFERENCES etl_runs(run_id));
    CREATE TABLE IF NOT EXISTS source_records(source_record_id TEXT PRIMARY KEY,source_id TEXT NOT NULL,archive_sha256 TEXT NOT NULL,record_sequence INTEGER NOT NULL,byte_start INTEGER NOT NULL,byte_end INTEGER NOT NULL,raw_sha256 TEXT NOT NULL,raw_record BLOB NOT NULL,url TEXT,acquired_at TEXT,UNIQUE(source_id,archive_sha256,record_sequence));
    CREATE TABLE IF NOT EXISTS operations(operation_id TEXT PRIMARY KEY,operation_type TEXT NOT NULL,rule_id TEXT NOT NULL,run_id TEXT NOT NULL REFERENCES etl_runs(run_id),parents TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS assertions(assertion_id TEXT PRIMARY KEY,entity_type TEXT NOT NULL,entity_id TEXT NOT NULL,field TEXT NOT NULL,raw_value TEXT,normalized_value TEXT,source_record_id TEXT REFERENCES source_records(source_record_id),field_locator TEXT NOT NULL,status TEXT NOT NULL,reason TEXT NOT NULL,operation_id TEXT NOT NULL REFERENCES operations(operation_id));
    CREATE INDEX IF NOT EXISTS assertion_entity ON assertions(entity_type,entity_id,field);
    CREATE INDEX IF NOT EXISTS assertion_source ON assertions(source_record_id);
    ''');return d
def table_hash(d,table,order=None):
    columns=[r[1] for r in d.execute(f'PRAGMA table_info("{table}")')]
    if not columns:raise ValueError('Unknown table')
    ordering=order or ','.join('"'+x+'"' for x in columns)
    h=hashlib.sha256();n=0
    for row in d.execute(f'SELECT * FROM "{table}" ORDER BY {ordering}'):
        h.update(js([{'blob_sha256':hashlib.sha256(v).hexdigest()} if isinstance(v,bytes) else v for v in row]).encode());h.update(b'\n');n+=1
    return dict(rows=n,ordered_rows_sha256=h.hexdigest())
def begin(d,stage,inputs):
    fingerprint=digest_json(inputs);old=d.execute('SELECT * FROM stage_state WHERE stage=?',(stage,)).fetchone()
    if old and old['input_fingerprint']!=fingerprint:raise ValueError(f'{stage}: inputs/rules changed; new version work_dir required')
    if old and old['status']=='complete':return None
    run=uuid.uuid4().hex
    with d:
        d.execute('INSERT INTO etl_runs VALUES (?,?,?,?,?,?)',(run,stage,utc_now(),None,'running',fingerprint))
        d.execute('INSERT OR REPLACE INTO stage_state VALUES (?,?,?,?)',(stage,fingerprint,'running',run))
    return run
def finish(d,stage,run):
    d.execute("UPDATE etl_runs SET status='complete',finished_at=? WHERE run_id=?",(utc_now(),run))
    d.execute("UPDATE stage_state SET status='complete' WHERE stage=?",(stage,))
def required(d,stage):
    row=d.execute('SELECT * FROM stage_state WHERE stage=?',(stage,)).fetchone()
    if not row or row['status']!='complete':raise ValueError('Complete prerequisite required: '+stage)
    return row['input_fingerprint']
def operation(d,run,kind,rule,parents=()):
    oid='op:'+digest_json([run,kind,rule,sorted(parents)])
    d.execute('INSERT OR IGNORE INTO operations VALUES (?,?,?,?,?)',(oid,kind,rule,run,js(sorted(parents))))
    return oid
def assertion(d,run,entity_type,entity_id,field,raw,normalized,sid,locator,rule,reason='retained_candidate',parents=()):
    aid='a:'+digest_json([entity_type,entity_id,field,sid,locator,raw,normalized,rule])
    op=operation(d,run,'extract_normalize',rule,parents)
    d.execute('INSERT OR IGNORE INTO assertions VALUES (?,?,?,?,?,?,?,?,?,?,?)',(aid,entity_type,entity_id,field,js(raw),js(normalized),sid,locator,'candidate',reason,op))
    return aid
def report(c,stage,d,tables,extra):
    result=dict(observed_at=utc_now(),status='complete_engineering_pending_human_acceptance' if stage=='task5' else 'complete',
        tables={t:table_hash(d,t) for t in tables},**extra)
    save_json(Path(c['report_root'])/stage/'full_statistics.json',result);return result
