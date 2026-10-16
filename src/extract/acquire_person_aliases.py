"""Optional Task5 supplement from the officially offered CC0 Wikidata query API.

Optional source adapter, not part of the default production DAG. Run after
Task5 and BEFORE Task7 in a new version directory if access is permitted.
Bounded to40 existing context-verified IMDb people. No name search identity
guesses. Restriction, failed policy or ambiguity leaves aliases unfilled.
"""
import argparse
from contextlib import closing
import hashlib
import json
from pathlib import Path
import re
import sys
from urllib.parse import urlencode
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'));sys.path.insert(0,str(ROOT/'src/extract'))
from integration_common import *
from acquire_products import HTTPClient,Policy,load_config as network_config,save_body
from resolve_people import compatible,norm_name,full_name
VERSION='external-alias-crosswalk-v1'
def run(c):
    reports=Path(c['report_root'])/'sources/wikidata';reports.mkdir(parents=True,exist_ok=True);raw=ROOT/'data/raw/wikidata/person_aliases_v1';raw.mkdir(parents=True,exist_ok=True)
    with closing(ro(Path(c['work_dir'])/'integration.sqlite3')) as d:
        candidates=[dict(r) for r in d.execute("SELECT p.*,count(*) n FROM people p JOIN credits c USING(person_id) WHERE p.nconst IS NOT NULL GROUP BY p.person_id ORDER BY n DESC,p.nconst LIMIT 40")]
    if not candidates:raise ValueError('No context-verified people')
    for p in candidates:
        if not re.fullmatch('nm[0-9]+',p['nconst']):raise ValueError('Unexpected external ID format')
    sparql='SELECT ?person ?imdb ?label ?alias WHERE { VALUES ?imdb { '+' '.join('"'+p['nconst']+'"' for p in candidates)+' } ?person wdt:P345 ?imdb ; rdfs:label ?label ; skos:altLabel ?alias . FILTER(LANG(?label)="en" && LANG(?alias)="en") }'
    url='https://query.wikidata.org/sparql?'+urlencode(dict(query=sparql,format='json'));manifest=raw/'manifest.json'
    if manifest.exists():
        m=json.loads(manifest.read_text());body=Path(m['path']).read_bytes()
        if hashlib.sha256(body).hexdigest()!=m['sha256'] or m['url']!=url:raise ValueError('Immutable Wikidata query/version changed')
    else:
        settings=network_config(ROOT/'config/task2.example.json');settings.update(base_url='https://query.wikidata.org',raw_dir=str(raw),user_agent='CourseMovieETL/1.0 (https://github.com/Leoyyc1121/Data_Storage_and_Management_ETL_Project; AI-assisted ChatGPT-User)',timeout_seconds=30)
        client=HTTPClient(settings);policy=Policy(settings,client);evidence=policy.preflight();save_json(reports/'access_policy.json',evidence)
        if policy.parser is None or not policy.parser.can_fetch(url,settings['robots_agent']):
            r=dict(status='not_collected_access_policy',observed_at=utc_now(),aliases_added=0);save_json(reports/'acquisition.json',r);return r
        response=client.get(url)
        status,headers,body=response.status,response.headers,response.body
        if status!=200:
            path,sha=save_body(raw,body,'failures','.body');r=dict(status='not_collected_http_failure',http_status=status,snapshot=path,sha256=sha,aliases_added=0);save_json(reports/'acquisition.json',r);return r
        payload=json.loads(body)
        path,sha=save_body(raw,body,'api_results','.json');m=dict(status='success',path=path,sha256=sha,url=url,downloaded_at=utc_now(),headers=dict(headers),source='https://www.wikidata.org/wiki/Help:Data_access',license='CC0',query=sparql)
        save_json(manifest,m);save_json(reports/'acquisition.json',m)
    data=json.loads(body);by_id={p['nconst']:p for p in candidates};groups={}
    for row in data['results']['bindings']:groups.setdefault(row['imdb']['value'],set()).add(row['person']['value'])
    with run_lock(c['work_dir']),closing(open_db(c)) as d:
        sealed=d.execute("SELECT status FROM stage_state WHERE stage='task7'").fetchone()
        existing=d.execute("SELECT status FROM stage_state WHERE stage='task5_aliases'").fetchone()
        if sealed and sealed['status']=='complete' and not existing:raise ValueError('Do not mutate sealed integration with new aliases; use a new version and run this adapter before Task7')
        d.execute('CREATE TABLE IF NOT EXISTS person_external_aliases(alias_id TEXT PRIMARY KEY,person_id TEXT REFERENCES people(person_id),raw_name TEXT,normalized_name TEXT,qid TEXT,source_record_id TEXT REFERENCES source_records(source_record_id),assertion_id TEXT REFERENCES assertions(assertion_id))')
        rid=begin(d,'task5_aliases',dict(task5=required(d,'task5'),source_sha256=m['sha256'],rule=VERSION,implementation=hash_file(__file__)))
        if rid:
            with d:
                for i,row in enumerate(data['results']['bindings']):
                    nconst=row['imdb']['value'];p=by_id.get(nconst);alias=row['alias']['value'];label=row['label']['value'];qid=row['person']['value']
                    if not p or len(groups[nconst])!=1 or not compatible(label,p['canonical_name']) or not full_name(alias) or norm_name(alias)==norm_name(p['canonical_name']):continue
                    sid='wikidata:'+digest_json([m['sha256'],i])
                    # Preserve the ORIGINAL complete response bytes. The
                    # binding index and assertion JSON path locate the member;
                    # do not pretend reserialized object bytes were file slices.
                    d.execute('INSERT OR IGNORE INTO source_records VALUES (?,?,?,?,?,?,?,?,?,?)',(sid,'wikidata_person_alias_query',m['sha256'],i+1,0,len(body),m['sha256'],body,m['url'],m['downloaded_at']))
                    parents=[p['name_source_record_id']]+[r[0] for r in d.execute('SELECT DISTINCT source_record_id FROM credits WHERE person_id=?',(p['person_id'],))]
                    aid=assertion(d,rid,'person',p['person_id'],'external_alias',alias,alias,sid,f'results.bindings[{i}].alias.value',VERSION,'Same unique P345 external ID plus compatible canonical name and already verified work-role context',parents)
                    key='wdalias:'+digest_json([p['person_id'],alias,qid]);d.execute('INSERT OR IGNORE INTO person_external_aliases VALUES (?,?,?,?,?,?,?)',(key,p['person_id'],alias,norm_name(alias),qid,sid,aid))
                finish(d,'task5_aliases',rid)
        result=dict(status='complete_source_claimed_aliases_not_human_gold',aliases_added=d.execute('SELECT count(*) FROM person_external_aliases').fetchone()[0],people_with_substantive_aliases=d.execute('SELECT count(distinct person_id) FROM person_external_aliases').fetchone()[0],source_sha256=m['sha256'],source_use='Wikidata CC0; raw/derived IMDb context remains local-only.',rule=VERSION)
        save_json(Path(c['report_root'])/'task5/external_alias_statistics.json',result);return result
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');a=p.parse_args();print(js(run(load_config(a.config))))
if __name__=='__main__':main()
