"""Read-only search for overlooked pending editions using independent context.

Never equates a matching title or shared external ID with a verified identity.
Proposals are not loaded automatically; cut/cluster checks are still required.
"""
from contextlib import closing
from collections import defaultdict
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
from resolve_movies import load_config as t4_config
from acquire_imdb import load_config as imdb_config
from resolve_people import candidate_evidence

def search(c):
    tc=t4_config(c['task4_config']);ic=imdb_config(c['imdb_config']);cache={};proposals=[];checked=all_checked=qualified=0;groups=defaultdict(list)
    with closing(ro(Path(tc['work_dir'])/'resolution.sqlite3')) as t4,closing(ro(Path(c['work_dir'])/'integration.sqlite3')) as integrated,closing(ro(Path(ic['work_dir'])/'catalog.sqlite3')) as imdb:
        works={r['tconst']:r['movie_id'] for r in integrated.execute("SELECT * FROM work_matches WHERE status='verified_multifield'")}
        def facts(tid):
            if tid not in cache:
                title=dict(imdb.execute('SELECT * FROM titles WHERE tconst=?',(tid,)).fetchone());crew=imdb.execute('SELECT * FROM crew WHERE tconst=?',(tid,)).fetchone()
                directors=[dict(n) for key in json.loads(crew['directors']) if (n:=imdb.execute('SELECT * FROM names WHERE nconst=?',(key,)).fetchone())] if crew else []
                actors=[dict(n) for n in imdb.execute("SELECT DISTINCT n.* FROM principals p JOIN names n USING(nconst) WHERE p.tconst=? AND p.category IN ('actor','actress')",(tid,))]
                cache[tid]=(title,directors,actors)
            return cache[tid]
        for row in t4.execute("SELECT p.*,EXISTS(SELECT 1 FROM product_movie m WHERE m.asin=p.asin) AS already_mapped FROM products p WHERE p.metadata_records>0 ORDER BY p.asin"):
            profile=json.loads(row['profile'])
            if not profile['eligible']:continue
            ids={r[0] for title in profile['titles'] for r in imdb.execute('SELECT tconst FROM title_lookup WHERE normalized_title=?',(title,))}
            if not ids:continue
            all_checked+=1;checked+=not row['already_mapped'];good=[]
            for tid in sorted(ids):
                title,dirs,actors=facts(tid);e=candidate_evidence([profile],title,dirs,actors)
                if e['qualified']:good.append((tid,e))
            if len(good)==1:
                qualified+=1;groups[good[0][0]].append(profile['asin'])
            if len(good)==1 and good[0][0] in works and not row['already_mapped']:
                tid,e=good[0];mid=works[tid]
                members=[json.loads(r[0]) for r in t4.execute('SELECT p.profile FROM products p JOIN product_movie m USING(asin) WHERE m.movie_id=?',(mid,))]
                safe=all(p['variants']==profile['variants'] and (not p['values'].get('runtimes') or not profile['values'].get('runtimes') or min(abs(x-y) for x in p['values']['runtimes'] for y in profile['values']['runtimes'])<=3) for p in members)
                proposals.append(dict(asin=profile['asin'],movie_id=mid,tconst=tid,evidence=e,all_exact_title_candidates=sorted(ids),cut_runtime_cluster_compatible=safe,existing_asins=[p['asin'] for p in members],source_classification=profile['classification']))
    packet=Path(c['work_dir'])/'supported_editions_proposals.json';save_json(packet,dict(status='proposal_only_not_loaded',proposals=proposals))
    group_path=Path(c['work_dir'])/'rechecked_external_edition_groups.json'
    save_json(group_path,dict(status='candidate_search_NOT_new_merge_decisions',groups={key:value for key,value in sorted(groups.items()) if len(value)>1}))
    result=dict(status='readonly_search_complete',pending_products_with_external_candidates_checked=checked,all_eligible_profiles_with_external_candidates_checked=all_checked,independently_qualified_profiles=qualified,shared_external_context_candidate_groups=sum(len(v)>1 for v in groups.values()),candidate_groups_packet=dict(path=str(group_path),sha256=hash_file(group_path)),independently_qualified_pending_editions=len(proposals),cut_runtime_compatible=sum(x['cut_runtime_cluster_compatible'] for x in proposals),distinct_existing_works=len({x['movie_id'] for x in proposals if x['cut_runtime_cluster_compatible']}),proposal_packet=dict(path=str(packet),sha256=hash_file(packet)),semantics='Requires independent complete title/work context and uniqueness; external ID alone insufficient. Search now includes pairs with no already-mapped anchor; candidates are not cluster-validated merges. Existing published mappings unchanged by search.')
    save_json(Path(c['report_root'])/'task4'/'external_edition_search.json',result);return result
if __name__=='__main__':print(js(search(load_config())))
