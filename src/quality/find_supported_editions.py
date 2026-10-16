"""Read-only search for overlooked pending editions using independent context.

Never equates a matching title or shared external ID with a verified identity.
Proposals are not loaded automatically; cut/cluster checks are still required.
"""
from contextlib import closing
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
from resolve_movies import load_config as t4_config
from acquire_imdb import load_config as imdb_config
from resolve_people import candidate_evidence

def search(c):
    tc=t4_config(c['task4_config']);ic=imdb_config(c['imdb_config']);cache={};proposals=[];checked=0
    with closing(ro(Path(tc['work_dir'])/'resolution.sqlite3')) as t4,closing(ro(Path(c['work_dir'])/'integration.sqlite3')) as integrated,closing(ro(Path(ic['work_dir'])/'catalog.sqlite3')) as imdb:
        works={r['tconst']:r['movie_id'] for r in integrated.execute("SELECT * FROM work_matches WHERE status='verified_multifield'")}
        def facts(tid):
            if tid not in cache:
                title=dict(imdb.execute('SELECT * FROM titles WHERE tconst=?',(tid,)).fetchone());crew=imdb.execute('SELECT * FROM crew WHERE tconst=?',(tid,)).fetchone()
                directors=[dict(n) for key in json.loads(crew['directors']) if (n:=imdb.execute('SELECT * FROM names WHERE nconst=?',(key,)).fetchone())] if crew else []
                actors=[dict(n) for n in imdb.execute("SELECT DISTINCT n.* FROM principals p JOIN names n USING(nconst) WHERE p.tconst=? AND p.category IN ('actor','actress')",(tid,))]
                cache[tid]=(title,directors,actors)
            return cache[tid]
        for row in t4.execute("SELECT p.* FROM products p WHERE p.metadata_records>0 AND NOT EXISTS(SELECT 1 FROM product_movie m WHERE m.asin=p.asin) ORDER BY p.asin"):
            profile=json.loads(row['profile'])
            if not profile['eligible']:continue
            ids={r[0] for title in profile['titles'] for r in imdb.execute('SELECT tconst FROM title_lookup WHERE normalized_title=?',(title,))}
            if not ids:continue
            checked+=1;good=[]
            for tid in sorted(ids):
                title,dirs,actors=facts(tid);e=candidate_evidence([profile],title,dirs,actors)
                if e['qualified']:good.append((tid,e))
            if len(good)==1 and good[0][0] in works:
                tid,e=good[0];mid=works[tid]
                members=[json.loads(r[0]) for r in t4.execute('SELECT p.profile FROM products p JOIN product_movie m USING(asin) WHERE m.movie_id=?',(mid,))]
                safe=all(p['variants']==profile['variants'] and (not p['values'].get('runtimes') or not profile['values'].get('runtimes') or min(abs(x-y) for x in p['values']['runtimes'] for y in profile['values']['runtimes'])<=3) for p in members)
                proposals.append(dict(asin=profile['asin'],movie_id=mid,tconst=tid,evidence=e,all_exact_title_candidates=sorted(ids),cut_runtime_cluster_compatible=safe,existing_asins=[p['asin'] for p in members],source_classification=profile['classification']))
    packet=Path(c['work_dir'])/'supported_editions_proposals.json';save_json(packet,dict(status='proposal_only_not_loaded',proposals=proposals))
    result=dict(status='readonly_search_complete',pending_products_with_external_candidates_checked=checked,independently_qualified_pending_editions=len(proposals),cut_runtime_compatible=sum(x['cut_runtime_cluster_compatible'] for x in proposals),distinct_existing_works=len({x['movie_id'] for x in proposals if x['cut_runtime_cluster_compatible']}),proposal_packet=dict(path=str(packet),sha256=hash_file(packet)),semantics='Requires independent complete title/work context and uniqueness; external ID alone insufficient. Existing published mappings unchanged by search.')
    save_json(Path(c['report_root'])/'task4'/'external_edition_search.json',result);return result
if __name__=='__main__':print(js(search(load_config())))
