"""Locate fields affected by a raw source or rule before versioned recomputation."""
import argparse
from contextlib import closing
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
QUERY='''WITH RECURSIVE affected(kind,id) AS (
 SELECT ?1,?2 UNION
 SELECT e.child_kind,e.child_id FROM lineage_edges e JOIN affected a ON e.parent_kind=a.kind AND e.parent_id=a.id
) SELECT DISTINCT s.entity_type,s.entity_id,s.field,s.status,s.rule_id FROM affected a JOIN field_selections s ON a.kind='selection' AND a.id=s.selection_id'''
def run(c,source_id=None,rule_id=None,show_fields=False):
    with closing(ro(Path(c['target_dir'])/'etl.sqlite3')) as d:
        d.execute('PRAGMA cache_size=-262144')
        if rule_id:
            roots=[('operation',r[0]) for r in d.execute('SELECT operation_id FROM operations WHERE rule_id=?',(rule_id,))]
        else:
            source_id=source_id or d.execute("SELECT parent_id FROM lineage_edges WHERE parent_kind='source' ORDER BY parent_id LIMIT1".replace('LIMIT1','LIMIT 1')).fetchone()[0]
            roots=[('source',source_id)]
        fields={}
        for kind,key in roots:
            for r in d.execute(QUERY,(kind,key)):fields[(r['entity_type'],r['entity_id'],r['field'])]=dict(r)
        stages=['task5','task6','task7','task9'] if source_id and source_id.startswith('imdb:') else ['task3','task4','task5','task6','task7','task9']
        result=dict(status='impact_located_read_only',source_record_id=source_id,rule_id=rule_id,affected_field_count=len(fields),
            affected_entities=len({(x[0],x[1]) for x in fields}),suggested_recompute_stages=stages,
            execution='Preserve old snapshots; change version directories; rerun affected stage and descendants via src.pipeline.run --from-job. Do not mutate registered inputs/decisions in place. Source-file version/ID changes can require broader source-stage recomputation.',
            evidence_of_actual_version_recompute='Task5 name-span refinement v2 to v3 recomputed in final_v6; old final_v5 identity/selection database and reports remain preserved.',
            scope='Dependency impact, not a claim that every located field necessarily changes value or that any source was modified.')
        if show_fields:result['fields']=list(fields.values())
        save_json(Path(c['report_root'])/'final/impact_recompute.json',{k:v for k,v in result.items() if k!='fields'});return result
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');p.add_argument('--source-record-id');p.add_argument('--rule-id');p.add_argument('--show-fields',action='store_true');a=p.parse_args();print(js(run(load_config(a.config),a.source_record_id,a.rule_id,a.show_fields)))
if __name__=='__main__':main()
