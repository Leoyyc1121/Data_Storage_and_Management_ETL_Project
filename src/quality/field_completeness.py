"""Report product and provisional-work field coverage with separate denominators."""
from contextlib import closing
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
def run(c):
    result=dict(status='complete',semantics='Coverage of loaded source-based product fields and current work records, NOT coverage of the unknown true movie population. Identifier/status fields excluded. Selected cast means some supported credits, not a complete cast list.',levels={})
    with closing(ro(Path(c['target_dir'])/'etl.sqlite3')) as d:
        d.execute('PRAGMA cache_size=-262144')
        for kind,table,fields in [('product','editions',['title','medium','language','subtitles','region_code','distributor','product_release_date']),('movie','movies',['title','genres','director','starring','cast','release_year','release_date','runtime_minutes'])]:
            denominator=d.execute(f'SELECT count(*) FROM {table}').fetchone()[0];coverage={}
            for field in fields:
                statuses=dict(d.execute('SELECT status,count(*) FROM field_selections WHERE entity_type=? AND field=? GROUP BY status',(kind,field)))
                coverage[field]=dict(status_counts=statuses,denominator=denominator,selected_fraction=statuses.get('selected',0)/denominator if denominator else None)
            result['levels'][kind]=dict(denominator=denominator,scope='All original ASIN' if kind=='product' else 'Provisional plus multifield work records, including169 external matches; not confirmed-film census',fields=coverage)
    save_json(Path(c['report_root'])/'final/field_completeness.json',result);return result
if __name__=='__main__':print(js(run(load_config())))
