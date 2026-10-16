"""Separate identified-work coverage from provisional display-field coverage."""
from contextlib import closing
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
def coverage(d,kind,fields,entity_query):
    ids=[r[0] for r in d.execute(entity_query)];denominator=len(ids);result={}
    d.execute('CREATE TEMP TABLE coverage_entities(id TEXT PRIMARY KEY)')
    d.executemany('INSERT INTO coverage_entities VALUES (?)',((x,) for x in ids))
    for field in fields:
        statuses=dict(d.execute('SELECT s.status,count(*) FROM field_selections s JOIN coverage_entities e ON s.entity_id=e.id WHERE s.entity_type=? AND s.field=? GROUP BY s.status',(kind,field)))
        result[field]=dict(status_counts=statuses,denominator=denominator,selected_fraction=statuses.get('selected',0)/denominator if denominator else None)
    d.execute('DROP TABLE coverage_entities')
    return dict(denominator=denominator,fields=result)

def run(c):
    movie_fields=['title','genres','director','starring','cast','release_year','release_date','runtime_minutes']
    with closing(ro(Path(c['target_dir'])/'etl.sqlite3')) as d:
        d.execute('PRAGMA cache_size=-262144')
        levels=dict(product=coverage(d,'product',['title','medium','language','subtitles','region_code','distributor','product_release_date'],'SELECT asin FROM editions'),
            identified_movie=coverage(d,'movie',movie_fields,"SELECT movie_id FROM movies WHERE identity_status='verified_multifield'"),
            all_work_records=coverage(d,'movie',movie_fields,'SELECT movie_id FROM movies'))
        total=levels['product']['denominator']
        identified_products=d.execute("SELECT count(DISTINCT m.asin) FROM product_movie m JOIN movies w USING(movie_id) WHERE w.identity_status='verified_multifield'").fetchone()[0]
        classified=d.execute("SELECT count(*) FROM editions WHERE classification='movie'").fetchone()[0]
        result=dict(status='complete',semantics='Movie-information denominator is independently multifield-identified works. Provisional/all work-record coverage is separate, never called identified movie coverage. Some cast credits are NOT a complete cast.',levels=levels,
            identification=dict(all_products=total,classified_movie_products=classified,multifield_identified_products=identified_products,identified_product_fraction=identified_products/total if total else None),
            release_semantics='Selected release_year means a sourced year for a matched work, not a verified full date or independently human-verified identity.')
    save_json(Path(c['report_root'])/'final/field_completeness.json',result);return result
if __name__=='__main__':print(js(run(load_config())))
