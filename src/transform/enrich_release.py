"""Task6: release-year enrichment only after unique multi-field work matching.

IMDb startYear supplies release YEAR, not a calendar date, release territory or
world-premiere guarantee. Reviews never silently replace release information.
"""
import argparse
from contextlib import closing
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
from integration_common import *
from acquire_imdb import load_config as imdb_config
from resolve_people import register_imdb

VERSION='verified-release-year-v1'
def release_record(match,title):
    verified=match['status']=='verified_multifield' and match['tconst'] is not None
    year=title['release_year'] if verified and title else None
    return dict(release_year=year,release_date=None,date_precision='year' if year is not None else 'unknown',region=None,
        release_type='source_release_year_unspecified_territory' if year is not None else None,
        year_reason='matched_work_imdb_startYear_release_year' if year is not None else 'missing_source_year' if verified else 'work_identity_not_verified',
        date_reason='source_year_only_no_calendar_date' if year is not None else 'no_verified_full_release_date',
        inferred_upper_bound=None,inference_reason='review_can_be_preorder_or_product_specific_no_bound_asserted',
        source_record_id=title['source_record_id'] if verified and title else None)
def run(c):
    ic=imdb_config(c['imdb_config'])
    with run_lock(c['work_dir']),closing(open_db(c)) as d,closing(ro(Path(ic['work_dir'])/'catalog.sqlite3')) as imdb:
        d.executescript('''CREATE TABLE IF NOT EXISTS release_info(movie_id TEXT PRIMARY KEY REFERENCES work_matches(movie_id),release_year INTEGER,release_date TEXT,date_precision TEXT NOT NULL CHECK(date_precision IN ('year','day','unknown')),region TEXT,release_type TEXT,year_reason TEXT NOT NULL,date_reason TEXT NOT NULL,inferred_upper_bound TEXT,inference_reason TEXT NOT NULL,source_record_id TEXT REFERENCES source_records(source_record_id),assertion_id TEXT REFERENCES assertions(assertion_id));''')
        rid=begin(d,'task6',dict(task5=required(d,'task5'),version=VERSION,implementation=hash_file(__file__)))
        if rid:
            with d:
                for m in d.execute('SELECT * FROM work_matches ORDER BY movie_id').fetchall():
                    t=imdb.execute('SELECT * FROM titles WHERE tconst=?',(m['tconst'],)).fetchone() if m['tconst'] else None
                    rec=release_record(m,t);aid=None
                    if rec['source_record_id']:
                        register_imdb(d,imdb,rec['source_record_id'])
                        aid=assertion(d,rid,'movie',m['movie_id'],'release_year',t['release_year'],rec['release_year'],rec['source_record_id'],'startYear',VERSION,rec['year_reason'])
                    d.execute('INSERT INTO release_info VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',(m['movie_id'],rec['release_year'],rec['release_date'],rec['date_precision'],rec['region'],rec['release_type'],rec['year_reason'],rec['date_reason'],rec['inferred_upper_bound'],rec['inference_reason'],rec['source_record_id'],aid))
                finish(d,'task6',rid)
        bad=d.execute("SELECT count(*) FROM release_info r JOIN work_matches w USING(movie_id) WHERE (r.release_year IS NOT NULL AND w.status!='verified_multifield') OR r.release_date IS NOT NULL OR r.inferred_upper_bound IS NOT NULL OR (r.release_year IS NOT NULL AND r.date_precision!='year')").fetchone()[0]
        if bad:raise ValueError('Release semantics/identity violation')
        result=report(c,'task6',d,['release_info'],dict(years_available=d.execute('SELECT count(*) FROM release_info WHERE release_year IS NOT NULL').fetchone()[0],
            full_dates_available=0,inferred_bounds_asserted=0,missing_reasons=dict(d.execute('SELECT year_reason,count(*) FROM release_info GROUP BY year_reason').fetchall()),
            semantics='Source release year only, territory unspecified; not DVD date or guaranteed earliest world premiere. No invented January1 or review-based release date.'))
        save_json(Path(c['report_root'])/'task6'/'full_verification.json',dict(status='passed',invalid_year_identity_or_precision_rows=bad,calendar_dates_not_fabricated=True));return result
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');a=p.parse_args();print(js(run(load_config(a.config))))
if __name__=='__main__':main()
