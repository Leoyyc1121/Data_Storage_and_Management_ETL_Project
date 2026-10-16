"""Final target integrity, full-count reconciliation and graph referential audit."""
import argparse
from contextlib import closing
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *
def verify(c):
    with closing(ro(Path(c['target_dir'])/'etl.sqlite3')) as d:
        d.execute('PRAGMA cache_size=-786432');d.execute('PRAGMA mmap_size=1073741824')
        if d.execute('PRAGMA foreign_key_check').fetchone():raise ValueError('Target FK violation')
        counts={t:d.execute(f'SELECT count(*) FROM {t}').fetchone()[0] for t in ['raw_reviews','logical_reviews','reviewers','editions','movies','people','credits','movie_person','movie_genres','source_records','assertions','field_selections','selection_members','lineage_edges','cross_edition_candidates']}
        if counts['raw_reviews']!=7911684 or counts['editions']!=253059 or counts['reviewers']!=889176:raise ValueError('Full dataset count mismatch')
        bad=d.execute('SELECT count(*) FROM editions e LEFT JOIN (SELECT product_id,count(*) n FROM raw_reviews GROUP BY product_id) r ON e.asin=r.product_id WHERE e.review_count!=r.n OR r.n IS NULL').fetchone()[0]
        if bad:raise ValueError('Product review count mismatch')
        unknown=d.execute("SELECT count(*) FROM lineage_edges e WHERE (e.parent_kind='source' AND NOT EXISTS (SELECT 1 FROM source_records s WHERE s.source_record_id=e.parent_id)) OR (e.parent_kind='operation' AND NOT EXISTS (SELECT 1 FROM operations o WHERE o.operation_id=e.parent_id)) OR (e.parent_kind='assertion' AND NOT EXISTS (SELECT 1 FROM assertions a WHERE a.assertion_id=e.parent_id))").fetchone()[0]
        if unknown:raise ValueError('Unknown lineage parents')
        no_source=d.execute("SELECT count(*) FROM field_selections s WHERE s.status='selected' AND NOT EXISTS(SELECT 1 FROM selection_members m JOIN assertions a USING(assertion_id) LEFT JOIN review_assertion_sources r USING(assertion_id) WHERE m.selection_id=s.selection_id AND m.status='selected' AND (a.source_record_id IS NOT NULL OR r.source_review_id IS NOT NULL))").fetchone()[0]
        if no_source:raise ValueError('Selected field with no raw evidence')
        b=d.execute("SELECT count(*) FROM raw_reviews WHERE helpfulness_ratio IS NOT NULL AND (helpful_votes<0 OR total_votes<=0 OR helpful_votes>total_votes OR ABS(helpfulness_ratio-1.0*helpful_votes/total_votes)>1e-12)").fetchone()[0]
        if b:raise ValueError('Invalid helpfulness ratio')
        result=dict(status='passed_integrity_not_human_accuracy',observed_at=utc_now(),counts=counts,foreign_key_errors=0,product_review_count_mismatches=bad,unknown_lineage_parent_nodes=unknown,selected_fields_without_source=no_source,invalid_helpfulness_ratios=b,
            date_semantics=dict(full_dates=d.execute('SELECT count(*) FROM movies WHERE release_date IS NOT NULL').fetchone()[0],years=d.execute('SELECT count(*) FROM movies WHERE release_year IS NOT NULL').fetchone()[0]),
            lineage_implementation='Materialized source/assertion/selection processing graph for metadata and people, queryable raw/clean field provenance view for all reviews; source review field prefixes and raw byte locations available.',
            limits='Engineering integrity does not establish human matching accuracy or completion of required independent evaluation/cases.')
        save_json(Path(c['report_root'])/'final/full_verification.json',result)
        dictionary={}
        for row in d.execute("SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"):
            dictionary[row['name']]=dict(create_sql=row['sql'],columns=[dict(x) for x in d.execute('PRAGMA table_info("'+row['name']+'")')],foreign_keys=[dict(x) for x in d.execute('PRAGMA foreign_key_list("'+row['name']+'")')],indexes=[dict(x) for x in d.execute('PRAGMA index_list("'+row['name']+'")')])
        save_json(Path(c['report_root'])/'final/data_dictionary.json',dict(engine='SQLite',tables=dictionary,semantics=dict(movie_id='Source-anchored system work ID; provisional unless supported. Not ASIN.',person_id='External-context anchor or explicit provisional source context; not name-only.',release_date='Calendar date only; year-only data never fake January1.',genres='Normalized member table with assertion-specific provenance.',raw_reviews='Every source record retained, raw/clean columns separated.',logical_reviews='Exact within-product full-record hash, differences checked; mapping is raw_reviews.logical_review_id.',field_selections='NULL plus explicit reason when missing/unresolved; chosen and unchosen candidates retained.',source_records='Metadata/IMDb retained raw bytes plus archive version, sequence, offsets, URL and acquisition time.',review_field_lineage='Virtual per-review per-field provenance; no materialization inflation. Source registry records unknown historical download time honestly.')))
        return result
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/integration.example.json');a=p.parse_args();print(js(verify(load_config(a.config))))
if __name__=='__main__':main()
