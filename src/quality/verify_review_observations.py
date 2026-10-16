"""Independently check all earliest-review observations against source Unix time."""
from contextlib import closing
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'src/transform'))
from integration_common import *

def verify(c):
    with closing(ro(Path(c['target_dir'])/'etl.sqlite3')) as d:
        d.execute('PRAGMA cache_size=-524288');d.execute('PRAGMA mmap_size=1073741824')
        observations=d.execute('SELECT count(*) FROM work_review_observations').fetchone()[0]
        bad=d.execute('''SELECT count(*) FROM work_review_observations o
         JOIN raw_reviews r ON o.source_review_id=r.source_review_id
         WHERE o.earliest_review_time!=r.review_time_utc OR o.source_file_sha256!=r.source_file_sha256
         OR o.byte_start!=r.byte_start OR o.byte_end!=r.byte_end OR o.inferred_release_bound IS NOT NULL
         OR r.review_time_unix!=(SELECT min(x.review_time_unix) FROM product_movie m
          CROSS JOIN raw_reviews x INDEXED BY review_product_index ON x.product_id=m.asin
          WHERE m.movie_id=o.movie_id AND m.relationship='single_work' AND x.review_time_utc IS NOT NULL)''').fetchone()[0]
        if bad:raise ValueError('Earliest review/source/inference semantics mismatch')
    result=dict(status='passed_full_earliest_observation_replay',work_observations=observations,mismatches=bad,
        semantics='Every returned work observation checked against minimum valid source Unix time, contributing raw review, source hash/offsets. No release inference made.')
    save_json(Path(c['report_root'])/'final/review_observation_verification.json',result);return result
if __name__=='__main__':print(js(verify(load_config())))
