"""Performance-only runtime wrapper: bounded 512MiB SQLite cache, same rules.

This wrapper does not modify the cleaning implementation/fingerprint or data
semantics. SQLite FULL durability and same committed batch cursor are retained.
"""
import argparse
from contextlib import closing
import logging
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent))
import clean_reviews as reviews
from integration_common import load_config,js,utc_now,ROOT

def run(c,stop_after_batches=None):
    original=reviews.open_reviews
    def tuned(config):
        d=original(config)
        d.execute('PRAGMA cache_size=-524288');d.execute('PRAGMA mmap_size=1073741824')
        with d:d.execute("UPDATE review_runs SET status='abandoned_recovered',finished_at=? WHERE status='running'",(utc_now(),))
        return d
    reviews.open_reviews=tuned
    try:return reviews.run(c,stop_after_batches)
    finally:reviews.open_reviews=original
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--config',type=Path,default=ROOT/'config/reviews.example.json');p.add_argument('--stop-after-batches',type=int);a=p.parse_args();c=load_config(a.config)
    logs=Path(c['report_root'])/'task8';logs.mkdir(parents=True,exist_ok=True)
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s',handlers=[logging.StreamHandler(),logging.FileHandler(logs/'execution.log',encoding='utf-8')])
    try:print(js(run(c,a.stop_after_batches)))
    except KeyboardInterrupt:raise SystemExit(130)
if __name__=='__main__':main()
