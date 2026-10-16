"""Stream complete retained IMDb archives, preserving matched TSV raw locators.

Subset selection is by exact normalized source titles; it is NOT movie matching.
Crew and principal cast are not confused with complete cast or starring billing.
"""
from __future__ import annotations
import argparse
from contextlib import closing
import gzip
import hashlib
import json
import logging
from pathlib import Path
import sqlite3
import sys

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/"src/transform"))
from resolve_movies import load_config as load_task4,normalize_title,digest_json
from acquire_imdb import load_config
from acquire_products import run_lock,save_json
from register_source import hash_file,utc_now

HEADERS={
 "title.basics.tsv.gz":["tconst","titleType","primaryTitle","originalTitle","isAdult","startYear","endYear","runtimeMinutes","genres"],
 "title.crew.tsv.gz":["tconst","directors","writers"],
 "title.principals.tsv.gz":["tconst","ordering","nconst","category","job","characters"],
 "name.basics.tsv.gz":["nconst","primaryName","birthYear","deathYear","primaryProfession","knownForTitles"]}


def open_db(config):
    Path(config["work_dir"]).mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(Path(config["work_dir"])/"catalog.sqlite3");db.row_factory=sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON");db.execute("PRAGMA journal_mode=WAL")
    db.executescript("""
     CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY,payload TEXT NOT NULL);
     CREATE TABLE IF NOT EXISTS sources(file TEXT PRIMARY KEY,registration TEXT NOT NULL);
     CREATE TABLE IF NOT EXISTS raw_records(source_record_id TEXT PRIMARY KEY,file TEXT REFERENCES sources(file),record_sequence INTEGER,byte_start INTEGER,byte_end INTEGER,raw_sha256 TEXT,raw_record BLOB,UNIQUE(file,record_sequence));
     CREATE TABLE IF NOT EXISTS titles(tconst TEXT PRIMARY KEY,title_type TEXT,primary_title TEXT,original_title TEXT,release_year INTEGER,runtime INTEGER,genres TEXT,source_record_id TEXT REFERENCES raw_records(source_record_id));
     CREATE TABLE IF NOT EXISTS title_lookup(normalized_title TEXT,tconst TEXT REFERENCES titles(tconst),PRIMARY KEY(normalized_title,tconst));
     CREATE TABLE IF NOT EXISTS crew(tconst TEXT PRIMARY KEY REFERENCES titles(tconst),directors TEXT,writers TEXT,source_record_id TEXT REFERENCES raw_records(source_record_id));
     CREATE TABLE IF NOT EXISTS principals(tconst TEXT REFERENCES titles(tconst),ordering INTEGER,nconst TEXT,category TEXT,job TEXT,characters TEXT,source_record_id TEXT REFERENCES raw_records(source_record_id),PRIMARY KEY(tconst,ordering));
     CREATE TABLE IF NOT EXISTS names(nconst TEXT PRIMARY KEY,primary_name TEXT,birth_year INTEGER,death_year INTEGER,professions TEXT,known_for TEXT,source_record_id TEXT REFERENCES raw_records(source_record_id));
     CREATE TABLE IF NOT EXISTS issues(file TEXT,record_sequence INTEGER,reason TEXT,raw_record BLOB,PRIMARY KEY(file,record_sequence));
     CREATE INDEX IF NOT EXISTS principal_person ON principals(nconst);
    """)
    return db


def get_state(db,key):
    row=db.execute("SELECT payload FROM state WHERE key=?",(key,)).fetchone()
    return json.loads(row[0]) if row else None


def missing(value):return None if value=="\\N" else value
def number(value):return None if value=="\\N" else int(value)
def array(value):return [] if value=="\\N" else value.split(",")


def import_file(db,config,file,registration,wanted_titles,stop_after_batches=None):
    existing=db.execute("SELECT registration FROM sources WHERE file=?",(file,)).fetchone()
    if existing and json.loads(existing[0])["sha256"]!=registration["sha256"]:raise ValueError("Archive version changed; new work_dir required")
    with db:db.execute("INSERT OR IGNORE INTO sources VALUES (?,?)",(file,json.dumps(registration)))
    state=get_state(db,file) or dict(sequence=1,next_offset=0,read=0,kept=0,issues=0,complete=False)
    if state["complete"]:return state
    selected_titles={r[0] for r in db.execute("SELECT tconst FROM titles")}
    people={r[0] for r in db.execute("SELECT DISTINCT nconst FROM principals")}
    for row in db.execute("SELECT directors,writers FROM crew"):
        people.update(json.loads(row[0]));people.update(json.loads(row[1]))
    selected_title_bytes={key.encode() for key in selected_titles};people_bytes={key.encode() for key in people}
    batches=0
    with gzip.open(registration["path"],"rb") as stream:
        header=stream.readline().decode("utf-8").strip().split("\t")
        if header!=HEADERS[file]:raise ValueError(f"Changed IMDb schema: {file}")
        if state["next_offset"]:stream.seek(state["next_offset"])
        else:state["next_offset"]=stream.tell()
        while True:
            start=stream.tell();raw=stream.readline(8*1024*1024+1)
            if not raw:
                state["complete"]=True;state["gzip_crc_verified"]=True
                with db:db.execute("INSERT OR REPLACE INTO state VALUES (?,?)",(file,json.dumps(state)))
                break
            if len(raw)>8*1024*1024:raise ValueError("Oversized TSV record; source preserved, import stopped")
            state["sequence"]+=1;state["read"]+=1;state["next_offset"]=stream.tell()
            cols=raw.rstrip(b"\r\n").split(b"\t")
            try:
                if len(cols)!=len(header):raise ValueError("column_count")
                key=cols[0]
                keep=False;norms=set()
                if file=="title.basics.tsv.gz":
                    if cols[1] in {b"movie",b"tvMovie",b"short"}:
                        norms={normalize_title(cols[2].decode("utf-8")),normalize_title(cols[3].decode("utf-8"))}
                        keep=bool(norms&wanted_titles)
                elif file=="name.basics.tsv.gz":keep=key in people_bytes
                else:keep=key in selected_title_bytes
                if keep:
                    values=[c.decode("utf-8") for c in cols]
                    sid="imdb:"+digest_json([registration["sha256"],state["sequence"]])
                    db.execute("INSERT INTO raw_records VALUES (?,?,?,?,?,?,?)",(sid,file,state["sequence"],start,stream.tell(),hashlib.sha256(raw).hexdigest(),raw))
                    if file=="title.basics.tsv.gz":
                        db.execute("INSERT INTO titles VALUES (?,?,?,?,?,?,?,?)",(values[0],values[1],values[2],values[3],number(values[5]),number(values[7]),json.dumps(array(values[8])),sid))
                        db.executemany("INSERT OR IGNORE INTO title_lookup VALUES (?,?)",[(norm,values[0]) for norm in norms])
                    elif file=="title.crew.tsv.gz":db.execute("INSERT INTO crew VALUES (?,?,?,?)",(values[0],json.dumps(array(values[1])),json.dumps(array(values[2])),sid))
                    elif file=="title.principals.tsv.gz":db.execute("INSERT INTO principals VALUES (?,?,?,?,?,?,?)",(values[0],int(values[1]),values[2],values[3],missing(values[4]),missing(values[5]),sid))
                    else:db.execute("INSERT INTO names VALUES (?,?,?,?,?,?,?)",(values[0],values[1],number(values[2]),number(values[3]),json.dumps(array(values[4])),json.dumps(array(values[5])),sid))
                    state["kept"]+=1
            except (UnicodeError,ValueError) as exc:
                db.execute("INSERT INTO issues VALUES (?,?,?,?)",(file,state["sequence"],str(exc),raw));state["issues"]+=1
            if state["read"]%50000==0:
                db.execute("INSERT OR REPLACE INTO state VALUES (?,?)",(file,json.dumps(state)));db.commit();batches+=1
                if state["read"]%1000000==0:logging.info("%s read=%s kept=%s",file,state["read"],state["kept"])
                if stop_after_batches and batches>=stop_after_batches:raise KeyboardInterrupt
    return state


def run(config):
    task4=load_task4(config["task4_config"])
    with closing(sqlite3.connect((Path(task4["work_dir"])/"resolution.sqlite3").as_uri()+"?mode=ro",uri=True)) as source:
        wanted=set()
        for profile, in source.execute("SELECT profile FROM products WHERE classification='movie'"):
            wanted.update(json.loads(profile)["titles"])
    acquisition=json.loads((Path(config["report_dir"])/"acquisition.json").read_text(encoding="utf-8"))
    if acquisition["status"]!="complete":raise ValueError("Download stage not complete; no invented catalog")
    files={Path(f["path"]).name:f for f in acquisition["files"]}
    fingerprint=digest_json(dict(wanted_titles=sorted(wanted),archives={key:value["sha256"] for key,value in files.items()},parser=hash_file(__file__)))
    with run_lock(config["work_dir"]),closing(open_db(config)) as db:
        old=get_state(db,"input")
        if old and old!=fingerprint:raise ValueError("Source/selection/parser changed; use new version directory")
        with db:db.execute("INSERT OR IGNORE INTO state VALUES ('input',?)",(json.dumps(fingerprint),))
        for file in config["files"]:
            if hash_file(files[file]["path"])!=files[file]["sha256"]:raise ValueError("Source archive changed")
            logging.info("Import complete source file: %s",file)
            import_file(db,config,file,files[file],wanted)
        states={row["key"]:json.loads(row["payload"]) for row in db.execute("SELECT * FROM state WHERE key!='input'")}
        result=dict(observed_at=utc_now(),status="complete",input_fingerprint=fingerprint,selected_title_forms=len(wanted),
            titles=db.execute("SELECT COUNT(*) FROM titles").fetchone()[0],names=db.execute("SELECT COUNT(*) FROM names").fetchone()[0],
            principal_credits=db.execute("SELECT COUNT(*) FROM principals").fetchone()[0],raw_source_records=db.execute("SELECT COUNT(*) FROM raw_records").fetchone()[0],states=states,
            scope="Complete bulk files streamed/CRC-checked; selected raw rows retained by title-key filter. Title selection is not movie identity verification. Principal cast, not full cast or starring.",catalog_path=str((Path(config["work_dir"])/"catalog.sqlite3").resolve()))
        save_json(Path(config["report_dir"])/"catalog_statistics.json",result)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--config",type=Path,default=ROOT/"config/imdb.example.json");args=parser.parse_args()
    config=load_config(args.config);logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run(config),indent=2))


if __name__=="__main__":main()
