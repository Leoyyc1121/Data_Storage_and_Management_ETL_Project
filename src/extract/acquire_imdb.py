"""Acquire only officially offered non-commercial IMDb bulk files; never scrape pages."""
from __future__ import annotations
import argparse
import gzip
import json
import logging
import os
from pathlib import Path
import re
import sys
import time
from urllib import request,error

from acquire_products import HTTPClient, Policy, load_config as load_task2, run_lock, save_body, save_json
from register_source import hash_file,utc_now

ROOT=Path(__file__).resolve().parents[2]
BASE="https://datasets.imdbws.com/"
ALLOWED={"title.basics.tsv.gz","title.crew.tsv.gz","title.principals.tsv.gz","name.basics.tsv.gz"}


def load_config(path):
    config=json.loads(Path(path).read_text(encoding="utf-8-sig"))
    for key in ("raw_dir","work_dir","report_dir","task4_config"):
        config[key]=str((ROOT/config[key]).resolve())
    if not set(config["files"])<=ALLOWED or not config["source_use"] or config["max_archive_bytes"]<=0:
        raise ValueError("Unsupported file/use/size limits")
    return config


def download(config,name,client):
    root=Path(config["raw_dir"]);root.mkdir(parents=True,exist_ok=True)
    path=root/name;record=root/(name+".manifest.json")
    if record.exists():
        registration=json.loads(record.read_text(encoding="utf-8"))
        if hash_file(path)!=registration["sha256"]:
            raise ValueError("Registered IMDb archive changed; do not overwrite")
        return registration
    if path.exists():
        raise ValueError("Unregistered final archive exists; preserve and inspect it")
    partial=path.with_suffix(path.suffix+".part");transfer_path=root/(name+".transfer.json")
    transfer=json.loads(transfer_path.read_text()) if transfer_path.exists() else {}
    for attempt in range(1,4):
        offset=partial.stat().st_size if partial.exists() and transfer.get("etag") and not transfer["etag"].startswith("W/") else 0
        headers={"User-Agent":client.config["user_agent"],"Accept-Encoding":"identity"}
        if offset:headers.update(Range=f"bytes={offset}-",**{"If-Range":transfer["etag"]})
        time.sleep(client.interval)
        try:
            with client.opener.open(request.Request(BASE+name,headers=headers),timeout=30) as response:
                if response.code not in {200,206}:raise ValueError("Unexpected download response")
                if response.code==206:
                    match=re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)",response.headers.get("Content-Range",""))
                    if not match or int(match[1])!=offset or not offset or response.headers.get("ETag")!=transfer["etag"]:
                        raise ValueError("Range/ETag mismatch; do not splice archive versions")
                    total_size=int(match[3]);mode="ab"
                else:
                    offset=0;mode="wb";total_size=int(response.headers["Content-Length"]) if response.headers.get("Content-Length") else None
                if total_size and total_size>config["max_archive_bytes"]:raise ValueError("Archive too large")
                transfer=dict(url=BASE+name,observed_at=utc_now(),http_status=response.code,headers=dict(response.headers),etag=response.headers.get("ETag"),expected_bytes=total_size,resumed_from=offset)
                save_json(transfer_path,transfer)
                total=offset;last=offset
                with partial.open(mode) as output:
                    while chunk:=response.read(1024*1024):
                        total+=len(chunk)
                        if total>config["max_archive_bytes"]:raise ValueError("Archive size limit exceeded")
                        output.write(chunk)
                        if total-last>=32*1024*1024:logging.info("%s downloaded %.1f MiB",name,total/1024**2);last=total
                if total_size and total!=total_size:raise OSError("Incomplete response")
                with partial.open("rb") as stream:
                    if stream.read(2)!=b"\x1f\x8b":raise ValueError("Not gzip; error page cannot be data")
            os.replace(partial,path)
            registration=dict(source_id="imdb:"+name,url=BASE+name,path=str(path.resolve()),bytes=path.stat().st_size,sha256=hash_file(path),downloaded_at=utc_now(),transfer=transfer,
                publisher="https://data.imdb.com/non-commercial-datasets/",license="https://help.imdb.com/article/imdb/general-information/can-i-use-imdb-data-in-my-software/G5JTRESSHJBBHTGX",use=config["source_use"],
                attribution="Information courtesy of IMDb (https://www.imdb.com). Used with permission.",historical_page_acquisition_time=None,gzip_crc_status="pending_full_import")
            save_json(record,registration);return registration
        except error.HTTPError as exc:
            with exc:body=exc.read(1024*1024)
            snap,digest=save_body(root,body,"download_errors",".html")
            save_json(root/(name+f".failure{attempt}.json"),dict(http_status=exc.code,at=utc_now(),snapshot=snap,sha256=digest))
            if exc.code in {401,403,429} or exc.code<500 or attempt==3:raise
            retry=exc.headers.get("Retry-After")
            wait=float(retry) if retry and retry.isdigit() else 2**attempt
            if wait>60:raise
            time.sleep(wait)
        except (OSError,error.URLError):
            if attempt==3:raise
            time.sleep(2**attempt)


def acquire(config):
    reports=Path(config["report_dir"]);reports.mkdir(parents=True,exist_ok=True)
    settings=load_task2(ROOT/"config/task2.example.json")
    settings.update(base_url=BASE.rstrip("/"),raw_dir=config["raw_dir"],request_interval_seconds=3.0)
    client=HTTPClient(settings);policy=Policy(settings,client)
    evidence=policy.preflight();save_json(reports/"access_policy.json",evidence)
    if policy.parser is None or any(not policy.parser.can_fetch(BASE+name,settings["robots_agent"]) for name in config["files"]):
        save_json(reports/"acquisition.json",dict(status="source_access_unavailable",observed_at=utc_now(),policy=evidence,files=[]))
        return []
    files=[]
    with run_lock(config["raw_dir"]):
        for name in config["files"]:
            files.append(download(config,name,client))
            save_json(reports/"acquisition.json",dict(status="complete" if len(files)==len(config["files"]) else "in_progress",observed_at=utc_now(),files=files,policy=evidence))
    return files


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--config",type=Path,default=ROOT/"config/imdb.example.json");args=parser.parse_args()
    config=load_config(args.config);Path(config["report_dir"]).mkdir(parents=True,exist_ok=True)
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s",handlers=[logging.StreamHandler(),logging.FileHandler(Path(config["report_dir"])/"execution.log",encoding="utf-8")])
    result=acquire(config);print(json.dumps(dict(archives=len(result),bytes=sum(f["bytes"] for f in result)),indent=2))


if __name__=="__main__":main()
