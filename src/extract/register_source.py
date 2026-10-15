"""Register existing review files without inventing download evidence."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def hash_stream(stream):
    digest = hashlib.sha256()
    while chunk := stream.read(8 * 1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def hash_file(path):
    with Path(path).open("rb") as stream:
        return hash_stream(stream)


def describe_file(path):
    path = Path(path).resolve()
    before = path.stat()
    digest = hash_file(path)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError("Source file changed during hashing")
    return {"path": str(path), "file_name": path.name,
            "file_size_bytes": after.st_size, "sha256": digest,
            "observed_mtime_ns": after.st_mtime_ns}


def register(source, output, archive=None, source_url=None,
             download_method=None, downloaded_at=None, downloaded_on=None,
             reported_evidence=None, reported_archive_size=None):
    source = Path(source).resolve()
    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError(f"Existing manifest preserved: {output}")
    manifest = {"manifest_version": "1", "observed_at": utc_now(),
                "dataset": "Stanford SNAP Amazon movie reviews",
                "source_url": source_url, "download_method": download_method,
                "downloaded_at": downloaded_at,
                "downloaded_on": downloaded_on,
                "download_time_precision": "timestamp" if downloaded_at else "date" if downloaded_on else "unknown",
                "reported_evidence": reported_evidence,
                "reported_archive_size": reported_archive_size,
                "download_evidence_note": "Missing historical information remains null; observed_at is not download time.",
                "source_file": describe_file(source), "archive_file": None,
                "archive_to_text_verified": False,
                "record_id_basis": "SHA256(uncompressed_file_sha256 + ':' + one_based_record_sequence)"}
    if archive:
        manifest["archive_file"] = describe_file(archive)
        with gzip.open(archive, "rb") as stream:
            uncompressed_hash = hash_stream(stream)
        if uncompressed_hash != manifest["source_file"]["sha256"]:
            raise ValueError("Archive contents do not match the supplied text file")
        manifest["archive_to_text_verified"] = True
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--source-url")
    parser.add_argument("--download-method")
    parser.add_argument("--downloaded-at", help="ISO 8601 timestamp with timezone; omit when unknown")
    parser.add_argument("--downloaded-on", help="Known download date YYYY-MM-DD; does not imply an exact time")
    parser.add_argument("--reported-evidence", help="Document or other user-provided evidence for historical facts")
    parser.add_argument("--reported-archive-size", help="Reported size label; not a measured byte count")
    args = parser.parse_args()
    if args.downloaded_at:
        timestamp = datetime.fromisoformat(args.downloaded_at)
        if timestamp.tzinfo is None:
            parser.error("--downloaded-at must include a timezone")
    if args.downloaded_on:
        date.fromisoformat(args.downloaded_on)
    manifest = register(args.source, args.output, args.archive, args.source_url,
                        args.download_method, args.downloaded_at, args.downloaded_on,
                        args.reported_evidence, args.reported_archive_size)
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
