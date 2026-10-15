from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/extract"))
import parse_reviews as parser
from register_source import register
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/quality"))
from verify_task1 import verify, compare


def review(asin="B000000001", user="U1", **changes):
    fields = {
        "product/productId": asin, "review/userId": user,
        "review/profileName": "  A name  ", "review/helpfulness": "0/0",
        "review/score": "5.0", "review/time": "1182729600",
        "review/summary": "Title: another colon",
        "review/text": "原文: <br />text &amp; more\u2028still the same physical line  ",
    }
    fields.update(changes)
    return ("\n".join(f"{k}: {v}" for k, v in fields.items()) + "\n").encode("utf-8")


class RecordTests(unittest.TestCase):
    def parse(self, raw, sequence=1):
        return parser.parse_record(raw, sequence, "a" * 64, (0, len(raw), 1, 8), "test")

    def test_preserves_text_and_zero_votes(self):
        row = self.parse(review())
        self.assertEqual(row["profile_name"], "  A name  ")
        self.assertEqual(row["review_text"], "原文: <br />text &amp; more\u2028still the same physical line  ")
        self.assertEqual(row["helpful_votes"], 0)
        self.assertIsNone(row["helpfulness_ratio"])
        self.assertEqual(row["quality_issues"], [])
        self.assertEqual(row["quarantine_reasons"], [])
        self.assertEqual(row["review_time_utc"], "2007-06-25T00:00:00+00:00")

    def test_stable_ids_and_locators(self):
        self.assertEqual(self.parse(review())["source_review_id"], self.parse(review())["source_review_id"])
        self.assertNotEqual(self.parse(review())["source_review_id"], self.parse(review(), 2)["source_review_id"])
        import io
        blob = b"\n" + review().replace(b"\n", b"\r\n") + b"\r\n" + review(user="U2").rstrip(b"\n")
        records = list(parser.iter_records(io.BytesIO(blob)))
        self.assertEqual(len(records), 2)
        for raw, start, end, *_ in records:
            self.assertEqual(blob[start:end], raw)
        resumed = list(parser.iter_records(io.BytesIO(blob), records[0][5], records[0][6]))
        self.assertEqual(resumed, records[1:])

    def test_quality_issues_do_not_quarantine(self):
        row = self.parse(review(user="", **{"review/score": "9", "review/helpfulness": "3/2", "review/time": "bad"}))
        self.assertFalse(row["quarantine_reasons"])
        self.assertIn("missing_user_id", row["quality_issues"])
        self.assertIn("invalid_score", row["quality_issues"])
        self.assertIn("helpful_votes_exceed_total", row["quality_issues"])
        self.assertIn("invalid_timestamp", row["quality_issues"])
        self.assertIsNone(row["score"])

    def test_structure_is_quarantined(self):
        row = self.parse(review(asin="") + b"review/score: 4\nunknown: retained in raw evidence\n")
        self.assertIn("missing_asin", row["quarantine_reasons"])
        self.assertIn("duplicate_field:review/score", row["quarantine_reasons"])
        self.assertIn("unrecognized_field_or_line", row["quarantine_reasons"])
        self.assertEqual(base64.b64decode(row["raw_record_base64"]), review(asin="") + b"review/score: 4\nunknown: retained in raw evidence\n")

    def test_invalid_encoding_keeps_exact_bytes(self):
        raw = review().replace("原文".encode("utf-8"), b"\xff")
        row = self.parse(raw)
        self.assertIn("invalid_utf8", row["quality_issues"])
        self.assertIsNone(row["review_text"])
        self.assertEqual(row["encoding_issue_fields"], ["review_text"])
        self.assertEqual(row["decoding_status"], "unresolved")
        self.assertEqual(base64.b64decode(row["raw_record_base64"]), raw)

    def test_latin1_is_explicitly_a_candidate(self):
        raw = review().replace(b"  A name  ", b"Fr\xf8nk\xfcnschtein")
        row = parser.parse_record(raw, 1, "a" * 64, (0, len(raw), 1, 8), "test", "latin1_candidate")
        self.assertEqual(row["profile_name"], "Frønkünschtein")
        self.assertEqual(row["decoding_status"], "latin1_candidate")
        self.assertEqual(base64.b64decode(row["raw_record_base64"]), raw)

    def test_blank_lines_inside_names_are_not_record_boundaries(self):
        import io
        raw = review().replace(b"  A name  ", b"First line\r\n\nSecond line: suffix")
        blob = raw + b"\n" + review(user="U2") + b"\n"
        records = list(parser.iter_records(io.BytesIO(blob)))
        self.assertEqual(len(records), 2)
        first = self.parse(records[0][0])
        self.assertEqual(first["profile_name"], "First line\r\n\nSecond line: suffix")
        self.assertEqual(first["quarantine_reasons"], [])
        self.assertIn("multiline_text_field", first["quality_issues"])
        for item in records:
            self.assertEqual(blob[item[1]:item[2]], item[0])
        resumed = list(parser.iter_records(io.BytesIO(blob), records[0][5], records[0][6]))
        self.assertEqual(records[1:], resumed)

    def test_multiline_review_text_and_summary_are_preserved(self):
        import io
        raw = review(**{"review/summary": "first\nsecond: subtitle", "review/text": "first\n\nsecond: part\nthird"})
        records = list(parser.iter_records(io.BytesIO(raw + b"\n" + review())))
        self.assertEqual(len(records), 2)
        row = self.parse(records[0][0])
        self.assertEqual(row["summary"], "first\nsecond: subtitle")
        self.assertEqual(row["review_text"], "first\n\nsecond: part\nthird")
        self.assertFalse(row["quarantine_reasons"])

    def test_incomplete_record_does_not_absorb_the_next_review(self):
        import io
        incomplete = b"product/productId: B000000003\nreview/userId: U3\n"
        records = list(parser.iter_records(io.BytesIO(incomplete + b"\n" + review())))
        self.assertEqual(len(records), 2)
        self.assertIn("missing_field:review/text", self.parse(records[0][0])["quarantine_reasons"])
        self.assertFalse(self.parse(records[1][0])["quarantine_reasons"])

    def test_numeric_overflow_and_published_window(self):
        row = self.parse(review(**{"review/helpfulness": "1/9999999999999999999999", "review/score": "nan", "review/time": "0"}))
        self.assertIn("invalid_helpfulness_format_or_range", row["quality_issues"])
        self.assertIn("invalid_score", row["quality_issues"])
        self.assertIn("outside_published_month_range", row["quality_issues"])


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "movies.txt"
        self.source.write_bytes(review() + b"\n" + review(user="U2") + b"\n" + review(asin="", user="U3") + b"\n" + review(asin="B000000002", user="") + b"\n" + review())
        self.manifest = self.root / "manifest.json"
        register(self.source, self.manifest)

    def args(self, name, **kwargs):
        result = argparse.Namespace(manifest=self.manifest, run_name=name, batch_size=2,
            max_records=None, stop_after_batches=None, resume=False,
            encoding_policy="unresolved",
            output_root=self.root / "output", report_root=self.root / "reports")
        for key, value in kwargs.items():
            setattr(result, key, value)
        return result

    def signature(self, name):
        root = self.root / "output" / name
        rows = []
        for folder in ("reviews", "quarantine"):
            for path in sorted((root / folder).glob("*.parquet")):
                rows.extend(pq.read_table(path).to_pylist())
        rows.sort(key=lambda row: row["record_sequence"])
        return [{k: v for k, v in row.items() if k != "run_id"} for row in rows]

    def test_repeat_and_completed_resume(self):
        first = parser.run(self.args("first"))
        repeat = parser.run(self.args("repeat"))
        again = parser.run(self.args("first", resume=True))
        self.assertEqual(first["records_read"], 5)
        self.assertEqual(first["valid_records"], 4)
        self.assertEqual(first["quarantined_records"], 1)
        self.assertEqual(first["distinct_products_all_parsed"], 2)
        self.assertEqual(first["distinct_users_all_parsed"], 3)
        self.assertEqual(first["distinct_users_valid_records"], 2)
        self.assertTrue(first["counts_reconcile"])
        self.assertEqual(first["ordered_records_fingerprint"], repeat["ordered_records_fingerprint"])
        self.assertEqual(first["ordered_records_fingerprint"], again["ordered_records_fingerprint"])
        self.assertEqual(self.signature("first"), self.signature("repeat"))
        self.assertEqual(len(self.signature("first")), 5)
        self.assertEqual(verify(self.root / "output/first")["records"], 5)
        self.assertTrue(compare(self.root / "output/first", self.root / "output/repeat")["all_fields_except_run_id_equal"])

    def test_interrupt_resume_equals_uninterrupted(self):
        interrupted = parser.run(self.args("resumed", stop_after_batches=1))
        self.assertEqual(interrupted["status"], "interrupted")
        self.assertEqual(interrupted["records_read"], 2)
        resumed = parser.run(self.args("resumed", resume=True))
        reference = parser.run(self.args("reference"))
        self.assertEqual(resumed["ordered_records_fingerprint"], reference["ordered_records_fingerprint"])
        self.assertEqual(self.signature("resumed"), self.signature("reference"))

    def test_uncommitted_file_recovery(self):
        write = pq.write_table
        calls = 0
        def fail_once(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 3:
                raise OSError("Injected write failure")
            return write(*args, **kwargs)
        with patch.object(parser.pq, "write_table", side_effect=fail_once):
            with self.assertRaises(OSError):
                parser.run(self.args("failed"))
        result = parser.run(self.args("failed", resume=True))
        reference = parser.run(self.args("reference"))
        self.assertEqual(result["ordered_records_fingerprint"], reference["ordered_records_fingerprint"])
        self.assertEqual(self.signature("failed"), self.signature("reference"))
        self.assertTrue(list((self.root / "output/failed/uncommitted").glob("*.parquet")))

    def test_detects_source_output_and_configuration_changes(self):
        parser.run(self.args("first"))
        with self.assertRaisesRegex(ValueError, "configuration"):
            parser.run(self.args("first", resume=True, batch_size=3))
        part = next((self.root / "output/first/reviews").glob("*.parquet"))
        part.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "missing or changed"):
            parser.run(self.args("first", resume=True))
        self.source.write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "manifest"):
            parser.run(self.args("another"))

    def test_sample_is_not_a_full_dataset(self):
        result = parser.run(self.args("sample", max_records=3))
        self.assertEqual(result["records_read"], 3)
        self.assertFalse(result["is_full_dataset"])
        self.assertIsNone(result["difference_from_official"])

    def test_parser_version_change_requires_a_separate_run(self):
        first = parser.run(self.args("old_version"))
        with patch.object(parser, "PARSER_VERSION", "test-next-version"):
            with self.assertRaisesRegex(ValueError, "configuration"):
                parser.run(self.args("old_version", resume=True))
            updated = parser.run(self.args("new_version"))
        self.assertEqual(updated["parser_version"], "test-next-version")
        self.assertNotEqual(first["parser_version"], updated["parser_version"])
        self.assertNotEqual(first["run_id"], updated["run_id"])
        # The source record identities remain stable even though the run version changed.
        self.assertEqual(first["ordered_records_fingerprint"], updated["ordered_records_fingerprint"])

    def test_archive_verification_and_missing_metadata(self):
        archive = self.root / "movies.txt.gz"
        archive.write_bytes(gzip.compress(self.source.read_bytes()))
        output = self.root / "with_archive.json"
        result = register(self.source, output, archive=archive)
        self.assertTrue(result["archive_to_text_verified"])
        self.assertIsNone(result["downloaded_at"])
        self.assertIsNone(result["source_url"])
        with self.assertRaises(FileExistsError):
            register(self.source, output)
        archive.write_bytes(gzip.compress(b"wrong"))
        with self.assertRaisesRegex(ValueError, "do not match"):
            register(self.source, self.root / "mismatch.json", archive=archive)

    def test_reported_date_does_not_invent_a_download_time(self):
        result = register(self.source, self.root / "reported.json", source_url="https://example.com/movies.txt.gz",
                          downloaded_on="2026-10-14", reported_evidence="user report",
                          reported_archive_size="3243938 KB")
        self.assertEqual(result["downloaded_on"], "2026-10-14")
        self.assertEqual(result["download_time_precision"], "date")
        self.assertIsNone(result["downloaded_at"])
        self.assertIsNone(result["archive_file"])


if __name__ == "__main__":
    unittest.main()
