"""Synthetic program tests only; these are NOT acquired Amazon product evidence."""
from __future__ import annotations

from copy import deepcopy
import io
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/extract"))
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src/quality"))
import acquire_products as acquisition
from verify_task2 import verify

ASIN = "B000000001"
OTHER = "B000000002"
BASE = "https://example.test"


def html(asin=ASIN, title=True):
    return ("<html><head><title>Synthetic test fixture</title></head><body>" +
        ("<span id='productTitle'>Synthetic title not real product data</span>" if title else "") +
        f"<input id='ASIN' value='{asin}'></body></html>").encode()


def response(asin=ASIN, status=200, body=None, **kwargs):
    return acquisition.Response(f"{BASE}/dp/{asin}", status,
        html(asin) if body is None else body, {"content-type": "text/html; charset=utf-8"}, **kwargs)


class FakeClient:
    def __init__(self, responses=None, robots=b"User-agent: *\nAllow: /\n"):
        self.responses = responses or {}
        self.robots = robots
        self.calls, self.waits = [], []
        self.interval = 0.01
        self.request_count = 0
        self.sleep = self.waits.append

    def get(self, url):
        self.calls.append(url)
        self.request_count += 1
        if url == BASE + "/robots.txt":
            if self.responses.get(url):
                return self.responses[url].pop(0)
            return acquisition.Response(url, 200, self.robots, {"content-type": "text/plain"})
        candidates = self.responses.get(url)
        if not candidates:
            raise AssertionError("Unexpected network request: " + url)
        return candidates.pop(0)


class Task2Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.config = json.loads((acquisition.ROOT / "config/task2.example.json").read_text())
        self.config.update(base_url=BASE, request_interval_seconds=0.01,
            source_run=str(self.root / "task1"), work_dir=str(self.root / "task2"),
            raw_dir=str(self.root / "raw"), report_dir=str(self.root / "reports"))
        source = Path(self.config["source_run"])
        source.mkdir()
        state = {"status": "complete", "max_records": None, "records_read": 3,
            "run_id": "synthetic-task1", "source_sha256": "0" * 64,
            "parser_version": "test", "rule_version": "synthetic-only"}
        self.source_db = sqlite3.connect(source / "checkpoint.sqlite3")
        self.addCleanup(self.source_db.close)
        self.source_db.executescript("CREATE TABLE state(payload TEXT); CREATE TABLE batches(batch_id INTEGER,payload TEXT); CREATE TABLE asins(id TEXT PRIMARY KEY,raw_records INTEGER);")
        self.source_db.execute("INSERT INTO state VALUES (?)", (json.dumps(state),))
        self.source_db.executemany("INSERT INTO asins VALUES (?,?)", [(ASIN, 2), (OTHER, 1)])
        for number, asins in enumerate(([ASIN, OTHER], [ASIN]), 1):
            file = source / f"part-{number}.parquet"
            pq.write_table(pa.Table.from_pylist([{"product_id": asin, "source_review_id": f"synthetic-{number}-{i}"}
                for i, asin in enumerate(asins)]), file)
            batch = {"batch_id": number, "records": len(asins), "files": [{"path": file.name,
                "rows": len(asins), "sha256": acquisition.hash_file(file)}]}
            self.source_db.execute("INSERT INTO batches VALUES (?,?)", (number, json.dumps(batch)))
        self.source_db.commit()
        self.db = acquisition.open_db(self.config["work_dir"])
        self.addCleanup(self.db.close)

    def ready(self):
        acquisition.prepare(self.db, self.config)

    def allow_live(self):
        permission = self.root / "synthetic-permission.json"
        acquisition.save_json(permission, {"origin": BASE, "allowed": True,
            "reviewed_by": "synthetic unit-test fixture", "reviewed_at": "2026-10-15T00:00:00+00:00",
            "basis": "No real requests; mocked example.test transport only"})
        self.config.update(permission_evidence=str(permission))

    def product(self, asin=ASIN):
        return dict(self.db.execute("SELECT * FROM products WHERE asin=?", (asin,)).fetchone())

    def test_prepare_complete_unique_manifest(self):
        self.ready()
        self.assertEqual(self.product()["review_count"], 2)
        self.assertEqual(self.product(OTHER)["review_count"], 1)
        self.assertEqual(self.product()["original_url"], BASE + "/dp/" + ASIN)
        self.assertEqual(self.product()["attempts"], 0)

    def test_cli_prepare_includes_export_and_verification(self):
        config_path = self.root / 'config.json'
        acquisition.save_json(config_path, self.config)
        result = subprocess.run([sys.executable, acquisition.__file__, 'prepare', '--config', str(config_path)],
            capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        verification = json.loads((Path(self.config['report_dir']) / 'full_verification.json').read_text())
        self.assertEqual(verification['status'], 'passed')
        self.assertEqual(verification['products'], 2)

    def test_prepare_idempotent_and_export_stable(self):
        self.ready()
        first = acquisition.export(self.db, self.config)
        self.ready()
        second = acquisition.export(self.db, self.config)
        self.assertEqual(first["product_manifest"], second["product_manifest"])

    def test_prepare_interruption_resume_exactly_once(self):
        with self.assertRaises(KeyboardInterrupt):
            acquisition.prepare(self.db, self.config, stop_after_batches=1)
        self.assertEqual(self.product()["review_count"], 1)
        self.assertIsNone(acquisition.meta(self.db, "prepared"))
        self.ready()
        self.assertEqual(self.product()["review_count"], 2)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM input_batches").fetchone()[0], 2)

    def test_changed_input_rejected(self):
        self.ready()
        file = Path(self.config["source_run"]) / "part-1.parquet"
        file.write_bytes(b"tampered")
        with self.assertRaises(ValueError):
            self.ready()

    def test_sample_input_rejected(self):
        state = json.loads(self.source_db.execute("SELECT payload FROM state").fetchone()[0])
        state["max_records"] = 3
        self.source_db.execute("UPDATE state SET payload=?", (json.dumps(state),))
        self.source_db.commit()
        with self.assertRaises(ValueError):
            self.ready()

    def test_changed_url_rules_rejected(self):
        self.ready()
        self.config["base_url"] = "https://other.test"
        with self.assertRaises(ValueError):
            acquisition.crawl(self.db, self.config, "test", client=FakeClient())

    def test_success_requires_body_identity(self):
        self.assertEqual(acquisition.classify(ASIN, response())["status"], "success")
        data = b"<html><span id='productTitle'>Synthetic</span></html>"
        self.assertEqual(acquisition.classify(ASIN, response(body=data))["reason"], "body_asin_not_verified")

    def test_related_data_asin_not_identity(self):
        data = f"<html><span id='productTitle'>Synthetic</span><div data-asin='{ASIN}'></div></html>".encode()
        self.assertNotEqual(acquisition.classify(ASIN, response(body=data))["status"], "success")

    def test_mismatched_identity_rejected(self):
        self.assertEqual(acquisition.classify(ASIN, response(body=html(OTHER)))["reason"], "asin_mismatch_or_ambiguous_identity")
        value = response()
        value.url = f"{BASE}/dp/{OTHER}"
        self.assertNotEqual(acquisition.classify(ASIN, value)["status"], "success")

    def test_http_200_challenge_not_success(self):
        for body in (b"<html>Robot Check</html>", b"<html>validateCaptcha</html>", b"<html>Access denied</html>"):
            self.assertEqual(acquisition.classify(ASIN, response(body=body))["status"], "restricted_access")

    def test_http_200_error_not_success(self):
        self.assertEqual(acquisition.classify(ASIN, response(body=b"<html>Page not found</html>"))["status"], "not_found")

    def test_statuses_not_found_restricted_timeout(self):
        for status, expected in [(404, "not_found"), (410, "not_found"), (403, "restricted_access"), (429, "restricted_access"), (500, "http_error")]:
            self.assertEqual(acquisition.classify(ASIN, response(status=status))["status"], expected)
        self.assertEqual(acquisition.classify(ASIN, acquisition.Response(BASE, error="timeout"))["status"], "timeout")

    def test_missing_product_marker_rejected(self):
        self.assertNotEqual(acquisition.classify(ASIN, response(body=html(title=False)))["status"], "success")

    def test_declared_charset_and_raw_preservation(self):
        value = response(body=html() + b"\xff")
        self.assertEqual(acquisition.classify(ASIN, value)["reason"], "undecodable_html_preserved_raw")
        value.headers["content-type"] = "text/html; charset=iso-8859-1"
        self.assertEqual(acquisition.classify(ASIN, value)["status"], "success")

    def test_oversize_and_wrong_content_type_rejected(self):
        self.assertEqual(acquisition.classify(ASIN, response(truncated=True))["reason"], "response_exceeds_size_limit")
        value = response()
        value.headers["content-type"] = "application/json"
        self.assertNotEqual(acquisition.classify(ASIN, value)["status"], "success")

    def test_json_ld_product_identity(self):
        data = f'<html><script type="application/ld+json">{{"@type":"Product","sku":"{ASIN}"}}</script></html>'.encode()
        self.assertEqual(acquisition.classify(ASIN, response(body=data))["status"], "success")

    def test_robots_denial_has_zero_product_attempts(self):
        self.ready()
        self.allow_live()
        client = FakeClient(robots=b"User-agent: ChatGPT-User\nDisallow: /\n")
        result = acquisition.crawl(self.db, self.config, "test", client=client)
        self.assertEqual(result["network_requests"], 1)
        self.assertEqual(self.product()["status"], "restricted_access")
        self.assertEqual(self.product()["attempts"], 0)
        self.assertIsNone(self.product()["http_status"])
        self.assertIsNone(self.product()["acquired_at"])
        acquisition.export(self.db, self.config)
        self.assertEqual(verify(self.config)["policy_denied_urls_checked"], 2)

    def test_permission_gate_even_when_robots_allow(self):
        self.ready()
        acquisition.crawl(self.db, self.config, "test", client=FakeClient())
        self.assertEqual(self.product()["reason"], "source_terms_not_reviewed")
        self.assertEqual(self.product()["status"], "not_collected")
        self.assertEqual(self.product()["attempts"], 0)

    def test_unavailable_policy_is_pending_and_repeat_stable(self):
        self.ready()
        client = FakeClient(robots=b'<html>Temporary service failure</html>')
        acquisition.crawl(self.db, self.config, 'first', client=client)
        self.assertEqual(self.product()['status'], 'not_collected')
        first = acquisition.export(self.db, self.config)
        acquisition.crawl(self.db, self.config, 'second', client=FakeClient())
        second = acquisition.export(self.db, self.config)
        self.assertEqual(first['product_manifest'], second['product_manifest'])
        self.assertEqual(verify(self.config)['policy_pending_urls_checked'], 2)

    def test_policy_cache_no_refetch(self):
        client = FakeClient()
        acquisition.Policy(self.config, client).preflight()
        second = FakeClient()
        acquisition.Policy(self.config, second).preflight()
        self.assertEqual(second.request_count, 0)

    def test_robots_transient_retry_then_recovery(self):
        url = BASE + '/robots.txt'
        client = FakeClient({url: [acquisition.Response(url, 503, b'Temporary failure'),
            acquisition.Response(url, 200, b'User-agent: *\nAllow: /\n')]})
        policy = acquisition.Policy(self.config, client)
        evidence = policy.preflight()
        self.assertEqual(client.request_count, 2)
        self.assertEqual(len(evidence['attempt_evidence_paths']), 2)
        self.assertIsNotNone(policy.parser)

    def test_robots_retries_bounded_and_all_responses_preserved(self):
        url = BASE + '/robots.txt'
        client = FakeClient({url: [acquisition.Response(url, 503, str(i).encode()) for i in range(3)]})
        policy = acquisition.Policy(self.config, client)
        evidence = policy.preflight()
        self.assertEqual(client.request_count, 3)
        self.assertIsNone(policy.parser)
        for path in evidence['attempt_evidence_paths']:
            self.assertTrue(Path(path).exists())

    def test_failed_robots_short_cache_expires(self):
        first = FakeClient(robots=b'<html>Temporary service failure</html>')
        policy = acquisition.Policy(self.config, first)
        policy.preflight()
        path = Path(self.config['raw_dir']) / 'policy/robots_latest.json'
        value = json.loads(path.read_text())
        value['observed_at'] = '2000-01-01T00:00:00+00:00'
        acquisition.save_json(path, value)
        second = FakeClient()
        policy = acquisition.Policy(self.config, second)
        policy.preflight()
        self.assertEqual(second.request_count, 1)
        self.assertIsNotNone(policy.parser)

    def test_force_policy_refresh_keeps_identity_and_denial(self):
        first = FakeClient()
        acquisition.Policy(self.config, first).preflight()
        second = FakeClient(robots=b'User-agent: ChatGPT-User\nDisallow: /\n')
        policy = acquisition.Policy(self.config, second)
        policy.preflight(force_refresh=True)
        self.assertEqual(second.request_count, 1)
        self.assertEqual(policy.decision(BASE + '/dp/' + ASIN), (False, 'robots_disallow'))
        self.assertIn('ChatGPT-User', self.config['user_agent'])

    def test_robots_403_is_not_retried(self):
        url = BASE + '/robots.txt'
        client = FakeClient({url: [acquisition.Response(url, 403, b'Access denied')]})
        policy = acquisition.Policy(self.config, client)
        policy.preflight()
        self.assertEqual(client.request_count, 1)
        self.assertIsNone(policy.parser)

    def test_robots_404_distinguished_from_server_failure(self):
        url = BASE + '/robots.txt'
        client = FakeClient({url: [acquisition.Response(url, 404, b'Not found')]})
        policy = acquisition.Policy(self.config, client)
        policy.preflight()
        self.assertIsNotNone(policy.parser)
        self.assertEqual(policy.decision(BASE + '/dp/' + ASIN), (False, 'source_terms_not_reviewed'))

    def test_robots_retry_after_long_wait_stops(self):
        url = BASE + '/robots.txt'
        client = FakeClient({url: [acquisition.Response(url, 503, b'Temporary failure', {'retry-after': '120'})]})
        acquisition.Policy(self.config, client).preflight()
        self.assertEqual(client.request_count, 1)
        self.assertEqual(client.waits, [])

    def test_failed_robots_fail_closed(self):
        client = FakeClient()
        client.robots = b"<html>Unexpected challenge</html>"
        policy = acquisition.Policy(self.config, client)
        policy.preflight()
        self.assertEqual(policy.decision(BASE + "/dp/" + ASIN), (False, "robots_unavailable_or_unparseable_fail_closed"))

    def test_robots_wildcards_and_crawl_delay(self):
        self.allow_live()
        client = FakeClient(robots=b'User-agent: *\nDisallow: /dp/*1$\nCrawl-delay: 7\n')
        policy = acquisition.Policy(self.config, client)
        policy.preflight()
        self.assertFalse(policy.decision(BASE + '/dp/' + ASIN)[0])
        self.assertTrue(policy.decision(BASE + '/dp/' + OTHER)[0])
        self.assertEqual(client.interval, 7)

    def test_retry_timeout_then_success_and_cache(self):
        self.ready()
        self.allow_live()
        url = BASE + "/dp/" + ASIN
        client = FakeClient({url: [acquisition.Response(url, error="timeout"), response()]})
        acquisition.crawl(self.db, self.config, "test", max_products=1, client=client)
        self.assertEqual(self.product()["attempts"], 2)
        self.assertEqual(self.product()["status"], "success")
        self.assertEqual(self.product(OTHER)["status"], "not_collected")
        second = FakeClient({BASE + "/dp/" + OTHER: [response(OTHER)]})
        acquisition.crawl(self.db, self.config, "again", client=second)
        self.assertNotIn(url, second.calls)
        acquisition.export(self.db, self.config)
        self.assertEqual(verify(self.config)["successful_page_evidence_checked"], 2)

    def test_retry_budget_bounded_across_runs(self):
        self.ready()
        self.allow_live()
        url = BASE + "/dp/" + ASIN
        client = FakeClient({url: [acquisition.Response(url, error="timeout") for _ in range(3)]})
        acquisition.crawl(self.db, self.config, "test", max_products=1, client=client)
        self.assertEqual(self.product()["attempts"], 3)
        acquisition.crawl(self.db, self.config, "again", max_products=1, retry_failed=True, client=FakeClient())
        self.assertEqual(self.product()["attempts"], 3)

    def test_challenge_stops_entire_origin_persistently(self):
        self.ready()
        self.allow_live()
        url = BASE + "/dp/" + ASIN
        client = FakeClient({url: [response(body=b"<html>Robot Check</html>")]})
        result = acquisition.crawl(self.db, self.config, "test", client=client)
        self.assertTrue(result["stopped_on_access_restriction"])
        self.assertEqual(self.product(OTHER)["status"], "not_collected")
        second = FakeClient()
        acquisition.crawl(self.db, self.config, "again", retry_failed=True, client=second)
        self.assertEqual(second.request_count, 0)

    def test_acquisition_interruption_resume_skips_success(self):
        self.ready()
        self.allow_live()
        first = FakeClient({BASE + "/dp/" + ASIN: [response()]})
        with self.assertRaises(KeyboardInterrupt):
            acquisition.crawl(self.db, self.config, "first", client=first, stop_after_products=1)
        second = FakeClient({BASE + "/dp/" + OTHER: [response(OTHER)]})
        acquisition.crawl(self.db, self.config, "second", client=second)
        self.assertEqual(self.product()["attempts"], 1)
        self.assertEqual(self.product(OTHER)["attempts"], 1)

    def test_pending_attempt_recovered_without_refund(self):
        self.ready()
        attempt_id = acquisition.begin_attempt(self.db, ASIN, "crashed", "live")
        acquisition.recover_attempts(self.db, self.config)
        self.assertEqual(self.product()["attempts"], 1)
        self.assertEqual(self.db.execute("SELECT status FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()[0], "interrupted")

    def test_redirect_preserved_and_foreign_origin_not_fetched(self):
        url = BASE + "/dp/" + ASIN
        redirect = acquisition.Response(url, 302, b"Redirect evidence", {"location": "https://foreign.test/dp/" + ASIN})
        client = FakeClient({url: [redirect]})
        policy = type("AllowedPolicy", (), {"decision": lambda self, url: (True, "test")})()
        result, chain, reason = acquisition.fetch_product(url, self.config, client, policy)
        self.assertEqual(result.error, "restricted_access")
        self.assertEqual(reason, "redirect_outside_approved_origin")
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(chain[0]["status"], 302)
        self.assertTrue(Path(chain[0]["snapshot_path"]).exists())

    def test_allowed_redirect_identity_checked(self):
        url, final = BASE + "/dp/" + ASIN, BASE + "/gp/product/" + ASIN
        client = FakeClient({url: [acquisition.Response(url, 301, b"", {"location": final})],
            final: [acquisition.Response(final, 200, html(), {"content-type": "text/html"})]})
        policy = type("AllowedPolicy", (), {"decision": lambda self, url: (True, "test")})()
        result, chain, reason = acquisition.fetch_product(url, self.config, client, policy)
        self.assertIsNone(reason)
        self.assertEqual(acquisition.classify(ASIN, result)["status"], "success")
        self.assertEqual(len(chain), 1)

    def test_retry_after_not_shortened(self):
        value = response(status=503)
        value.headers["retry-after"] = "120"
        self.assertIsNone(acquisition.retry_wait(value, 1, self.config))
        value.headers["retry-after"] = "10"
        self.assertGreaterEqual(acquisition.retry_wait(value, 1, self.config), 10)

    def test_rate_limit_and_response_size_in_transport(self):
        client = acquisition.HTTPClient(self.config, sleep=lambda value: waits.append(value), monotonic=lambda: 0)
        waits = []
        class Stream(io.BytesIO):
            code = 200
            headers = {"Content-Type": "text/html", "Set-Cookie": "must not persist"}
        class Opener:
            def open(self, req, timeout):
                return Stream(b"x" * 10)
        client.opener = Opener()
        client.config = dict(self.config, max_response_bytes=5)
        self.assertTrue(client.get(BASE).truncated)
        result = client.get(BASE)
        self.assertEqual(result.body, b"xxxxx")
        self.assertEqual(waits, [0.01])
        self.assertNotIn("set-cookie", result.headers)

    def snapshot_package(self, body=None):
        source = self.root / "snapshot.html"
        source.write_bytes(html() if body is None else body)
        item = {"asin": ASIN, "original_url": BASE + "/dp/" + ASIN,
            "final_url": BASE + "/dp/" + ASIN, "http_status": 200,
            "acquired_at": "2012-10-01T00:00:00+00:00", "snapshot_path": source.name,
            "sha256": acquisition.hash_file(source), "authorization_note": "SYNTHETIC TEST ONLY, not real authorization",
            "provider": "unittest synthetic generator", "content_type": "text/html; charset=utf-8"}
        manifest = self.root / "snapshots.jsonl"
        manifest.write_text(json.dumps(item) + "\n", encoding="utf-8")
        return manifest, item

    def test_snapshot_import_provenance_timestamp_and_idempotency(self):
        self.ready()
        manifest, item = self.snapshot_package()
        result = acquisition.import_snapshots(self.db, self.config, manifest, "test")
        self.assertEqual(result["imported"], 1)
        row = self.product()
        self.assertEqual(row["acquired_at"], item["acquired_at"])
        self.assertEqual(row["acquisition_mode"], "authorized_snapshot")
        again = acquisition.import_snapshots(self.db, self.config, manifest, "again")
        self.assertEqual(again, {"imported": 0, "cached": 1})
        self.assertEqual(self.product()["attempts"], 1)

    def test_failed_snapshot_import_also_idempotent(self):
        self.ready()
        manifest, _ = self.snapshot_package(body=b"<html>Robot Check</html>")
        acquisition.import_snapshots(self.db, self.config, manifest, "test")
        acquisition.import_snapshots(self.db, self.config, manifest, "again")
        self.assertEqual(self.product()["attempts"], 1)
        self.assertEqual(self.product()["status"], "restricted_access")

    def test_snapshot_checksum_and_permission_note_required(self):
        self.ready()
        manifest, item = self.snapshot_package()
        item["authorization_note"] = ""
        manifest.write_text(json.dumps(item), encoding="utf-8")
        with self.assertRaises(ValueError):
            acquisition.import_snapshots(self.db, self.config, manifest, "test")
        item["authorization_note"] = "synthetic test"
        item["sha256"] = "0" * 64
        manifest.write_text(json.dumps(item), encoding="utf-8")
        with self.assertRaises(ValueError):
            acquisition.import_snapshots(self.db, self.config, manifest, "test")

    def test_cached_success_tampering_rejected(self):
        self.ready()
        manifest, _ = self.snapshot_package()
        acquisition.import_snapshots(self.db, self.config, manifest, "test")
        Path(self.product()["snapshot_path"]).write_bytes(b"tamper")
        with self.assertRaises(ValueError):
            acquisition.validate_cached_successes(self.db)

    def test_verifier_detects_checkpoint_export_mismatch(self):
        self.ready()
        acquisition.export(self.db, self.config)
        self.assertEqual(verify(self.config)["status"], "passed")
        with self.db:
            self.db.execute("UPDATE products SET status='timeout' WHERE asin=?", (ASIN,))
        with self.assertRaises(ValueError):
            verify(self.config)

    def test_verifier_rejects_unsupported_restricted_status(self):
        self.ready()
        with self.db:
            self.db.execute("UPDATE products SET status='restricted_access' WHERE asin=?", (ASIN,))
        acquisition.export(self.db, self.config)
        with self.assertRaises(ValueError):
            verify(self.config)

    def test_verifier_checks_actual_source_lineage(self):
        self.ready()
        with self.db:
            self.db.execute("UPDATE products SET source_sha256='wrong-source' WHERE asin=?", (ASIN,))
        acquisition.export(self.db, self.config)
        with self.assertRaises(ValueError):
            verify(self.config)

    def test_manifest_success_denominator_is_all_products(self):
        self.ready()
        manifest, _ = self.snapshot_package()
        acquisition.import_snapshots(self.db, self.config, manifest, "test")
        stats = acquisition.export(self.db, self.config)
        self.assertEqual(stats["success_rate_all_products"], 0.5)
        self.assertEqual(stats["offline_success_rate_all_products"], 0.5)
        self.assertEqual(stats["live_success_rate_all_products"], 0)

    def test_unsafe_url_and_config_identity_rejected(self):
        for url in ("http://example.test", "https://user:secret@example.test"):
            with self.assertRaises(ValueError):
                acquisition.origin(url)
        path = self.root / "config.json"
        value = deepcopy(self.config)
        value["robots_agent"] = "NewIdentityToAvoidBan"
        acquisition.save_json(path, value)
        with self.assertRaises(ValueError):
            acquisition.load_config(path)


if __name__ == "__main__":
    unittest.main()
