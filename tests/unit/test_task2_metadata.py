"""Synthetic public-archive tests; no test data enters real product statistics."""
from contextlib import closing
import gzip
import hashlib
import io
import json
from pathlib import Path
import ssl
import unittest
from unittest.mock import Mock, patch

import test_task2 as fixtures
import acquire_products as acquisition
import acquire_product_metadata as metadata

ASIN, OTHER = fixtures.ASIN, fixtures.OTHER


class MetadataTests(unittest.TestCase):
    def setUp(self):
        fixtures.Task2Tests.setUp(self)
        fixtures.Task2Tests.ready(self)
        self.spec = metadata.read_source_spec(acquisition.ROOT / 'config/task2.metadata.example.json')
        self.spec.update(source_id='synthetic_metadata', citation='Synthetic test fixture only')
        self.spec.pop('publisher_reference', None)
        self.lines = [json.dumps({'asin': ASIN, 'title': 'Synthetic title  你好'}, ensure_ascii=False).encode()+b'\n',
            json.dumps({'asin': OTHER, 'title': 'Synthetic second'}).encode()+b'\n',
            b'{"asin":"B000000003"}\n', b'not json\n', b'{"title":"Missing identifier"}\n',
            json.dumps({'asin': ASIN, 'title': 'Second candidate, do not silently merge'}).encode()+b'\n',
            b'{"asin":null}\n', b'\n']
        self.archive = self.root / 'source.json.gz'
        self.archive.write_bytes(gzip.compress(b''.join(self.lines), mtime=0))
        self.publisher = self.root / 'publisher.json'
        self.publisher.write_text('{"source":"SYNTHETIC UNIT TEST"}', encoding='utf-8')
        self.registration = dict(source_id='synthetic_metadata', archive_path=str(self.archive),
            archive_sha256=acquisition.hash_file(self.archive), downloaded_at='2026-10-15T00:00:00+00:00',
            publisher_snapshot=str(self.publisher), publisher_snapshot_sha256=acquisition.hash_file(self.publisher))
        self.fingerprint = acquisition.meta(self.db, 'source_fingerprint')
        self.metadata_db = metadata.open_metadata_db(self.config)
        self.addCleanup(self.metadata_db.close)

    def ingest(self, **kwargs):
        return metadata.import_archive(self.metadata_db, self.registration, self.spec, self.fingerprint, **kwargs)

    def test_complete_archive_counts_exact_membership_and_raw_values(self):
        state = self.ingest()
        self.assertEqual((state['records_read'], state['valid_records'], state['invalid_records']), (8, 4, 4))
        self.assertEqual((state['matched_records'], state['unmatched_records']), (3, 1))
        first = self.metadata_db.execute('SELECT raw_record FROM records ORDER BY record_sequence').fetchone()[0]
        self.assertEqual(first, self.lines[0])
        self.assertTrue(state['complete'])

    def test_duplicate_asin_candidates_are_preserved_not_merged(self):
        self.ingest()
        self.assertEqual(self.metadata_db.execute('SELECT COUNT(*) FROM records WHERE asin=?', (ASIN,)).fetchone()[0], 2)

    def test_conflicting_inner_asin_is_preserved_and_flagged(self):
        self.lines[0] = json.dumps({'asin': ASIN, 'details': {'ASIN: ': OTHER}}).encode() + b'\n'
        self.archive.write_bytes(gzip.compress(b''.join(self.lines), mtime=0))
        self.registration['archive_sha256'] = acquisition.hash_file(self.archive)
        self.ingest()
        stats = metadata.export_metadata(self.metadata_db, self.config)
        self.assertEqual(stats['identity_conflict_records'], 1)
        self.assertEqual(stats['identity_conflict_asins'], 1)
        self.assertEqual(stats['asins_without_identity_conflicts'], 1)
        self.assertEqual(stats['matched_records'], 3)

    def test_identity_audit_is_idempotent(self):
        self.lines[0] = json.dumps({'asin': ASIN, 'details': {'ASIN': OTHER}}).encode() + b'\n'
        self.archive.write_bytes(gzip.compress(b''.join(self.lines), mtime=0))
        self.registration['archive_sha256'] = acquisition.hash_file(self.archive)
        self.ingest()
        metadata.audit_identities(self.metadata_db)
        metadata.audit_identities(self.metadata_db)
        self.assertEqual(self.metadata_db.execute('SELECT COUNT(*) FROM identity_issues').fetchone()[0], 1)

    def test_source_import_interruption_and_resume_exactly_once(self):
        with self.assertRaises(KeyboardInterrupt):
            self.ingest(stop_after_records=2)
        self.assertEqual(self.metadata_db.execute('SELECT COUNT(*) FROM records').fetchone()[0], 2)
        state = self.ingest()
        self.assertEqual(state['matched_records'], 3)
        self.assertEqual(self.metadata_db.execute('SELECT COUNT(*) FROM records').fetchone()[0], 3)

    def test_reimport_and_export_are_idempotent(self):
        self.ingest()
        before = metadata.export_metadata(self.metadata_db, self.config)
        self.ingest()
        after = metadata.export_metadata(self.metadata_db, self.config)
        self.assertEqual(before['output'], after['output'])
        self.assertEqual(after['matched_asins'], 2)
        self.assertEqual(after['matched_records'], 3)
        self.assertFalse(after['counts_as_successful_product_pages'])

    def test_all_source_archive_locators_verified(self):
        self.ingest()
        metadata.export_metadata(self.metadata_db, self.config)
        result = metadata.verify_metadata(self.config)
        self.assertEqual(result['all_matched_raw_locators_checked'], 3)

    def test_wrong_source_archive_rejected(self):
        self.archive.write_bytes(b'tampered')
        with self.assertRaises(ValueError):
            self.ingest()

    def test_wrong_target_version_rejected(self):
        self.ingest()
        with self.assertRaises(ValueError):
            metadata.import_archive(self.metadata_db, self.registration, self.spec, 'different-target')

    def test_tampered_locator_detected_even_after_reexport(self):
        self.ingest()
        with self.metadata_db:
            self.metadata_db.execute('UPDATE records SET byte_start=byte_start+1 WHERE record_sequence=1')
        metadata.export_metadata(self.metadata_db, self.config)
        with self.assertRaises(ValueError):
            metadata.verify_metadata(self.config)

    def test_source_size_limits_enforced(self):
        spec = dict(self.spec, max_record_bytes=5)
        with self.assertRaises(ValueError):
            metadata.import_archive(self.metadata_db, self.registration, spec, self.fingerprint)

    def test_tls_validation_remains_enabled(self):
        client = acquisition.HTTPClient(self.config)
        self.assertEqual(client.ssl_context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(client.ssl_context.check_hostname)

    def download_client(self, body, status=200, response_headers=None, publisher_link=True):
        class Stream(io.BytesIO):
            code = status
            headers = response_headers or {'Content-Length': str(len(body)), 'Content-Type': 'application/gzip', 'ETag': 'synthetic-etag'}
        client = acquisition.HTTPClient(self.config, sleep=lambda _: None)
        client.opener = Mock()
        client.opener.open.return_value = Stream(body)
        link = self.spec['download_url'] if publisher_link else 'https://example.test/not-this-archive.gz'
        client.get = Mock(return_value=acquisition.Response(self.spec['publisher_page'], 200,
            f'<html><a href="{link}">Synthetic archive link</a></html>'.encode()))
        return client

    def test_official_link_download_and_cache_checksum(self):
        body = self.archive.read_bytes()
        client = self.download_client(body)
        with patch.object(metadata, 'source_policy', return_value={'synthetic_test': True}):
            result = metadata.download_source(self.config, self.spec, client)
            again = metadata.download_source(self.config, self.spec, client)
        self.assertEqual(result['archive_sha256'], hashlib.sha256(body).hexdigest())
        self.assertEqual(result, again)
        self.assertEqual(client.opener.open.call_count, 1)

    def test_archive_must_be_linked_by_publisher(self):
        client = self.download_client(self.archive.read_bytes(), publisher_link=False)
        with patch.object(metadata, 'source_policy', return_value={}):
            with self.assertRaises(ValueError):
                metadata.download_source(self.config, self.spec, client)
        self.assertEqual(client.opener.open.call_count, 0)

    def test_http_200_error_page_is_not_research_archive(self):
        client = self.download_client(b'<html>Robot Check</html>')
        with patch.object(metadata, 'source_policy', return_value={}):
            with self.assertRaises(ValueError):
                metadata.download_source(self.config, self.spec, client)

    def test_http_range_resume_validates_byte_range_and_etag(self):
        body = self.archive.read_bytes()
        root = Path(self.config['raw_dir']) / 'third_party' / self.spec['source_id']
        root.mkdir(parents=True)
        (root / (self.spec['archive_name'] + '.part')).write_bytes(body[:10])
        acquisition.save_json(root / 'download_transfer.json', {'etag': 'synthetic-etag'})
        client = self.download_client(body[10:], status=206, response_headers={
            'Content-Range': f'bytes 10-{len(body)-1}/{len(body)}', 'ETag': 'synthetic-etag'})
        with patch.object(metadata, 'source_policy', return_value={}):
            result = metadata.download_source(self.config, self.spec, client)
        self.assertEqual(result['archive_sha256'], hashlib.sha256(body).hexdigest())
        self.assertEqual(client.opener.open.call_args.args[0].get_header('Range'), 'bytes=10-')


if __name__ == '__main__':
    unittest.main()
