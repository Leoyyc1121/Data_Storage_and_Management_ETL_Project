"""Synthetic cached-source tests; never historical download evidence."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/extract'))
from acquire_imdb import replay_registered_archives,BASE,hash_file


class OfflineArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.name='title.basics.tsv.gz';self.path=self.root/self.name
        self.path.write_bytes(b'synthetic bytes, not a real IMDb archive')
        self.record=self.root/(self.name+'.manifest.json')
        self.record.write_text(json.dumps(dict(url=BASE+self.name,bytes=self.path.stat().st_size,sha256=hash_file(self.path),downloaded_at='2026-10-01T00:00:00+00:00')),encoding='utf-8')
        self.config=dict(raw_dir=str(self.root),report_dir=str(self.root/'reports'),files=[self.name])
    def test_verified_cache_never_constructs_http_client(self):
        with patch('acquire_imdb.HTTPClient',side_effect=AssertionError('Network forbidden')):
            result=replay_registered_archives(self.config)
        self.assertEqual(result[0]['downloaded_at'],'2026-10-01T00:00:00+00:00')
        report=json.loads((self.root/'reports/acquisition.json').read_text())
        self.assertEqual(report['actual_network_requests'],0)
    def test_changed_cache_is_rejected(self):
        self.path.write_bytes(b'changed')
        with self.assertRaises(ValueError):replay_registered_archives(self.config)
    def test_missing_registration_is_not_invented(self):
        self.record.unlink()
        with self.assertRaises(ValueError):replay_registered_archives(self.config)
