"""Synthetic raw-failure fixture; never a real successful acquisition."""
import base64
from copy import deepcopy
import hashlib
from pathlib import Path
import sys
import unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'src/quality'))
from offline_acceptance import verify_policy_failure


class OfflinePolicyTests(unittest.TestCase):
    def setUp(self):
        raw=b'Synthetic robots unavailable';sha=hashlib.sha256(raw).hexdigest()
        self.value=dict(raw_response_base64=base64.b64encode(raw).decode(),raw_response_sha256=sha,
            original_policy=dict(policy=dict(http_status=503,rules_usable=False,content_hash=sha)),
            product_statuses=[dict(status='not_collected',attempts=0,http_status=None,acquired_at=None)])
    def test_original_failure_and_unattempted_products_replay(self):
        self.assertEqual(verify_policy_failure(self.value)['product_attempts'],0)
    def test_tampered_failure_rejected(self):
        self.value['raw_response_base64']=base64.b64encode(b'changed').decode()
        with self.assertRaises(ValueError):verify_policy_failure(self.value)
    def test_failure_cannot_be_claimed_as_product_success(self):
        self.value['product_statuses'][0].update(status='success',attempts=1,http_status=200)
        with self.assertRaises(ValueError):verify_policy_failure(self.value)
