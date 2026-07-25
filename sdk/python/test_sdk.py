"""
Tests for the Nexora-RS Python SDK.
Run with: python -m pytest test_sdk.py
"""

import json
import unittest
from unittest.mock import patch, MagicMock
from nexora_rs import NexoraClient, NexoraError


class TestNexoraClient(unittest.TestCase):
    def setUp(self):
        self.client = NexoraClient("http://localhost:8080")

    def test_hex_id(self):
        """Test that node IDs are correctly hex-encoded."""
        hex_id = self.client._hex_id("test-node")
        self.assertEqual(hex_id, "746573742d6e6f6465")

    @patch("urllib.request.urlopen")
    def test_set_property(self, mock_urlopen):
        """Test setting a property."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"ok": True}).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = self.client.set_property("node1", "speed", 80)
        self.assertTrue(result["ok"])

    @patch("urllib.request.urlopen")
    def test_get_property(self, mock_urlopen):
        """Test getting a property."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"value": 80}).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        value = self.client.get_property("node1", "speed")
        self.assertEqual(value, 80)

    @patch("urllib.request.urlopen")
    def test_cypher(self, mock_urlopen):
        """Test Cypher query execution."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "columns": ["n"],
            "rows": [["node1"], ["node2"]],
        }).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = self.client.cypher("MATCH (n) RETURN n LIMIT 2")
        self.assertEqual(len(result["columns"]), 1)
        self.assertEqual(len(result["rows"]), 2)

    @patch("urllib.request.urlopen")
    def test_health(self, mock_urlopen):
        """Test health check."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({
            "status": "healthy",
            "active_nodes": 10,
            "shards": 4,
        }).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        health = self.client.health()
        self.assertEqual(health["status"], "healthy")
        self.assertEqual(health["active_nodes"], 10)

    @patch("urllib.request.urlopen")
    def test_register_standing_query(self, mock_urlopen):
        """Test standing query registration."""
        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"id": "sq-123"}).encode()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = self.client.register_standing_query(
            "fast_cars", "speed", "GreaterThan", 100
        )
        self.assertEqual(result["id"], "sq-123")

    @patch("urllib.request.urlopen")
    def test_connection_error(self, mock_urlopen):
        """Test error handling."""
        import urllib.error
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")

        with self.assertRaises(NexoraError):
            self.client.health()


if __name__ == "__main__":
    unittest.main()
