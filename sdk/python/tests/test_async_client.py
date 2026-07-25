"""
Tests for AsyncNexoraClient and streaming subscriptions.

Run with: pytest tests/test_async_client.py -v
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import websockets

from nexora_rs import (
    AsyncNexoraClient,
    CypherStreamSubscription,
)
from nexora_rs.streaming import (
    CypherStreamSubscription as StreamingCypher,
    StandingQuerySubscription,
)
from nexora_rs.exceptions import NexoraError, QueryError


# ===========================================================================
# Helpers — mock aiohttp responses (reused pattern from test_client.py)
# ===========================================================================

def make_mock_response(status: int, body: dict[str, Any]):
    """Create a mock async context manager for an aiohttp response."""
    resp = AsyncMock()
    resp.status = status
    resp.text = AsyncMock(return_value=json.dumps(body))

    class _Ctx:
        async def __aenter__(self):
            return resp

        async def __aexit__(self, *args):
            return None

    return _Ctx()


@pytest.fixture
def async_client():
    """Provide an AsyncNexoraClient with a mocked session."""
    client = AsyncNexoraClient("http://localhost:8080")
    client._session = MagicMock()
    client._session.closed = False
    client._session.request = MagicMock()
    return client


# ===========================================================================
# AsyncNexoraClient context manager
# ===========================================================================

class TestAsyncClientContextManager:
    @pytest.mark.asyncio
    async def test_context_manager_creates_and_closes_session(self):
        """Test that __aenter__ creates a session and __aexit__ closes it."""
        client = AsyncNexoraClient("http://localhost:8080")

        with patch("nexora_rs.async_client.aiohttp.ClientSession") as mock_cls:
            mock_session = MagicMock()
            mock_session.closed = False
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with client as c:
                assert c is client

            mock_session.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager_with_api_key(self):
        """Test that API key is passed as Bearer token."""
        client = AsyncNexoraClient("http://localhost:8080", api_key="secret-token")

        with patch("nexora_rs.async_client.aiohttp.ClientSession") as mock_cls:
            mock_session = MagicMock()
            mock_session.closed = False
            mock_session.close = AsyncMock()
            mock_cls.return_value = mock_session

            async with client:
                pass

            call_kwargs = mock_cls.call_args.kwargs
            assert "Authorization" in call_kwargs["headers"]
            assert call_kwargs["headers"]["Authorization"] == "Bearer secret-token"


# ===========================================================================
# Node CRUD operations
# ===========================================================================

class TestAsyncNodeCRUD:
    @pytest.mark.asyncio
    async def test_create_node(self, async_client):
        """Test creating a node with labels and properties."""
        async_client._session.request.return_value = make_mock_response(
            200, {"status": "ok"}
        )

        node_id = await async_client.create_node(
            labels=["Person"],
            properties={"id": "alice", "name": "Alice", "age": 30},
        )

        assert node_id == "alice"
        # 3 set_property calls: labels, name, age
        assert async_client._session.request.call_count == 3

    @pytest.mark.asyncio
    async def test_get_property(self, async_client):
        """Test getting a property value."""
        async_client._session.request.return_value = make_mock_response(
            200, {"node_id": "616c696365", "key": "name", "value": "Alice"}
        )

        val = await async_client.get_property("alice", "name")
        assert val == "Alice"

    @pytest.mark.asyncio
    async def test_get_property_not_found(self, async_client):
        """Test getting a non-existent property returns None."""
        async_client._session.request.return_value = make_mock_response(
            200, {"not_found": True, "value": None}
        )

        val = await async_client.get_property("alice", "missing")
        assert val is None

    @pytest.mark.asyncio
    async def test_set_property(self, async_client):
        """Test setting a property."""
        async_client._session.request.return_value = make_mock_response(
            200, {"status": "ok"}
        )

        await async_client.set_property("alice", "age", 25)

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "PUT"
        assert "616c696365" in call_args.args[1]
        assert "age" in call_args.args[1]
        assert call_args.kwargs["json"] == {"value": 25}

    @pytest.mark.asyncio
    async def test_delete_node(self, async_client):
        """Test deleting a node via Cypher."""
        async_client._session.request.return_value = make_mock_response(
            200, {"columns": [], "rows": [], "error": None}
        )

        await async_client.delete_node("alice")

        call_args = async_client._session.request.call_args
        body = call_args.kwargs.get("json")
        assert "616c696365" in body["query"]
        assert "DETACH DELETE" in body["query"]

    @pytest.mark.asyncio
    async def test_get_all_properties(self, async_client):
        """Test getting all properties of a node."""
        async_client._session.request.return_value = make_mock_response(
            200,
            {"properties": {"name": "Alice", "age": 30, "city": "NYC"}},
        )

        props = await async_client.get_all_properties("alice")
        assert props["name"] == "Alice"
        assert props["age"] == 30
        assert props["city"] == "NYC"

    @pytest.mark.asyncio
    async def test_get_all_properties_empty(self, async_client):
        """Test getting properties for a node with no properties."""
        async_client._session.request.return_value = make_mock_response(
            200, {"properties": {}}
        )

        props = await async_client.get_all_properties("alice")
        assert props == {}


# ===========================================================================
# Edge operations
# ===========================================================================

class TestAsyncEdgeOps:
    @pytest.mark.asyncio
    async def test_add_edge(self, async_client):
        """Test adding an edge."""
        async_client._session.request.return_value = make_mock_response(
            200, {"status": "ok"}
        )

        await async_client.add_edge("alice", "bob", "KNOWS")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        body = call_args.kwargs.get("json")
        assert body["edge_type"] == "KNOWS"
        assert body["target"] == "626f62"
        assert body["direction"] == "out"

    @pytest.mark.asyncio
    async def test_get_edges(self, async_client):
        """Test getting edges."""
        async_client._session.request.return_value = make_mock_response(
            200,
            {"edges": [
                {"edge_type": "KNOWS", "direction": "out", "other": "626f62"},
                {"edge_type": "LIKES", "direction": "in", "other": "6361726c"},
            ]},
        )

        edges = await async_client.get_edges("alice")
        assert len(edges) == 2

    @pytest.mark.asyncio
    async def test_remove_edge(self, async_client):
        """Test removing an edge via remove_edge (alias)."""
        async_client._session.request.return_value = make_mock_response(
            200, {"columns": [], "rows": [], "error": None}
        )

        await async_client.remove_edge("alice", "KNOWS", "bob")

        call_args = async_client._session.request.call_args
        body = call_args.kwargs.get("json")
        assert "DELETE r" in body["query"]
        assert "KNOWS" in body["query"]


# ===========================================================================
# Bulk operations
# ===========================================================================

class TestAsyncBulkOps:
    @pytest.mark.asyncio
    async def test_bulk_create(self, async_client):
        """Test bulk_create creates multiple nodes."""
        async_client._session.request.return_value = make_mock_response(
            200, {"status": "ok"}
        )

        ids = await async_client.bulk_create([
            {"labels": ["Person"], "properties": {"id": "alice", "name": "Alice"}},
            {"labels": ["Person"], "properties": {"id": "bob", "name": "Bob"}},
        ])

        assert ids == ["alice", "bob"]
        # Each create_node: labels + name = 2 calls each, 4 total
        assert async_client._session.request.call_count == 4

    @pytest.mark.asyncio
    async def test_bulk_create_edges(self, async_client):
        """Test bulk_create_edges creates multiple edges."""
        async_client._session.request.return_value = make_mock_response(
            200, {"status": "ok"}
        )

        await async_client.bulk_create_edges([
            {"from_id": "alice", "to_id": "bob", "edge_type": "KNOWS"},
            {"from_id": "bob", "to_id": "carl", "edge_type": "LIKES"},
        ])

        assert async_client._session.request.call_count == 2


# ===========================================================================
# Query operations
# ===========================================================================

class TestAsyncQueryOps:
    @pytest.mark.asyncio
    async def test_query_returns_rows(self, async_client):
        """Test query returns rows as dicts."""
        async_client._session.request.return_value = make_mock_response(
            200,
            {"columns": ["name", "age"], "rows": [["Alice", 30], ["Bob", 25]], "error": None},
        )

        results = await async_client.query("MATCH (n:Person) RETURN n.name, n.age")
        assert len(results) == 2
        assert results[0] == {"name": "Alice", "age": 30}
        assert results[1] == {"name": "Bob", "age": 25}

    @pytest.mark.asyncio
    async def test_query_empty(self, async_client):
        """Test query with no results."""
        async_client._session.request.return_value = make_mock_response(
            200, {"columns": [], "rows": [], "error": None}
        )

        results = await async_client.query("MATCH (n) RETURN n")
        assert results == []

    @pytest.mark.asyncio
    async def test_query_with_params(self, async_client):
        """Test query with parameter substitution."""
        async_client._session.request.return_value = make_mock_response(
            200, {"columns": ["n"], "rows": [["alice"]], "error": None}
        )

        await async_client.query(
            "MATCH (n) WHERE n.name = $name RETURN n",
            {"name": "Alice"},
        )

        call_args = async_client._session.request.call_args
        body = call_args.kwargs.get("json")
        assert '"Alice"' in body["query"]

    @pytest.mark.asyncio
    async def test_query_error_raises(self, async_client):
        """Test that query errors raise QueryError."""
        async_client._session.request.return_value = make_mock_response(
            400, {"error": "syntax error", "columns": [], "rows": []}
        )

        with pytest.raises(QueryError):
            await async_client.query("INVALID CYPHER")

    @pytest.mark.asyncio
    async def test_query_one(self, async_client):
        """Test query_one returns first row."""
        async_client._session.request.return_value = make_mock_response(
            200,
            {"columns": ["name"], "rows": [["Alice"], ["Bob"]], "error": None},
        )

        row = await async_client.query_one("MATCH (n) RETURN n.name LIMIT 2")
        assert row == {"name": "Alice"}

    @pytest.mark.asyncio
    async def test_query_one_empty(self, async_client):
        """Test query_one returns None when no results."""
        async_client._session.request.return_value = make_mock_response(
            200, {"columns": [], "rows": [], "error": None}
        )

        row = await async_client.query_one("MATCH (n) RETURN n")
        assert row is None


# ===========================================================================
# Health & Stats
# ===========================================================================

class TestAsyncHealthStats:
    @pytest.mark.asyncio
    async def test_health(self, async_client):
        """Test health check."""
        async_client._session.request.return_value = make_mock_response(
            200,
            {"status": "healthy", "active_nodes": 42, "shards": 256},
        )

        h = await async_client.health()
        assert h["status"] == "healthy"
        assert h["active_nodes"] == 42

    @pytest.mark.asyncio
    async def test_stats(self, async_client):
        """Test stats endpoint."""
        async_client._session.request.return_value = make_mock_response(
            200,
            {"version": "0.2.0", "num_shards": 256, "max_nodes_per_shard": 10000},
        )

        s = await async_client.stats()
        assert s["version"] == "0.2.0"
        assert s["num_shards"] == 256


# ===========================================================================
# StandingQuerySubscription (streaming.py) tests
# ===========================================================================

class TestStandingQuerySubscription:
    @pytest.mark.asyncio
    async def test_start_receives_messages(self):
        """Test that start() calls callback for each message."""
        messages = [
            json.dumps({"type": "SqMatch", "sq_id": "sq-1", "match_count": 3}),
            json.dumps({"type": "SqMatch", "sq_id": "sq-1", "match_count": 5}),
        ]

        mock_ws = AsyncMock()
        mock_ws.close = AsyncMock()

        async def mock_async_iter():
            for msg in messages:
                yield msg

        mock_ws.__aiter__ = lambda self: mock_async_iter()

        received: list[dict] = []

        async def on_result(data):
            received.append(data)

        sub = StandingQuerySubscription("ws://localhost:8080", "sq-1")

        with patch("nexora_rs.streaming.connect") as mock_connect:
            mock_ctx = AsyncMock()
            mock_ctx.__aenter__ = AsyncMock(return_value=mock_ws)
            mock_ctx.__aexit__ = AsyncMock(return_value=None)
            mock_connect.return_value = mock_ctx

            await sub.start(on_result)

        assert len(received) == 2
        assert received[0]["match_count"] == 3
        assert received[1]["match_count"] == 5

    @pytest.mark.asyncio
    async def test_stop_sets_running_false(self):
        """Test that stop() sets running to False."""
        sub = StandingQuerySubscription("ws://localhost:8080", "sq-1")
        sub._running = True

        await sub.stop()

        assert sub._running is False

    @pytest.mark.asyncio
    async def test_async_iterator(self):
        """Test async iteration over results."""
        messages = [
            json.dumps({"type": "SqMatch", "match_count": 1}),
            json.dumps({"type": "SqMatch", "match_count": 2}),
        ]

        mock_ws = AsyncMock()
        mock_ws.close = AsyncMock()

        async def mock_async_iter():
            for msg in messages:
                yield msg

        mock_ws.__aiter__ = lambda self: mock_async_iter()

        sub = StandingQuerySubscription("ws://localhost:8080", "sq-1")

        with patch("nexora_rs.streaming.connect") as mock_connect:
            mock_ctx = AsyncMock()
            mock_ctx.__aenter__ = AsyncMock(return_value=mock_ws)
            mock_ctx.__aexit__ = AsyncMock(return_value=None)
            mock_connect.return_value = mock_ctx

            results = []
            async for data in sub:
                results.append(data)

        assert len(results) == 2
        assert results[0]["match_count"] == 1

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test use as async context manager."""
        sub = StandingQuerySubscription("ws://localhost:8080", "sq-1")
        async with sub as s:
            assert s is sub

    @pytest.mark.asyncio
    async def test_is_running_property(self):
        """Test is_running property."""
        sub = StandingQuerySubscription("ws://localhost:8080", "sq-1")
        assert sub.is_running is False

        sub._running = True
        assert sub.is_running is True


# ===========================================================================
# CypherStreamSubscription tests
# ===========================================================================

class TestCypherStreamSubscription:
    @pytest.mark.asyncio
    async def test_start_sends_query_and_receives(self):
        """Test that start() sends the query and receives results."""
        messages = [
            json.dumps({"type": "TabularResults", "queryId": "q", "columns": ["n"], "results": [["alice"]]}),
            json.dumps({"type": "QueryFinished", "queryId": "q"}),
        ]

        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock()
        mock_ws.close = AsyncMock()

        async def mock_async_iter():
            for msg in messages:
                yield msg

        mock_ws.__aiter__ = lambda self: mock_async_iter()

        received: list[dict] = []

        async def on_result(data):
            received.append(data)

        sub = CypherStreamSubscription("ws://localhost:8080")

        with patch("nexora_rs.streaming.connect") as mock_connect:
            mock_ctx = AsyncMock()
            mock_ctx.__aenter__ = AsyncMock(return_value=mock_ws)
            mock_ctx.__aexit__ = AsyncMock(return_value=None)
            mock_connect.return_value = mock_ctx

            await sub.start("MATCH (n) RETURN n LIMIT 1", on_result)

        assert len(received) == 2
        assert received[0]["type"] == "TabularResults"
        assert received[1]["type"] == "QueryFinished"
        # Verify query was sent
        mock_ws.send.assert_called_once()
        sent = json.loads(mock_ws.send.call_args.args[0])
        assert "MATCH" in sent["query"]

    @pytest.mark.asyncio
    async def test_stream_async_iterator(self):
        """Test stream() as async iterator."""
        messages = [
            json.dumps({"type": "TabularResults", "results": [["a"]]}),
            json.dumps({"type": "QueryFinished"}),
        ]

        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock()
        mock_ws.close = AsyncMock()

        async def mock_async_iter():
            for msg in messages:
                yield msg

        mock_ws.__aiter__ = lambda self: mock_async_iter()

        sub = CypherStreamSubscription("ws://localhost:8080")

        with patch("nexora_rs.streaming.connect") as mock_connect:
            mock_ctx = AsyncMock()
            mock_ctx.__aenter__ = AsyncMock(return_value=mock_ws)
            mock_ctx.__aexit__ = AsyncMock(return_value=None)
            mock_connect.return_value = mock_ctx

            results = []
            async for data in sub.stream("MATCH (n) RETURN n"):
                results.append(data)

        assert len(results) == 2
        assert results[1]["type"] == "QueryFinished"

    @pytest.mark.asyncio
    async def test_stop(self):
        """Test stop() closes the connection."""
        sub = CypherStreamSubscription("ws://localhost:8080")
        sub._running = True

        mock_ws = AsyncMock()
        mock_ws.close = AsyncMock()
        sub._ws = mock_ws

        await sub.stop()

        assert sub._running is False
        mock_ws.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test use as async context manager."""
        sub = CypherStreamSubscription("ws://localhost:8080")
        async with sub as s:
            assert s is sub

    @pytest.mark.asyncio
    async def test_aiter_raises_typeerror(self):
        """Test that __aiter__ raises TypeError directing to stream()."""
        sub = CypherStreamSubscription("ws://localhost:8080")
        with pytest.raises(TypeError, match="stream"):
            async for _ in sub:
                pass


# ===========================================================================
# Integration tests (skipped if server not running)
# ===========================================================================

@pytest.mark.asyncio
@pytest.mark.skipif(
    True,  # Set to False to run integration tests against a live server
    reason="Integration tests require a running nexora server",
)
class TestIntegration:
    async def test_integration_crud(self):
        """Integration test: full CRUD cycle against live server."""
        async with AsyncNexoraClient("http://localhost:8080") as client:
            # Create
            await client.set_property("test-node", "name", "TestNode")
            await client.set_property("test-node", "age", 42)

            # Read
            name = await client.get_property("test-node", "name")
            assert name == "TestNode"

            # Query
            results = await client.query("MATCH (n) WHERE n.name = 'TestNode' RETURN n.age")
            assert len(results) >= 1

            # Delete
            await client.delete_node("test-node")

    async def test_integration_bulk(self):
        """Integration test: bulk create."""
        async with AsyncNexoraClient("http://localhost:8080") as client:
            ids = await client.bulk_create([
                {"labels": ["Test"], "properties": {"id": "bulk-1", "val": 1}},
                {"labels": ["Test"], "properties": {"id": "bulk-2", "val": 2}},
                {"labels": ["Test"], "properties": {"id": "bulk-3", "val": 3}},
            ])
            assert len(ids) == 3

            results = await client.query("MATCH (n:Test) RETURN n.val ORDER BY n.val")
            assert len(results) == 3
