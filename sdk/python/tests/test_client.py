"""
Comprehensive tests for the Nexora-RS Python SDK.

Run with: pytest tests/ -v
"""

from __future__ import annotations

import asyncio
import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from nexora_rs import (
    AsyncNexoraClient,
    NexoraClient,
    StandingQuerySubscription,
    NexoraError,
    QueryError,
    NodeNotFoundError,
    AuthenticationError,
    ConnectionError,
    TimeoutError,
)
from nexora_rs.types import Node, Edge, QueryResult


# ===========================================================================
# Helpers
# ===========================================================================

class MockAsyncIterator:
    """Async iterator that yields pre-defined messages for WebSocket mocking."""

    def __init__(self, messages: list[Any]) -> None:
        self._messages = list(messages)
        self._index = 0
        self.closed = False
        self.close = AsyncMock()

    def __aiter__(self) -> "MockAsyncIterator":
        return self

    async def __anext__(self) -> Any:
        if self._index >= len(self._messages):
            raise StopAsyncIteration
        msg = self._messages[self._index]
        self._index += 1
        return msg


def make_mock_response(status: int, body: dict[str, Any]):
    """Create a mock async context manager for an aiohttp response.

    aiohttp's session.request() returns a context manager (not a coroutine),
    so we return a plain object that supports __aenter__/__aexit__.
    """
    resp = AsyncMock()
    resp.status = status
    resp.text = AsyncMock(return_value=json.dumps(body))

    class _Ctx:
        async def __aenter__(self):
            return resp

        async def __aexit__(self, *args):
            return None

    return _Ctx()


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture
def async_client():
    """Provide an AsyncNexoraClient with a mocked session.

    We use MagicMock (not AsyncMock) for session.request so that calling it
    returns the mock context manager directly — matching aiohttp's real
    behaviour where session.request() returns a context manager, not a coroutine.
    """
    client = AsyncNexoraClient("http://localhost:8080")
    client._session = MagicMock()
    client._session.closed = False
    client._session.request = MagicMock()
    return client


@pytest.fixture
def sync_client():
    """Provide a NexoraClient."""
    return NexoraClient("http://localhost:8080")


# ===========================================================================
# Hex ID encoding
# ===========================================================================

class TestHexId:
    def test_hex_id_basic(self):
        """Test that node IDs are correctly hex-encoded."""
        assert AsyncNexoraClient._hex_id("test-node") == "746573742d6e6f6465"

    def test_hex_id_alice(self):
        assert AsyncNexoraClient._hex_id("alice") == "616c696365"

    def test_hex_id_empty(self):
        assert AsyncNexoraClient._hex_id("") == ""

    def test_hex_id_unicode(self):
        assert AsyncNexoraClient._hex_id("héllo") == "68c3a96c6c6f"


# ===========================================================================
# Parameter substitution
# ===========================================================================

class TestParamSubstitution:
    def test_no_params(self):
        assert AsyncNexoraClient._substitute_params("MATCH (n) RETURN n", None) == "MATCH (n) RETURN n"

    def test_string_param(self):
        result = AsyncNexoraClient._substitute_params(
            "MATCH (n) WHERE n.name = $name RETURN n",
            {"name": "Alice"},
        )
        assert '"Alice"' in result

    def test_int_param(self):
        result = AsyncNexoraClient._substitute_params(
            "MATCH (n) WHERE n.age > $age RETURN n",
            {"age": 30},
        )
        assert "30" in result

    def test_missing_param_preserved(self):
        result = AsyncNexoraClient._substitute_params(
            "MATCH (n) WHERE n.name = $name RETURN n",
            {},
        )
        assert "$name" in result


# ===========================================================================
# Async client: Cypher queries
# ===========================================================================

class TestAsyncQuery:
    @pytest.mark.asyncio
    async def test_query_success(self, async_client):
        """Test successful Cypher query returning rows."""
        async_client._session.request.return_value = make_mock_response(200, {
            "columns": ["n"],
            "rows": [["alice"], ["bob"]],
            "error": None,
        })

        results = await async_client.query("MATCH (n) RETURN n LIMIT 2")
        assert len(results) == 2
        assert results[0] == {"n": "alice"}
        assert results[1] == {"n": "bob"}

    @pytest.mark.asyncio
    async def test_query_empty(self, async_client):
        """Test query with no results."""
        async_client._session.request.return_value = make_mock_response(200, {
            "columns": [],
            "rows": [],
            "error": None,
        })

        results = await async_client.query("MATCH (n) RETURN n")
        assert results == []

    @pytest.mark.asyncio
    async def test_query_error(self, async_client):
        """Test query with server-side error."""
        async_client._session.request.return_value = make_mock_response(400, {
            "columns": [],
            "rows": [],
            "error": "syntax error",
        })

        with pytest.raises(QueryError):
            await async_client.query("INVALID CYPHER")

    @pytest.mark.asyncio
    async def test_query_one_returns_first(self, async_client):
        """Test query_one returns the first row."""
        async_client._session.request.return_value = make_mock_response(200, {
            "columns": ["name"],
            "rows": [["Alice"], ["Bob"]],
            "error": None,
        })

        row = await async_client.query_one("MATCH (n) RETURN n.name LIMIT 2")
        assert row == {"name": "Alice"}

    @pytest.mark.asyncio
    async def test_query_one_empty(self, async_client):
        """Test query_one returns None when no results."""
        async_client._session.request.return_value = make_mock_response(200, {
            "columns": [],
            "rows": [],
            "error": None,
        })

        row = await async_client.query_one("MATCH (n) RETURN n")
        assert row is None

    @pytest.mark.asyncio
    async def test_query_with_params(self, async_client):
        """Test query with parameter substitution."""
        async_client._session.request.return_value = make_mock_response(200, {
            "columns": ["name"],
            "rows": [["Alice"]],
            "error": None,
        })

        results = await async_client.query(
            "MATCH (n) WHERE n.name = $name RETURN n.name",
            {"name": "Alice"},
        )
        assert len(results) == 1
        # Verify the query was sent with substituted param
        call_args = async_client._session.request.call_args
        body = call_args.kwargs.get("json")
        assert body is not None
        assert '"Alice"' in body["query"]


# ===========================================================================
# Async client: Node operations
# ===========================================================================

class TestAsyncNodeOps:
    @pytest.mark.asyncio
    async def test_set_property(self, async_client):
        """Test setting a property."""
        async_client._session.request.return_value = make_mock_response(200, {"status": "ok"})

        await async_client.set_property("alice", "name", "Alice")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "PUT"
        assert "616c696365" in call_args.args[1]  # hex of "alice"
        assert "name" in call_args.args[1]
        assert call_args.kwargs.get("json") == {"value": "Alice"}

    @pytest.mark.asyncio
    async def test_get_property(self, async_client):
        """Test getting a property."""
        async_client._session.request.return_value = make_mock_response(200, {
            "node_id": "616c696365",
            "key": "name",
            "value": "Alice",
        })

        val = await async_client.get_property("alice", "name")
        assert val == "Alice"

    @pytest.mark.asyncio
    async def test_get_property_not_found(self, async_client):
        """Test getting a property that doesn't exist."""
        async_client._session.request.return_value = make_mock_response(200, {
            "node_id": "616c696365",
            "key": "missing",
            "value": None,
            "not_found": True,
        })

        val = await async_client.get_property("alice", "missing")
        assert val is None

    @pytest.mark.asyncio
    async def test_delete_node(self, async_client):
        """Test deleting a node via Cypher."""
        async_client._session.request.return_value = make_mock_response(200, {
            "columns": [],
            "rows": [],
            "error": None,
        })

        await async_client.delete_node("alice")

        call_args = async_client._session.request.call_args
        body = call_args.kwargs.get("json")
        assert body is not None
        assert "616c696365" in body["query"]
        assert "DETACH DELETE" in body["query"]

    @pytest.mark.asyncio
    async def test_create_node(self, async_client):
        """Test creating a node with labels and properties."""
        async_client._session.request.return_value = make_mock_response(200, {"status": "ok"})

        node_id = await async_client.create_node(
            labels=["Person"],
            properties={"id": "alice", "name": "Alice", "age": 30},
        )

        assert node_id == "alice"
        # Should have made 3 set_property calls: labels, name, age
        assert async_client._session.request.call_count == 3

    @pytest.mark.asyncio
    async def test_create_node_no_id(self, async_client):
        """Test that creating a node without an 'id' property raises an error."""
        with pytest.raises(NexoraError):
            await async_client.create_node(["Person"], {"name": "Alice"})


# ===========================================================================
# Async client: Edge operations
# ===========================================================================

class TestAsyncEdgeOps:
    @pytest.mark.asyncio
    async def test_add_edge(self, async_client):
        """Test adding an edge."""
        async_client._session.request.return_value = make_mock_response(200, {"status": "ok"})

        await async_client.add_edge("alice", "bob", "KNOWS")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        body = call_args.kwargs.get("json")
        assert body is not None
        assert body["edge_type"] == "KNOWS"
        assert body["target"] == "626f62"  # hex of "bob"
        assert body["direction"] == "out"

    @pytest.mark.asyncio
    async def test_add_edge_with_properties(self, async_client):
        """Test adding an edge with properties (uses Cypher)."""
        # First call: add edge (200), second call: set properties via Cypher (200)
        async_client._session.request.side_effect = [
            make_mock_response(200, {"status": "ok"}),
            make_mock_response(200, {"columns": [], "rows": [], "error": None}),
        ]

        await async_client.add_edge("alice", "bob", "KNOWS", {"since": 2020})

        assert async_client._session.request.call_count == 2

    @pytest.mark.asyncio
    async def test_get_edges_all(self, async_client):
        """Test getting all edges."""
        async_client._session.request.return_value = make_mock_response(200, {
            "edges": [
                {"edge_type": "KNOWS", "direction": "out", "other": "626f62"},
                {"edge_type": "LIKES", "direction": "in", "other": "6361726c"},
            ]
        })

        edges = await async_client.get_edges("alice")
        assert len(edges) == 2

    @pytest.mark.asyncio
    async def test_get_edges_out_only(self, async_client):
        """Test getting only outgoing edges."""
        async_client._session.request.return_value = make_mock_response(200, {
            "edges": [
                {"edge_type": "KNOWS", "direction": "out", "other": "626f62"},
                {"edge_type": "LIKES", "direction": "in", "other": "6361726c"},
            ]
        })

        edges = await async_client.get_edges("alice", direction="out")
        assert len(edges) == 1
        assert edges[0]["direction"] == "out"

    @pytest.mark.asyncio
    async def test_delete_edge(self, async_client):
        """Test deleting an edge via Cypher."""
        async_client._session.request.return_value = make_mock_response(200, {
            "columns": [], "rows": [], "error": None,
        })

        await async_client.delete_edge("alice", "bob", "KNOWS")

        call_args = async_client._session.request.call_args
        body = call_args.kwargs.get("json")
        assert body is not None
        assert "DELETE r" in body["query"]
        assert "KNOWS" in body["query"]


# ===========================================================================
# Async client: Batch operations
# ===========================================================================

class TestAsyncBatch:
    @pytest.mark.asyncio
    async def test_batch_query(self, async_client):
        """Test batch query execution."""
        async_client._session.request.side_effect = [
            make_mock_response(200, {"columns": ["n"], "rows": [["a"]], "error": None}),
            make_mock_response(200, {"columns": ["n"], "rows": [["b"], ["c"]], "error": None}),
        ]

        results = await async_client.batch_query([
            "MATCH (n) RETURN n LIMIT 1",
            "MATCH (n) RETURN n LIMIT 2",
        ])

        assert len(results) == 2
        assert len(results[0]) == 1
        assert len(results[1]) == 2

    @pytest.mark.asyncio
    async def test_batch_create_nodes(self, async_client):
        """Test batch node creation."""
        # Each create_node calls set_property for labels + each property (minus id)
        # Node 1: "alice" -> labels, name (2 calls)
        # Node 2: "bob" -> labels, name (2 calls)
        async_client._session.request.return_value = make_mock_response(200, {"status": "ok"})

        ids = await async_client.batch_create_nodes([
            {"labels": ["Person"], "properties": {"id": "alice", "name": "Alice"}},
            {"labels": ["Person"], "properties": {"id": "bob", "name": "Bob"}},
        ])

        assert ids == ["alice", "bob"]
        assert async_client._session.request.call_count == 4


# ===========================================================================
# Async client: Standing Queries
# ===========================================================================

class TestAsyncStandingQuery:
    @pytest.mark.asyncio
    async def test_create_standing_query(self, async_client):
        """Test creating a standing query."""
        async_client._session.request.return_value = make_mock_response(201, {
            "id": "sq-123", "name": "fast_cars",
        })

        sq_id = await async_client.create_standing_query(
            {
                "type": "PropertyFilter",
                "key": "speed",
                "condition": {"type": "GreaterThan", "value": 100},
            },
            name="fast_cars",
        )

        assert sq_id == "sq-123"
        call_args = async_client._session.request.call_args
        body = call_args.kwargs.get("json")
        assert body is not None
        assert body["name"] == "fast_cars"
        assert body["pattern"]["type"] == "PropertyFilter"

    @pytest.mark.asyncio
    async def test_list_standing_queries(self, async_client):
        """Test listing standing queries."""
        async_client._session.request.return_value = make_mock_response(200, {
            "standing_queries": [
                {"id": "sq-1", "name": "fast_cars", "match_count": 5},
                {"id": "sq-2", "name": "slow_cars", "match_count": 0},
            ]
        })

        queries = await async_client.list_standing_queries()
        assert len(queries) == 2
        assert queries[0]["id"] == "sq-1"
        assert queries[0]["match_count"] == 5

    @pytest.mark.asyncio
    async def test_delete_standing_query(self, async_client):
        """Test deleting a standing query."""
        async_client._session.request.return_value = make_mock_response(200, {"status": "deleted"})

        await async_client.delete_standing_query("sq-123")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "DELETE"
        assert "sq-123" in call_args.args[1]


# ===========================================================================
# Async client: Health & Stats
# ===========================================================================

class TestAsyncHealth:
    @pytest.mark.asyncio
    async def test_health(self, async_client):
        """Test health check."""
        async_client._session.request.return_value = make_mock_response(200, {
            "status": "healthy",
            "active_nodes": 10,
            "shards": 4,
        })

        h = await async_client.health()
        assert h["status"] == "healthy"
        assert h["active_nodes"] == 10

    @pytest.mark.asyncio
    async def test_stats(self, async_client):
        """Test stats endpoint."""
        async_client._session.request.return_value = make_mock_response(200, {
            "version": "0.1.0",
            "num_shards": 256,
            "max_nodes_per_shard": 10000,
        })

        s = await async_client.stats()
        assert s["version"] == "0.1.0"
        assert s["num_shards"] == 256


# ===========================================================================
# Async client: Error handling
# ===========================================================================

class TestAsyncErrors:
    @pytest.mark.asyncio
    async def test_auth_error(self, async_client):
        """Test authentication error (401)."""
        async_client._session.request.return_value = make_mock_response(401, {"error": "unauthorized"})

        with pytest.raises(AuthenticationError):
            await async_client.health()

    @pytest.mark.asyncio
    async def test_not_found_error(self, async_client):
        """Test node not found error (404)."""
        async_client._session.request.return_value = make_mock_response(404, {"error": "not found"})

        with pytest.raises(NodeNotFoundError):
            await async_client._request("GET", "/api/v2/standing-query/bad-id")

    @pytest.mark.asyncio
    async def test_query_error_400(self, async_client):
        """Test query error (400)."""
        async_client._session.request.return_value = make_mock_response(400, {"error": "bad query"})

        with pytest.raises(QueryError):
            await async_client._request("POST", "/api/v2/query/cypher", {"query": "INVALID"})


# ===========================================================================
# Async client: Context manager
# ===========================================================================

class TestAsyncContextManager:
    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test async context manager usage."""
        client = AsyncNexoraClient("http://localhost:8080")
        # Mock the session creation
        with patch.object(aiohttp, "ClientSession") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.closed = False
            mock_session.request = MagicMock(return_value=make_mock_response(200, {"status": "healthy"}))
            mock_session.close = AsyncMock()
            mock_session_cls.return_value = mock_session

            async with client as c:
                assert c is client
                health = await c.health()
                assert health["status"] == "healthy"

            # Session should be closed after context exit
            mock_session.close.assert_called_once()


# ===========================================================================
# WebSocket subscription tests
# ===========================================================================

class TestWebSocketSubscription:
    @pytest.mark.asyncio
    async def test_subscribe_receives_messages(self):
        """Test that subscribe calls the callback for each message."""
        # Create mock WebSocket messages
        msg1 = MagicMock()
        msg1.type = aiohttp.WSMsgType.TEXT
        msg1.data = json.dumps({"type": "SqMatch", "sq_id": "sq-1", "match_count": 3})

        msg2 = MagicMock()
        msg2.type = aiohttp.WSMsgType.TEXT
        msg2.data = json.dumps({"type": "SqMatch", "sq_id": "sq-1", "match_count": 5})

        close_msg = MagicMock()
        close_msg.type = aiohttp.WSMsgType.CLOSED

        # Create mock WebSocket with async iterator
        mock_ws = MockAsyncIterator([msg1, msg2, close_msg])

        # Create mock session
        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)

        received: list[dict] = []

        async def on_result(data):
            received.append(data)

        sub = StandingQuerySubscription("ws://localhost:8080", "sq-1")
        with patch.object(aiohttp, "ClientSession", return_value=mock_session):
            await sub.subscribe(on_result)

        assert len(received) == 2
        assert received[0]["match_count"] == 3
        assert received[1]["match_count"] == 5

    @pytest.mark.asyncio
    async def test_subscribe_callback_error_continues(self):
        """Test that errors in the callback don't stop subscription."""
        msg1 = MagicMock()
        msg1.type = aiohttp.WSMsgType.TEXT
        msg1.data = json.dumps({"type": "SqMatch", "match_count": 1})

        msg2 = MagicMock()
        msg2.type = aiohttp.WSMsgType.TEXT
        msg2.data = json.dumps({"type": "SqMatch", "match_count": 2})

        close_msg = MagicMock()
        close_msg.type = aiohttp.WSMsgType.CLOSED

        mock_ws = MockAsyncIterator([msg1, msg2, close_msg])

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)

        received: list[int] = []

        async def on_result(data):
            received.append(data["match_count"])
            if len(received) == 1:
                raise ValueError("oops")

        sub = StandingQuerySubscription("ws://localhost:8080", "sq-1")
        with patch.object(aiohttp, "ClientSession", return_value=mock_session):
            await sub.subscribe(on_result)

        # Both messages should be processed despite the error on the first
        assert len(received) == 2

    @pytest.mark.asyncio
    async def test_unsubscribe(self):
        """Test that unsubscribe closes the WebSocket."""
        mock_ws = AsyncMock()
        mock_ws.closed = False

        mock_session = AsyncMock()
        mock_session.closed = False

        sub = StandingQuerySubscription("ws://localhost:8080", "sq-1")
        sub._ws = mock_ws
        sub._session = mock_session
        sub._running = True

        await sub.unsubscribe()

        assert sub._running is False
        mock_ws.close.assert_called_once()
        mock_session.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test WebSocket subscription as a context manager."""
        mock_ws = MockAsyncIterator([])

        mock_session = MagicMock()
        mock_session.closed = False
        mock_session.close = AsyncMock()
        mock_session.ws_connect = AsyncMock(return_value=mock_ws)

        sub = StandingQuerySubscription("ws://localhost:8080", "sq-1")

        with patch.object(aiohttp, "ClientSession", return_value=mock_session):
            async with sub as s:
                assert s is sub
                # Connection should be established
                assert sub._ws is not None

    @pytest.mark.asyncio
    async def test_is_running_property(self):
        """Test the is_running property."""
        sub = StandingQuerySubscription("ws://localhost:8080", "sq-1")
        assert sub.is_running is False

        sub._running = True
        assert sub.is_running is True


# ===========================================================================
# Sync client tests
# ===========================================================================

class TestSyncClient:
    def test_query(self, sync_client):
        """Test sync query."""
        with patch.object(sync_client._async, "query", new_callable=AsyncMock) as mock_q:
            mock_q.return_value = [{"n": "alice"}]
            results = sync_client.query("MATCH (n) RETURN n")
            assert len(results) == 1
            mock_q.assert_called_once()

    def test_set_property(self, sync_client):
        """Test sync set_property."""
        with patch.object(sync_client._async, "set_property", new_callable=AsyncMock) as mock_sp:
            sync_client.set_property("alice", "name", "Alice")
            mock_sp.assert_called_once_with("alice", "name", "Alice")

    def test_get_property(self, sync_client):
        """Test sync get_property."""
        with patch.object(sync_client._async, "get_property", new_callable=AsyncMock) as mock_gp:
            mock_gp.return_value = "Alice"
            val = sync_client.get_property("alice", "name")
            assert val == "Alice"

    def test_health(self, sync_client):
        """Test sync health."""
        with patch.object(sync_client._async, "health", new_callable=AsyncMock) as mock_h:
            mock_h.return_value = {"status": "healthy"}
            h = sync_client.health()
            assert h["status"] == "healthy"

    def test_context_manager(self):
        """Test sync context manager."""
        client = NexoraClient("http://localhost:8080")
        with patch.object(client._async, "health", new_callable=AsyncMock) as mock_h, \
             patch.object(client._async, "close", new_callable=AsyncMock) as mock_close:
            mock_h.return_value = {"status": "healthy"}
            with client as c:
                assert c is client
                c.health()  # Force loop creation
            mock_close.assert_called_once()

    def test_batch_query(self, sync_client):
        """Test sync batch_query."""
        with patch.object(sync_client._async, "batch_query", new_callable=AsyncMock) as mock_bq:
            mock_bq.return_value = [[{"n": "a"}], [{"n": "b"}]]
            results = sync_client.batch_query(["Q1", "Q2"])
            assert len(results) == 2

    def test_create_standing_query(self, sync_client):
        """Test sync create_standing_query."""
        with patch.object(sync_client._async, "create_standing_query", new_callable=AsyncMock) as mock_sq:
            mock_sq.return_value = "sq-123"
            sq_id = sync_client.create_standing_query(
                {"type": "PropertyFilter", "key": "speed",
                 "condition": {"type": "GreaterThan", "value": 100}},
                name="fast_cars",
            )
            assert sq_id == "sq-123"


# ===========================================================================
# Type definitions tests
# ===========================================================================

class TestTypes:
    def test_node_dataclass(self):
        """Test Node dataclass creation."""
        node = Node(id="alice", labels=["Person"], properties={"name": "Alice"})
        assert node.id == "alice"
        assert node.labels == ["Person"]
        assert node.properties["name"] == "Alice"

    def test_edge_dataclass(self):
        """Test Edge dataclass creation."""
        edge = Edge(from_id="alice", to_id="bob", edge_type="KNOWS")
        assert edge.from_id == "alice"
        assert edge.to_id == "bob"
        assert edge.edge_type == "KNOWS"
        assert edge.properties == {}

    def test_query_result_dataclass(self):
        """Test QueryResult dataclass."""
        qr = QueryResult(columns=["n"], rows=[{"n": "alice"}], duration_ms=1.5)
        assert qr.columns == ["n"]
        assert len(qr.rows) == 1
        assert qr.duration_ms == 1.5

    def test_node_defaults(self):
        """Test Node default values."""
        node = Node(id="x")
        assert node.labels == []
        assert node.properties == {}


# ===========================================================================
# Vector operations tests
# ===========================================================================

class TestAsyncVector:
    @pytest.mark.asyncio
    async def test_vector_index(self, async_client):
        """Test indexing a vector."""
        async_client._session.request.return_value = make_mock_response(200, {
            "status": "indexed", "qid": "616c696365", "index_size": 1,
        })

        result = await async_client.vector_index("alice", [0.1, 0.2, 0.3])

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        assert call_args.args[1] == "/api/v2/vector/index"
        body = call_args.kwargs.get("json")
        assert body["qid"] == "616c696365"
        assert body["vector"] == [0.1, 0.2, 0.3]
        assert result["status"] == "indexed"

    @pytest.mark.asyncio
    async def test_vector_search(self, async_client):
        """Test vector similarity search."""
        async_client._session.request.return_value = make_mock_response(200, {
            "query": [0.1, 0.2], "k": 5,
            "neighbors": [
                {"qid": "616c696365", "distance": 0.1},
                {"qid": "626f62", "distance": 0.5},
            ],
        })

        result = await async_client.vector_search([0.1, 0.2], k=5)

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        assert call_args.args[1] == "/api/v2/vector/search"
        body = call_args.kwargs.get("json")
        assert body["vector"] == [0.1, 0.2]
        assert body["k"] == 5
        assert len(result["neighbors"]) == 2

    @pytest.mark.asyncio
    async def test_vector_get(self, async_client):
        """Test getting a vector for a node."""
        async_client._session.request.return_value = make_mock_response(200, {
            "qid": "616c696365", "vector": [0.1, 0.2, 0.3],
        })

        result = await async_client.vector_get("alice")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "GET"
        assert "616c696365" in call_args.args[1]
        assert result["vector"] == [0.1, 0.2, 0.3]

    @pytest.mark.asyncio
    async def test_vector_get_not_found(self, async_client):
        """Test getting a vector for a node that doesn't have one."""
        async_client._session.request.return_value = make_mock_response(404, {
            "error": "vector not found for node",
        })

        result = await async_client.vector_get("alice")
        assert result is None

    @pytest.mark.asyncio
    async def test_vector_delete(self, async_client):
        """Test deleting a vector."""
        async_client._session.request.return_value = make_mock_response(200, {
            "status": "removed", "qid": "616c696365", "index_size": 0,
        })

        result = await async_client.vector_delete("alice")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "DELETE"
        assert "616c696365" in call_args.args[1]
        assert result["status"] == "removed"


# ===========================================================================
# Materialized Views tests
# ===========================================================================

class TestAsyncMaterializedViews:
    @pytest.mark.asyncio
    async def test_create_materialized_view(self, async_client):
        """Test creating a materialized view."""
        async_client._session.request.return_value = make_mock_response(201, {
            "view_id": "mv-123", "name": "test_view", "status": "created",
        })

        result = await async_client.create_materialized_view({
            "name": "test_view",
            "query": "MATCH (n) RETURN n",
            "refresh_mode": "incremental",
        })

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        assert call_args.args[1] == "/api/v2/materialized-views"
        body = call_args.kwargs.get("json")
        assert body["name"] == "test_view"
        assert result["view_id"] == "mv-123"

    @pytest.mark.asyncio
    async def test_list_materialized_views(self, async_client):
        """Test listing materialized views."""
        async_client._session.request.return_value = make_mock_response(200, [
            {"id": "mv-1", "name": "view1", "query": "MATCH (n) RETURN n"},
            {"id": "mv-2", "name": "view2", "query": "MATCH (m) RETURN m"},
        ])

        views = await async_client.list_materialized_views()
        assert len(views) == 2
        assert views[0]["id"] == "mv-1"

    @pytest.mark.asyncio
    async def test_get_materialized_view(self, async_client):
        """Test getting a materialized view."""
        async_client._session.request.return_value = make_mock_response(200, {
            "id": "mv-123", "name": "test_view",
            "query": "MATCH (n) RETURN n",
        })

        result = await async_client.get_materialized_view("mv-123")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "GET"
        assert "mv-123" in call_args.args[1]
        assert result["name"] == "test_view"

    @pytest.mark.asyncio
    async def test_delete_materialized_view(self, async_client):
        """Test deleting a materialized view."""
        async_client._session.request.return_value = make_mock_response(200, {
            "status": "dropped", "view_id": "mv-123",
        })

        result = await async_client.delete_materialized_view("mv-123")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "DELETE"
        assert "mv-123" in call_args.args[1]
        assert result["status"] == "dropped"

    @pytest.mark.asyncio
    async def test_query_materialized_view(self, async_client):
        """Test querying a materialized view."""
        async_client._session.request.return_value = make_mock_response(200, {
            "view_id": "mv-123", "rows": [], "count": 0,
        })

        result = await async_client.query_materialized_view("mv-123", limit=100)

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "GET"
        assert "mv-123" in call_args.args[1]
        assert "limit=100" in call_args.args[1]
        assert result["count"] == 0

    @pytest.mark.asyncio
    async def test_refresh_materialized_view(self, async_client):
        """Test refreshing a materialized view."""
        async_client._session.request.return_value = make_mock_response(200, {
            "status": "refreshed", "view_id": "mv-123",
            "query": "MATCH (n) RETURN n",
        })

        result = await async_client.refresh_materialized_view("mv-123")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        assert "mv-123" in call_args.args[1]
        assert "refresh" in call_args.args[1]
        assert result["status"] == "refreshed"


# ===========================================================================
# UDF tests
# ===========================================================================

class TestAsyncUDF:
    @pytest.mark.asyncio
    async def test_register_udf(self, async_client):
        """Test registering a UDF."""
        async_client._session.request.return_value = make_mock_response(201, {
            "status": "registered", "name": "my_udf", "language": "native",
        })

        result = await async_client.register_udf("my_udf", '{"expr": "a + b"}', "native")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        assert call_args.args[1] == "/api/v2/udf/register"
        body = call_args.kwargs.get("json")
        assert body["name"] == "my_udf"
        assert body["language"] == "native"
        assert result["status"] == "registered"

    @pytest.mark.asyncio
    async def test_list_udfs(self, async_client):
        """Test listing UDFs."""
        async_client._session.request.return_value = make_mock_response(200, {
            "udfs": [
                {"name": "my_udf", "language": "native"},
                {"name": "py_udf", "language": "python"},
            ],
            "count": 2,
        })

        udfs = await async_client.list_udfs()
        assert len(udfs) == 2
        assert udfs[0]["name"] == "my_udf"
        assert udfs[1]["language"] == "python"

    @pytest.mark.asyncio
    async def test_execute_udf(self, async_client):
        """Test executing a UDF."""
        async_client._session.request.return_value = make_mock_response(200, {
            "result": 42, "name": "my_udf",
        })

        result = await async_client.execute_udf("my_udf", {"a": 10, "b": 32})

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        assert "my_udf" in call_args.args[1]
        assert "execute" in call_args.args[1]
        body = call_args.kwargs.get("json")
        assert body == {"a": 10, "b": 32}
        assert result["result"] == 42

    @pytest.mark.asyncio
    async def test_delete_udf(self, async_client):
        """Test deleting a UDF."""
        async_client._session.request.return_value = make_mock_response(200, {
            "status": "unregistered", "name": "my_udf",
        })

        result = await async_client.delete_udf("my_udf")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "DELETE"
        assert "my_udf" in call_args.args[1]
        assert result["status"] == "unregistered"


# ===========================================================================
# Recipes tests
# ===========================================================================

class TestAsyncRecipes:
    @pytest.mark.asyncio
    async def test_create_recipe(self, async_client):
        """Test creating a recipe."""
        async_client._session.request.return_value = make_mock_response(201, {
            "status": "created", "name": "my_recipe", "recipe_count": 1,
        })

        result = await async_client.create_recipe({
            "name": "my_recipe",
            "steps": [{"query": "MATCH (n) RETURN n"}],
        })

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        assert call_args.args[1] == "/api/v2/recipes"
        body = call_args.kwargs.get("json")
        assert body["name"] == "my_recipe"
        assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_list_recipes(self, async_client):
        """Test listing recipes."""
        async_client._session.request.return_value = make_mock_response(200, {
            "recipes": [
                {"name": "recipe1", "description": "First"},
                {"name": "recipe2", "description": "Second"},
            ],
            "count": 2,
        })

        recipes = await async_client.list_recipes()
        assert len(recipes) == 2
        assert recipes[0]["name"] == "recipe1"

    @pytest.mark.asyncio
    async def test_get_recipe(self, async_client):
        """Test getting a recipe."""
        async_client._session.request.return_value = make_mock_response(200, {
            "name": "my_recipe", "steps": [],
        })

        result = await async_client.get_recipe("my_recipe")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "GET"
        assert "my_recipe" in call_args.args[1]
        assert result["name"] == "my_recipe"

    @pytest.mark.asyncio
    async def test_delete_recipe(self, async_client):
        """Test deleting a recipe."""
        async_client._session.request.return_value = make_mock_response(200, {
            "status": "deleted", "name": "my_recipe",
        })

        result = await async_client.delete_recipe("my_recipe")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "DELETE"
        assert "my_recipe" in call_args.args[1]
        assert result["status"] == "deleted"

    @pytest.mark.asyncio
    async def test_execute_recipe(self, async_client):
        """Test executing a recipe."""
        async_client._session.request.return_value = make_mock_response(200, {
            "run_id": "run-123", "recipe": "my_recipe",
            "status": "success", "result": {},
        })

        result = await async_client.execute_recipe("my_recipe")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        assert "my_recipe" in call_args.args[1]
        assert "execute" in call_args.args[1]
        assert result["run_id"] == "run-123"


# ===========================================================================
# SQL & Explain tests
# ===========================================================================

class TestAsyncSqlExplain:
    @pytest.mark.asyncio
    async def test_sql(self, async_client):
        """Test SQL query."""
        async_client._session.request.return_value = make_mock_response(200, {
            "columns": ["id", "name"],
            "rows": [[1, "Alice"]],
            "row_count": 1,
            "query_time_ms": 5.2,
            "translated_cypher": "MATCH (n) RETURN n.id, n.name",
        })

        result = await async_client.sql("SELECT id, name FROM nodes")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        assert call_args.args[1] == "/api/v2/query/sql"
        body = call_args.kwargs.get("json")
        assert body["query"] == "SELECT id, name FROM nodes"
        assert result["row_count"] == 1

    @pytest.mark.asyncio
    async def test_explain(self, async_client):
        """Test EXPLAIN query."""
        async_client._session.request.return_value = make_mock_response(200, {
            "query": "MATCH (n) RETURN n",
            "plan": {"start_with": "AllNodesScan", "filters": [], "cost": 10.0},
            "estimated_cost": 10.0,
            "estimated_rows": 10,
            "explanation": "Query Execution Plan:\n  1. Start with: AllNodesScan",
        })

        result = await async_client.explain("MATCH (n) RETURN n")

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        assert call_args.args[1] == "/api/v2/query/explain"
        body = call_args.kwargs.get("json")
        assert body["query"] == "MATCH (n) RETURN n"
        assert body["analyze"] is False
        assert result["estimated_cost"] == 10.0

    @pytest.mark.asyncio
    async def test_explain_with_analyze(self, async_client):
        """Test EXPLAIN ANALYZE."""
        async_client._session.request.return_value = make_mock_response(200, {
            "query": "MATCH (n) RETURN n",
            "plan": {"start_with": "AllNodesScan", "filters": [], "cost": 5.0},
            "estimated_cost": 5.0,
            "estimated_rows": 5,
            "explanation": "Plan...",
            "actual_stats": {"actual_rows": 3, "execution_time_ms": 1.2, "nodes_examined": 3},
        })

        result = await async_client.explain("MATCH (n) RETURN n", analyze=True)

        body = async_client._session.request.call_args.kwargs.get("json")
        assert body["analyze"] is True
        assert result["actual_stats"]["actual_rows"] == 3


# ===========================================================================
# Cluster, Storage & System tests
# ===========================================================================

class TestAsyncClusterStorageSystem:
    @pytest.mark.asyncio
    async def test_cluster_stats(self, async_client):
        """Test cluster stats."""
        async_client._session.request.return_value = make_mock_response(200, {
            "nodes": 3, "shards": 256, "status": "healthy",
        })

        result = await async_client.cluster_stats()

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "GET"
        assert call_args.args[1] == "/api/v2/cluster/stats"
        assert result["nodes"] == 3

    @pytest.mark.asyncio
    async def test_storage_status(self, async_client):
        """Test storage status."""
        async_client._session.request.return_value = make_mock_response(200, {
            "backend": "tiered", "total_objects": 100,
            "hot_objects": 50, "warm_objects": 30, "cold_objects": 20,
        })

        result = await async_client.storage_status()

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "GET"
        assert call_args.args[1] == "/api/v2/storage/status"
        assert result["backend"] == "tiered"
        assert result["hot_objects"] == 50

    @pytest.mark.asyncio
    async def test_storage_migrate(self, async_client):
        """Test storage migration."""
        async_client._session.request.return_value = make_mock_response(200, {
            "status": "completed", "migrated_objects": 15,
        })

        result = await async_client.storage_migrate()

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        assert call_args.args[1] == "/api/v2/storage/migrate"
        assert result["migrated_objects"] == 15

    @pytest.mark.asyncio
    async def test_system_info(self, async_client):
        """Test system info."""
        async_client._session.request.return_value = make_mock_response(200, {
            "version": "0.2.0", "num_shards": 256,
            "max_nodes_per_shard": 10000,
        })

        result = await async_client.system_info()

        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "GET"
        assert call_args.args[1] == "/api/v2/system/info"
        assert result["version"] == "0.2.0"


# ===========================================================================
# Cypher alias test
# ===========================================================================

class TestCypherAlias:
    @pytest.mark.asyncio
    async def test_cypher_alias(self, async_client):
        """Test that cypher() works as an alias for query()."""
        async_client._session.request.return_value = make_mock_response(200, {
            "columns": ["n"], "rows": [["alice"]], "error": None,
        })

        results = await async_client.cypher("MATCH (n) RETURN n LIMIT 1")
        assert len(results) == 1
        assert results[0] == {"n": "alice"}

        # Verify it hits the same endpoint as query()
        call_args = async_client._session.request.call_args
        assert call_args.args[0] == "POST"
        assert call_args.args[1] == "/api/v2/query/cypher"


# ===========================================================================
# Retry configuration tests
# ===========================================================================

class TestRetryConfig:
    def test_default_retry_config(self):
        """Test default retry configuration."""
        client = AsyncNexoraClient("http://localhost:8080")
        assert client.max_retries == 3
        assert client.backoff_factor == 0.5

    def test_custom_retry_config(self):
        """Test custom retry configuration."""
        client = AsyncNexoraClient(
            "http://localhost:8080",
            max_retries=5,
            backoff_factor=1.0,
        )
        assert client.max_retries == 5
        assert client.backoff_factor == 1.0

    def test_connect_read_timeout(self):
        """Test separate connect and read timeouts."""
        client = AsyncNexoraClient(
            "http://localhost:8080",
            timeout=60.0,
            connect_timeout=5.0,
            read_timeout=30.0,
        )
        assert client._connect_timeout == 5.0
        assert client._read_timeout == 30.0
        assert client._total_timeout == 60.0

    def test_sync_client_passes_retry_config(self):
        """Test that NexoraClient passes retry config to async client."""
        client = NexoraClient(
            "http://localhost:8080",
            max_retries=10,
            backoff_factor=0.1,
            connect_timeout=3.0,
            read_timeout=15.0,
        )
        assert client._async.max_retries == 10
        assert client._async.backoff_factor == 0.1
        assert client._async._connect_timeout == 3.0
        assert client._async._read_timeout == 15.0

    @pytest.mark.asyncio
    async def test_retry_on_connection_error(self, async_client):
        """Test that connection errors are retried."""
        call_count = 0

        async def failing_then_success(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("connection failed")
            return {"status": "ok"}

        async_client._do_request = failing_then_success
        async_client.max_retries = 3
        async_client.backoff_factor = 0.001  # Fast backoff for tests

        result = await async_client._request("GET", "/api/v2/health")
        assert result == {"status": "ok"}
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_retry_on_5xx_error(self, async_client):
        """Test that 5xx errors are retried."""
        call_count = 0

        async def failing_then_success(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise NexoraError("server error", status_code=500)
            return {"status": "healthy"}

        async_client._do_request = failing_then_success
        async_client.max_retries = 3
        async_client.backoff_factor = 0.001

        result = await async_client._request("GET", "/api/v2/health")
        assert result == {"status": "healthy"}
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_no_retry_on_4xx_error(self, async_client):
        """Test that 4xx errors are NOT retried."""
        call_count = 0

        async def always_fail(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            raise NexoraError("bad request", status_code=400)

        async_client._do_request = always_fail
        async_client.max_retries = 3

        with pytest.raises(NexoraError):
            await async_client._request("GET", "/api/v2/health")
        assert call_count == 1


# ===========================================================================
# Sync client: new methods tests
# ===========================================================================

class TestSyncClientNewMethods:
    def test_cypher_alias(self, sync_client):
        """Test sync cypher() alias."""
        with patch.object(sync_client._async, "cypher", new_callable=AsyncMock) as mock_c:
            mock_c.return_value = [{"n": "alice"}]
            results = sync_client.cypher("MATCH (n) RETURN n")
            assert len(results) == 1
            mock_c.assert_called_once()

    def test_vector_index(self, sync_client):
        """Test sync vector_index."""
        with patch.object(sync_client._async, "vector_index", new_callable=AsyncMock) as mock_vi:
            mock_vi.return_value = {"status": "indexed"}
            result = sync_client.vector_index("alice", [0.1, 0.2])
            assert result["status"] == "indexed"
            mock_vi.assert_called_once_with("alice", [0.1, 0.2])

    def test_vector_search(self, sync_client):
        """Test sync vector_search."""
        with patch.object(sync_client._async, "vector_search", new_callable=AsyncMock) as mock_vs:
            mock_vs.return_value = {"neighbors": []}
            result = sync_client.vector_search([0.1, 0.2], k=5)
            assert "neighbors" in result
            mock_vs.assert_called_once_with([0.1, 0.2], 5)

    def test_sql(self, sync_client):
        """Test sync sql()."""
        with patch.object(sync_client._async, "sql", new_callable=AsyncMock) as mock_sql:
            mock_sql.return_value = {"columns": [], "rows": []}
            result = sync_client.sql("SELECT * FROM nodes")
            assert "columns" in result
            mock_sql.assert_called_once_with("SELECT * FROM nodes")

    def test_explain(self, sync_client):
        """Test sync explain()."""
        with patch.object(sync_client._async, "explain", new_callable=AsyncMock) as mock_ex:
            mock_ex.return_value = {"estimated_cost": 10.0}
            result = sync_client.explain("MATCH (n) RETURN n")
            assert result["estimated_cost"] == 10.0
            mock_ex.assert_called_once_with("MATCH (n) RETURN n", False)

    def test_create_materialized_view(self, sync_client):
        """Test sync create_materialized_view."""
        with patch.object(sync_client._async, "create_materialized_view", new_callable=AsyncMock) as mock_mv:
            mock_mv.return_value = {"view_id": "mv-1"}
            result = sync_client.create_materialized_view({"name": "test", "query": "MATCH (n) RETURN n"})
            assert result["view_id"] == "mv-1"

    def test_register_udf(self, sync_client):
        """Test sync register_udf."""
        with patch.object(sync_client._async, "register_udf", new_callable=AsyncMock) as mock_udf:
            mock_udf.return_value = {"status": "registered"}
            result = sync_client.register_udf("my_func", "code", "native")
            assert result["status"] == "registered"

    def test_create_recipe(self, sync_client):
        """Test sync create_recipe."""
        with patch.object(sync_client._async, "create_recipe", new_callable=AsyncMock) as mock_rc:
            mock_rc.return_value = {"status": "created"}
            result = sync_client.create_recipe({"name": "test"})
            assert result["status"] == "created"

    def test_cluster_stats(self, sync_client):
        """Test sync cluster_stats."""
        with patch.object(sync_client._async, "cluster_stats", new_callable=AsyncMock) as mock_cs:
            mock_cs.return_value = {"nodes": 3}
            result = sync_client.cluster_stats()
            assert result["nodes"] == 3

    def test_storage_status(self, sync_client):
        """Test sync storage_status."""
        with patch.object(sync_client._async, "storage_status", new_callable=AsyncMock) as mock_ss:
            mock_ss.return_value = {"backend": "memory"}
            result = sync_client.storage_status()
            assert result["backend"] == "memory"

    def test_system_info(self, sync_client):
        """Test sync system_info."""
        with patch.object(sync_client._async, "system_info", new_callable=AsyncMock) as mock_si:
            mock_si.return_value = {"version": "0.2.0"}
            result = sync_client.system_info()
            assert result["version"] == "0.2.0"
