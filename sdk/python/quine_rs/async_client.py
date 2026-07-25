"""
Async client for the Nexora-RS streaming graph database.

Uses ``aiohttp`` for non-blocking HTTP requests.  For synchronous usage,
see :class:`~nexora_rs.client.NexoraClient`.

Example::

    import asyncio
    from nexora_rs import AsyncNexoraClient

    async def main():
        async with AsyncNexoraClient("http://localhost:8080") as client:
            await client.set_property("alice", "name", "Alice")
            name = await client.get_property("alice", "name")
            print(name)  # "Alice"

            results = await client.query("MATCH (n) RETURN n LIMIT 10")
            for row in results:
                print(row)

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any, Awaitable, Callable

import aiohttp

from .exceptions import (
    AuthenticationError,
    ConnectionError as NexoraConnectionError,
    NodeNotFoundError,
    QueryError,
    NexoraError,
    TimeoutError as NexoraTimeoutError,
)
from .types import StandingQueryPattern


class AsyncNexoraClient:
    """Async client for the Nexora-RS graph database.

    Args:
        base_url: Base URL of the Nexora-RS server (default ``http://localhost:8080``).
        api_key: Optional Bearer token for authentication.
        timeout: Default total request timeout in seconds.
        max_retries: Maximum number of retries for failed requests (default 3).
            Retries are attempted on connection errors and HTTP 5xx responses.
        backoff_factor: Multiplier for exponential backoff between retries
            (default 0.5).  Wait time is ``backoff_factor * (2 ** attempt)``.
        connect_timeout: Connection-level timeout in seconds.  If ``None``,
            falls back to *timeout*.
        read_timeout: Read-level timeout in seconds.  If ``None``,
            falls back to *timeout*.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        api_key: str | None = None,
        timeout: float = 30.0,
        *,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self._total_timeout = timeout
        self._connect_timeout = connect_timeout if connect_timeout is not None else timeout
        self._read_timeout = read_timeout if read_timeout is not None else timeout
        self.timeout = aiohttp.ClientTimeout(
            total=self._total_timeout,
            connect=self._connect_timeout,
            sock_read=self._read_timeout,
        )
        self._session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> AsyncNexoraClient:
        await self._ensure_session()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._session = aiohttp.ClientSession(
                base_url=self.base_url,
                headers=headers,
                timeout=self.timeout,
            )
        return self._session

    # ------------------------------------------------------------------
    # Low-level request helper
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        json_body: Any = None,
    ) -> Any:
        """Execute an HTTP request and return the parsed JSON response.

        Retries on connection errors and HTTP 5xx responses up to
        ``max_retries`` times, with exponential backoff.

        Raises:
            AuthenticationError: On HTTP 401/403.
            NodeNotFoundError: On HTTP 404.
            QueryError: On HTTP 400 (bad query).
            TimeoutError: On request timeout.
            ConnectionError: On network failure.
            NexoraError: On any other error.
        """
        last_exc: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                return await self._do_request(method, path, json_body)
            except NexoraConnectionError as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    delay = self.backoff_factor * (2 ** attempt)
                    await asyncio.sleep(delay)
                    continue
                raise
            except NexoraError as exc:
                # Retry on 5xx server errors
                if exc.status_code and 500 <= exc.status_code < 600:
                    last_exc = exc
                    if attempt < self.max_retries:
                        delay = self.backoff_factor * (2 ** attempt)
                        await asyncio.sleep(delay)
                        continue
                raise
            except NexoraTimeoutError as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    delay = self.backoff_factor * (2 ** attempt)
                    await asyncio.sleep(delay)
                    continue
                raise

        # Should not reach here, but just in case
        if last_exc:
            raise last_exc
        raise NexoraError("Unexpected state in request retry loop")

    async def _do_request(
        self,
        method: str,
        path: str,
        json_body: Any = None,
    ) -> Any:
        """Execute a single HTTP request (no retry)."""
        session = await self._ensure_session()
        url = path  # session has base_url set

        try:
            async with session.request(method, url, json=json_body) as resp:
                body_text = await resp.text()
                try:
                    body = json.loads(body_text) if body_text else {}
                except json.JSONDecodeError:
                    body = {"raw": body_text}

                if resp.status >= 400:
                    error_msg = (
                        body.get("error", body.get("raw", body_text))
                        if isinstance(body, dict)
                        else body_text
                    )
                    if resp.status in (401, 403):
                        raise AuthenticationError(
                            str(error_msg), status_code=resp.status, response_body=body
                        )
                    if resp.status == 404:
                        raise NodeNotFoundError(
                            str(error_msg), status_code=resp.status, response_body=body
                        )
                    if resp.status == 400:
                        raise QueryError(
                            str(error_msg), status_code=resp.status, response_body=body
                        )
                    raise NexoraError(
                        str(error_msg), status_code=resp.status, response_body=body
                    )
                return body

        except aiohttp.ClientConnectorError as exc:
            raise NexoraConnectionError(f"Cannot connect to {self.base_url}: {exc}") from exc
        except aiohttp.ServerTimeoutError as exc:
            raise NexoraTimeoutError(f"Request to {path} timed out") from exc
        except aiohttp.ClientError as exc:
            if "timeout" in str(exc).lower():
                raise NexoraTimeoutError(f"Request to {path} timed out") from exc
            raise NexoraConnectionError(f"Connection error: {exc}") from exc

    # ------------------------------------------------------------------
    # ID encoding
    # ------------------------------------------------------------------

    @staticmethod
    def _hex_id(node_id: str) -> str:
        """Encode a string node ID to hexadecimal.

        Nexora-RS uses hex-encoded node IDs in URL paths.
        """
        return node_id.encode("utf-8").hex()

    # ------------------------------------------------------------------
    # Cypher queries
    # ------------------------------------------------------------------

    async def query(
        self,
        cypher: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a Cypher query and return rows as dictionaries.

        Args:
            cypher: Cypher query string.
            params: Optional query parameters (client-side substitution).

        Returns:
            List of row dictionaries keyed by column name.

        Raises:
            QueryError: If the query is invalid.
        """
        query_str = self._substitute_params(cypher, params)
        body = {"query": query_str}
        resp = await self._request("POST", "/api/v2/query/cypher", json_body=body)

        if isinstance(resp, dict) and resp.get("error"):
            raise QueryError(resp["error"])

        columns: list[str] = resp.get("columns", []) if isinstance(resp, dict) else []
        raw_rows: list[list[Any]] = resp.get("rows", []) if isinstance(resp, dict) else []

        # Convert positional rows to dicts keyed by column name
        result: list[dict[str, Any]] = []
        for row in raw_rows:
            if isinstance(row, list):
                result.append(dict(zip(columns, row)))
            elif isinstance(row, dict):
                result.append(row)
            else:
                result.append({"value": row})
        return result

    async def query_one(
        self,
        cypher: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Execute a Cypher query and return the first row, or ``None``.

        Args:
            cypher: Cypher query string.
            params: Optional query parameters.

        Returns:
            First row as a dict, or ``None`` if no results.
        """
        rows = await self.query(cypher, params)
        return rows[0] if rows else None

    async def cypher(
        self,
        cypher: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a Cypher query (alias for :meth:`query`).

        Args:
            cypher: Cypher query string.
            params: Optional query parameters (client-side substitution).

        Returns:
            List of row dictionaries keyed by column name.
        """
        return await self.query(cypher, params)

    @staticmethod
    def _substitute_params(cypher: str, params: dict[str, Any] | None) -> str:
        """Substitute $param placeholders with JSON-encoded values.

        This is a simple client-side substitution since the server does not
        yet support parameterized queries natively.
        """
        if not params:
            return cypher

        def replacer(match: re.Match) -> str:
            key = match.group(1)
            if key in params:
                val = params[key]
                if isinstance(val, str):
                    return json.dumps(val)
                return str(val)
            return match.group(0)

        return re.sub(r"\$(\w+)", replacer, cypher)

    # ------------------------------------------------------------------
    # Node operations
    # ------------------------------------------------------------------

    async def create_node(
        self,
        labels: list[str],
        properties: dict[str, Any],
    ) -> str:
        """Create a node by setting its labels and properties.

        Nexora-RS creates nodes implicitly when properties are set.
        A ``labels`` property is set to identify the node's labels.

        Args:
            labels: List of labels (e.g. ``["Person"]``).
            properties: Property key-value pairs.

        Returns:
            The node ID (same as the key used in properties, or generated).
        """
        node_id = properties.get("id", "")
        if not node_id:
            raise NexoraError("properties must contain an 'id' field for node creation")

        # Set labels property first
        if labels:
            await self.set_property(node_id, "labels", labels)

        # Set all other properties
        for key, value in properties.items():
            if key != "id":
                await self.set_property(node_id, key, value)

        return node_id

    async def get_node(self, node_id: str) -> dict[str, Any] | None:
        """Retrieve a node's data via a Cypher query.

        Args:
            node_id: The node identifier string.

        Returns:
            Dict with ``id``, ``labels``, and ``properties``, or ``None``
            if the node does not exist.
        """
        qid = self._hex_id(node_id)
        cypher = f"MATCH (n) WHERE id(n) = '{qid}' RETURN n LIMIT 1"
        rows = await self.query(cypher)
        if not rows:
            return None
        node_data = rows[0].get("n", rows[0])
        return {"id": node_id, **node_data} if isinstance(node_data, dict) else {"id": node_id, "value": node_data}

    async def set_property(
        self,
        node_id: str,
        key: str,
        value: Any,
    ) -> None:
        """Set a property on a node.

        Args:
            node_id: The node identifier string.
            key: Property key.
            value: Property value (will be JSON-encoded).
        """
        qid = self._hex_id(node_id)
        await self._request(
            "PUT",
            f"/api/v2/graph/node/{qid}/property/{key}",
            json_body={"value": value},
        )

    async def get_property(self, node_id: str, key: str) -> Any:
        """Get a property value from a node.

        Args:
            node_id: The node identifier string.
            key: Property key.

        Returns:
            The property value, or ``None`` if not found.
        """
        qid = self._hex_id(node_id)
        resp = await self._request("GET", f"/api/v2/graph/node/{qid}/property/{key}")
        if isinstance(resp, dict) and resp.get("not_found"):
            return None
        return resp.get("value") if isinstance(resp, dict) else resp

    async def get_all_properties(self, node_id: str) -> dict[str, Any]:
        """Get all properties of a node.

        Args:
            node_id: The node identifier string.

        Returns:
            Dict of all property key-value pairs on the node.
        """
        qid = self._hex_id(node_id)
        resp = await self._request("GET", f"/api/v2/graph/node/{qid}/properties")
        if isinstance(resp, dict):
            return resp.get("properties", {})
        return {}

    async def delete_node(self, node_id: str) -> None:
        """Delete a node and all its edges via a Cypher query.

        Args:
            node_id: The node identifier string.
        """
        qid = self._hex_id(node_id)
        cypher = f"MATCH (n) WHERE id(n) = '{qid}' DETACH DELETE n"
        await self.query(cypher)

    # ------------------------------------------------------------------
    # Edge operations
    # ------------------------------------------------------------------

    async def add_edge(
        self,
        from_id: str,
        to_id: str,
        edge_type: str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Create a directed edge from one node to another.

        Args:
            from_id: Source node ID.
            to_id: Target node ID.
            edge_type: Edge type label (e.g. ``"KNOWS"``).
            properties: Optional edge properties (stored via Cypher if provided).
        """
        qid = self._hex_id(from_id)
        target_hex = self._hex_id(to_id)
        await self._request(
            "POST",
            f"/api/v2/graph/node/{qid}/edges",
            json_body={
                "edge_type": edge_type,
                "target": target_hex,
                "direction": "out",
            },
        )

        # If edge properties are provided, set them via Cypher
        if properties:
            prop_set = ", ".join(
                f"r.{k} = {json.dumps(v)}" for k, v in properties.items()
            )
            cypher = (
                f"MATCH (a)-[r:{edge_type}]->(b) "
                f"WHERE id(a) = '{qid}' AND id(b) = '{target_hex}' "
                f"SET {prop_set}"
            )
            await self.query(cypher)

    async def get_edges(
        self,
        node_id: str,
        direction: str = "both",
    ) -> list[dict[str, Any]]:
        """Get edges connected to a node.

        Args:
            node_id: The node identifier string.
            direction: ``"out"``, ``"in"``, or ``"both"`` (default).

        Returns:
            List of edge dicts with ``edge_type``, ``direction``, and ``other``
            (the other node's hex ID).
        """
        qid = self._hex_id(node_id)
        resp = await self._request("GET", f"/api/v2/graph/node/{qid}/edges")
        edges = resp.get("edges", []) if isinstance(resp, dict) else []

        if direction == "both":
            return edges
        return [e for e in edges if e.get("direction") == direction]

    async def delete_edge(self, from_id: str, to_id: str, edge_type: str) -> None:
        """Delete an edge between two nodes via a Cypher query.

        Args:
            from_id: Source node ID.
            to_id: Target node ID.
            edge_type: Edge type label to delete.
        """
        from_hex = self._hex_id(from_id)
        to_hex = self._hex_id(to_id)
        cypher = (
            f"MATCH (a)-[r:{edge_type}]->(b) "
            f"WHERE id(a) = '{from_hex}' AND id(b) = '{to_hex}' "
            f"DELETE r"
        )
        await self.query(cypher)

    async def remove_edge(self, from_id: str, edge_type: str, to_id: str) -> None:
        """Delete an edge between two nodes (alias with reordered args).

        Args:
            from_id: Source node ID.
            edge_type: Edge type label to delete.
            to_id: Target node ID.
        """
        await self.delete_edge(from_id, to_id, edge_type)

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    async def batch_create_nodes(self, nodes: list[dict[str, Any]]) -> list[str]:
        """Create multiple nodes sequentially.

        Args:
            nodes: List of node dicts, each with ``labels`` and ``properties``.

        Returns:
            List of created node IDs.
        """
        ids: list[str] = []
        for node in nodes:
            labels = node.get("labels", [])
            properties = node.get("properties", {})
            node_id = await self.create_node(labels, properties)
            ids.append(node_id)
        return ids

    async def batch_query(
        self,
        queries: list[str],
    ) -> list[list[dict[str, Any]]]:
        """Execute multiple Cypher queries sequentially.

        Args:
            queries: List of Cypher query strings.

        Returns:
            List of result lists, one per query.
        """
        results: list[list[dict[str, Any]]] = []
        for q in queries:
            rows = await self.query(q)
            results.append(rows)
        return results

    async def bulk_create(self, nodes: list[dict[str, Any]]) -> list[str]:
        """Create multiple nodes in bulk.

        Alias for :meth:`batch_create_nodes`.

        Args:
            nodes: List of dicts with ``labels`` and ``properties`` keys.

        Returns:
            List of created node IDs.
        """
        return await self.batch_create_nodes(nodes)

    async def bulk_create_edges(self, edges: list[dict[str, Any]]) -> None:
        """Create multiple edges in bulk.

        Each dict in *edges* should have ``from_id``, ``to_id``,
        ``edge_type``, and optionally ``properties``.

        Args:
            edges: List of edge specification dicts.
        """
        for edge in edges:
            await self.add_edge(
                edge["from_id"],
                edge["to_id"],
                edge["edge_type"],
                edge.get("properties"),
            )

    # ------------------------------------------------------------------
    # Standing Queries
    # ------------------------------------------------------------------

    async def create_standing_query(
        self,
        pattern: StandingQueryPattern,
        name: str | None = None,
    ) -> str:
        """Register a new Standing Query.

        Args:
            pattern: Pattern specification dict.
            name: Optional name for the query (auto-generated if omitted).

        Returns:
            The Standing Query ID assigned by the server.
        """
        body = {
            "name": name or f"sq-{pattern.get('type', 'query')}",
            "pattern": pattern,
        }
        resp = await self._request("POST", "/api/v2/standing-query", json_body=body)
        return resp.get("id", "") if isinstance(resp, dict) else ""

    async def list_standing_queries(self) -> list[dict[str, Any]]:
        """List all registered Standing Queries.

        Returns:
            List of dicts with ``id``, ``name``, and ``match_count``.
        """
        resp = await self._request("GET", "/api/v2/standing-query")
        return resp.get("standing_queries", []) if isinstance(resp, dict) else []

    async def delete_standing_query(self, query_id: str) -> None:
        """Delete a Standing Query by ID.

        Args:
            query_id: The Standing Query UUID.
        """
        await self._request("DELETE", f"/api/v2/standing-query/{query_id}")

    # ------------------------------------------------------------------
    # Health & Stats
    # ------------------------------------------------------------------

    async def health(self) -> dict[str, Any]:
        """Check server health.

        Returns:
            Dict with ``status``, ``active_nodes``, ``shards``, etc.
        """
        return await self._request("GET", "/api/v2/health")

    async def stats(self) -> dict[str, Any]:
        """Get server statistics.

        Returns:
            Dict with ``version``, ``num_shards``, ``max_nodes_per_shard``, etc.
        """
        return await self._request("GET", "/api/v2/system/info")

    # ------------------------------------------------------------------
    # Convenience: ingest
    # ------------------------------------------------------------------

    async def ingest_file(self, path: str, id_field: str = "id") -> dict[str, Any]:
        """Start a file ingest pipeline.

        Args:
            path: Relative path to a JSONL file (must be within the server's
                allowed ingest directory).
            id_field: Field name to use as the node ID.

        Returns:
            Dict with ``status``, ``path``, and ``name``.
        """
        return await self._request(
            "POST",
            "/api/v2/ingest/file",
            json_body={"path": path, "id_field": id_field},
        )

    # ------------------------------------------------------------------
    # Vector operations (HNSW similarity search)
    # ------------------------------------------------------------------

    async def vector_index(self, qid: str, vector: list[float]) -> dict[str, Any]:
        """Index a vector for a node.

        Args:
            qid: Node identifier string (will be hex-encoded).
            vector: Embedding vector (list of floats).

        Returns:
            Dict with ``status``, ``qid``, and ``index_size``.
        """
        hex_qid = self._hex_id(qid)
        return await self._request(
            "POST",
            "/api/v2/vector/index",
            json_body={"qid": hex_qid, "vector": vector},
        )

    async def vector_search(self, vector: list[float], k: int = 10) -> dict[str, Any]:
        """Search for k-nearest neighbors of a vector.

        Args:
            vector: Query embedding vector.
            k: Number of nearest neighbors to return (default 10).

        Returns:
            Dict with ``query``, ``k``, and ``neighbors`` (list of
            ``{qid, distance}`` dicts).
        """
        return await self._request(
            "POST",
            "/api/v2/vector/search",
            json_body={"vector": vector, "k": k},
        )

    async def vector_get(self, qid: str) -> dict[str, Any] | None:
        """Get the vector associated with a node.

        Args:
            qid: Node identifier string (will be hex-encoded).

        Returns:
            Dict with ``qid`` and ``vector``, or ``None`` if not found.
        """
        hex_qid = self._hex_id(qid)
        try:
            return await self._request("GET", f"/api/v2/vector/node/{hex_qid}")
        except NodeNotFoundError:
            return None

    async def vector_delete(self, qid: str) -> dict[str, Any]:
        """Delete the vector associated with a node.

        Args:
            qid: Node identifier string (will be hex-encoded).

        Returns:
            Dict with ``status``, ``qid``, and ``index_size``.
        """
        hex_qid = self._hex_id(qid)
        return await self._request("DELETE", f"/api/v2/vector/node/{hex_qid}")

    # ------------------------------------------------------------------
    # Materialized Views
    # ------------------------------------------------------------------

    async def create_materialized_view(self, definition: dict[str, Any]) -> dict[str, Any]:
        """Create a new materialized view.

        Args:
            definition: View definition dict with keys:

                - ``name`` (str): View name.
                - ``query`` (str): Source Cypher query.
                - ``refresh_mode`` (str, optional): ``"incremental"``,
                  ``"manual"``, or a schedule expression. Defaults to
                  ``"incremental"``.
                - ``schema`` (list, optional): Column definitions, each
                  with ``name`` and ``data_type``.

        Returns:
            Dict with ``view_id``, ``name``, and ``status``.
        """
        return await self._request(
            "POST",
            "/api/v2/materialized-views",
            json_body=definition,
        )

    async def list_materialized_views(self) -> list[dict[str, Any]]:
        """List all materialized views.

        Returns:
            List of view dicts with ``id``, ``name``, ``query``,
            ``refresh_mode``, ``created_at``, and ``last_refreshed``.
        """
        resp = await self._request("GET", "/api/v2/materialized-views")
        if isinstance(resp, list):
            return resp
        return resp.get("views", []) if isinstance(resp, dict) else []

    async def get_materialized_view(self, view_id: str) -> dict[str, Any]:
        """Get a materialized view definition by ID.

        Args:
            view_id: The materialized view identifier.

        Returns:
            Dict with ``id``, ``name``, ``query``, ``refresh_mode``,
            ``schema``, etc.
        """
        return await self._request("GET", f"/api/v2/materialized-views/{view_id}")

    async def delete_materialized_view(self, view_id: str) -> dict[str, Any]:
        """Delete a materialized view by ID.

        Args:
            view_id: The materialized view identifier.

        Returns:
            Dict with ``status`` and ``view_id``.
        """
        return await self._request("DELETE", f"/api/v2/materialized-views/{view_id}")

    async def query_materialized_view(
        self,
        view_id: str,
        limit: int | None = None,
        column: str | None = None,
        value: str | None = None,
    ) -> dict[str, Any]:
        """Query data from a materialized view.

        Args:
            view_id: The materialized view identifier.
            limit: Optional maximum number of rows to return.
            column: Optional column name for filtered query.
            value: Optional value to match (requires *column*).

        Returns:
            Dict with ``view_id``, ``rows``, and ``count``.
        """
        params: list[str] = []
        if limit is not None:
            params.append(f"limit={limit}")
        if column is not None:
            params.append(f"column={column}")
        if value is not None:
            params.append(f"value={value}")
        query_string = f"?{'&'.join(params)}" if params else ""
        return await self._request(
            "GET", f"/api/v2/materialized-views/{view_id}/data{query_string}"
        )

    async def refresh_materialized_view(self, view_id: str) -> dict[str, Any]:
        """Manually refresh a materialized view.

        Args:
            view_id: The materialized view identifier.

        Returns:
            Dict with ``status``, ``view_id``, and ``query``.
        """
        return await self._request(
            "POST", f"/api/v2/materialized-views/{view_id}/refresh"
        )

    # ------------------------------------------------------------------
    # User-Defined Functions (UDF)
    # ------------------------------------------------------------------

    async def register_udf(
        self,
        name: str,
        code: str,
        language: str = "native",
    ) -> dict[str, Any]:
        """Register a new user-defined function.

        Args:
            name: UDF name.
            code: Function code.  For ``"native"`` language, this is a
                JSON expression specification.  For ``"wasm"``, hex-encoded
                wasm bytes.  For ``"python"``, Python source code.
            language: One of ``"native"`` (default), ``"wasm"``, or
                ``"python"``.

        Returns:
            Dict with ``status``, ``name``, and ``language``.
        """
        return await self._request(
            "POST",
            "/api/v2/udf/register",
            json_body={"name": name, "code": code, "language": language},
        )

    async def list_udfs(self) -> list[dict[str, Any]]:
        """List all registered UDFs.

        Returns:
            List of dicts with ``name`` and ``language``.
        """
        resp = await self._request("GET", "/api/v2/udf")
        return resp.get("udfs", []) if isinstance(resp, dict) else []

    async def execute_udf(self, name: str, input: Any) -> dict[str, Any]:
        """Execute a UDF by name with the given input.

        Args:
            name: UDF name.
            input: Input value passed to the UDF (any JSON-serializable
                value).

        Returns:
            Dict with ``result`` and ``name``.
        """
        return await self._request(
            "POST",
            f"/api/v2/udf/{name}/execute",
            json_body=input,
        )

    async def delete_udf(self, name: str) -> dict[str, Any]:
        """Delete a UDF by name.

        Args:
            name: UDF name.

        Returns:
            Dict with ``status`` and ``name``.
        """
        return await self._request("DELETE", f"/api/v2/udf/{name}")

    # ------------------------------------------------------------------
    # Recipes
    # ------------------------------------------------------------------

    async def create_recipe(self, recipe: dict[str, Any]) -> dict[str, Any]:
        """Create a new recipe.

        Args:
            recipe: Recipe definition dict with keys:

                - ``name`` (str): Recipe name.
                - ``description`` (str, optional): Description.
                - ``steps`` (list): List of step dicts, each with ``query``
                  and optional ``description``.
                - ``trigger`` (dict, optional): Trigger definition with
                  ``event_type`` and optional ``filter``.

        Returns:
            Dict with ``status``, ``name``, and ``recipe_count``.
        """
        return await self._request("POST", "/api/v2/recipes", json_body=recipe)

    async def list_recipes(self) -> list[dict[str, Any]]:
        """List all recipes.

        Returns:
            List of recipe summary dicts.
        """
        resp = await self._request("GET", "/api/v2/recipes")
        return resp.get("recipes", []) if isinstance(resp, dict) else []

    async def get_recipe(self, name: str) -> dict[str, Any]:
        """Get a recipe definition by name.

        Args:
            name: Recipe name.

        Returns:
            Dict with recipe details including ``steps``, ``ingest_sources``,
            ``outputs``, and ``status``.
        """
        return await self._request("GET", f"/api/v2/recipes/{name}")

    async def delete_recipe(self, name: str) -> dict[str, Any]:
        """Delete a recipe by name.

        Args:
            name: Recipe name.

        Returns:
            Dict with ``status`` and ``name``.
        """
        return await self._request("DELETE", f"/api/v2/recipes/{name}")

    async def execute_recipe(self, name: str) -> dict[str, Any]:
        """Execute a recipe by name.

        Args:
            name: Recipe name.

        Returns:
            Dict with ``run_id``, ``recipe``, ``status``, and ``result``.
        """
        return await self._request("POST", f"/api/v2/recipes/{name}/execute")

    # ------------------------------------------------------------------
    # SQL & Explain
    # ------------------------------------------------------------------

    async def sql(self, query: str) -> dict[str, Any]:
        """Execute a SQL query.

        Args:
            query: SQL query string.

        Returns:
            Dict with ``columns``, ``rows``, ``row_count``,
            ``query_time_ms``, and ``translated_cypher``.
        """
        return await self._request(
            "POST",
            "/api/v2/query/sql",
            json_body={"query": query},
        )

    async def explain(self, query: str, analyze: bool = False) -> dict[str, Any]:
        """Generate an execution plan for a Cypher query.

        Args:
            query: Cypher query string to explain.
            analyze: If ``True``, also execute the query and include
                actual execution statistics.

        Returns:
            Dict with ``query``, ``plan``, ``estimated_cost``,
            ``estimated_rows``, ``explanation``, and optionally
            ``actual_stats``.
        """
        return await self._request(
            "POST",
            "/api/v2/query/explain",
            json_body={"query": query, "analyze": analyze},
        )

    # ------------------------------------------------------------------
    # Cluster, Storage & System
    # ------------------------------------------------------------------

    async def cluster_stats(self) -> dict[str, Any]:
        """Get cluster statistics.

        Returns:
            Dict with cluster-level metrics.
        """
        return await self._request("GET", "/api/v2/cluster/stats")

    async def storage_status(self) -> dict[str, Any]:
        """Get storage tier statistics.

        Returns:
            Dict with ``backend``, ``total_objects``, ``total_size_bytes``,
            ``hot_objects``, ``warm_objects``, and ``cold_objects``.
        """
        return await self._request("GET", "/api/v2/storage/status")

    async def storage_migrate(self) -> dict[str, Any]:
        """Manually trigger cold storage migration.

        Returns:
            Dict with ``status`` and ``migrated_objects``.
        """
        return await self._request("POST", "/api/v2/storage/migrate")

    async def system_info(self) -> dict[str, Any]:
        """Get system information.

        Returns:
            Dict with ``version``, ``rust_version``, ``mode``,
            ``num_shards``, ``max_nodes_per_shard``, etc.
        """
        return await self._request("GET", "/api/v2/system/info")
