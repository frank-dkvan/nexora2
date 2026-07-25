"""
Synchronous client for the Nexora-RS streaming graph database.

A thin wrapper around :class:`~nexora_rs.async_client.AsyncNexoraClient` that
exposes a synchronous (blocking) API.  Internally, an event loop is created
on demand to drive the async client.

Example::

    from nexora_rs import NexoraClient

    client = NexoraClient("http://localhost:8080")
    client.set_property("alice", "name", "Alice")
    name = client.get_property("alice", "name")
    print(name)  # "Alice"

    results = client.query("MATCH (n) RETURN n LIMIT 10")
    for row in results:
        print(row)

    client.close()
"""

from __future__ import annotations

import asyncio
from typing import Any

from .async_client import AsyncNexoraClient
from .exceptions import NexoraError
from .types import StandingQueryPattern


class NexoraClient:
    """Synchronous client for the Nexora-RS graph database.

    Args:
        base_url: Base URL of the Nexora-RS server.
        api_key: Optional Bearer token for authentication.
        timeout: Default total request timeout in seconds.
        max_retries: Maximum number of retries for failed requests (default 3).
        backoff_factor: Multiplier for exponential backoff between retries
            (default 0.5).
        connect_timeout: Connection-level timeout in seconds.
        read_timeout: Read-level timeout in seconds.
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
        self._async = AsyncNexoraClient(
            base_url,
            api_key,
            timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._owns_loop = False

    # ------------------------------------------------------------------
    # Event loop management
    # ------------------------------------------------------------------

    def _get_loop(self) -> asyncio.AbstractEventLoop:
        """Get or create an event loop for async operations."""
        if self._loop is None or self._loop.is_closed():
            try:
                self._loop = asyncio.get_event_loop()
                if self._loop.is_closed():
                    raise RuntimeError("loop closed")
            except RuntimeError:
                self._loop = asyncio.new_event_loop()
                self._owns_loop = True
        return self._loop

    def _run(self, coro: Any) -> Any:
        """Run a coroutine on the event loop (blocking)."""
        loop = self._get_loop()
        if loop.is_running():
            # If we're already inside an event loop, use a thread
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        return loop.run_until_complete(coro)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> NexoraClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def close(self) -> None:
        """Close the client and release resources."""
        if self._loop and not self._loop.is_closed():
            try:
                self._run(self._async.close())
            except Exception:
                pass
            if self._owns_loop:
                self._loop.close()

    # ------------------------------------------------------------------
    # Cypher queries
    # ------------------------------------------------------------------

    def query(
        self,
        cypher: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a Cypher query and return rows as dictionaries.

        Args:
            cypher: Cypher query string.
            params: Optional query parameters.

        Returns:
            List of row dictionaries keyed by column name.
        """
        return self._run(self._async.query(cypher, params))

    def query_one(
        self,
        cypher: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Execute a Cypher query and return the first row, or ``None``."""
        return self._run(self._async.query_one(cypher, params))

    def cypher(
        self,
        cypher: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a Cypher query (alias for :meth:`query`).

        Args:
            cypher: Cypher query string.
            params: Optional query parameters.

        Returns:
            List of row dictionaries keyed by column name.
        """
        return self._run(self._async.cypher(cypher, params))

    # ------------------------------------------------------------------
    # Node operations
    # ------------------------------------------------------------------

    def create_node(self, labels: list[str], properties: dict[str, Any]) -> str:
        """Create a node with labels and properties.

        Args:
            labels: List of labels.
            properties: Property dict (must contain ``id``).

        Returns:
            The node ID.
        """
        return self._run(self._async.create_node(labels, properties))

    def get_node(self, node_id: str) -> dict[str, Any] | None:
        """Retrieve a node by ID."""
        return self._run(self._async.get_node(node_id))

    def set_property(self, node_id: str, key: str, value: Any) -> None:
        """Set a property on a node."""
        return self._run(self._async.set_property(node_id, key, value))

    def get_property(self, node_id: str, key: str) -> Any:
        """Get a property value from a node."""
        return self._run(self._async.get_property(node_id, key))

    def delete_node(self, node_id: str) -> None:
        """Delete a node and all its edges."""
        return self._run(self._async.delete_node(node_id))

    # ------------------------------------------------------------------
    # Edge operations
    # ------------------------------------------------------------------

    def add_edge(
        self,
        from_id: str,
        to_id: str,
        edge_type: str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Create a directed edge between two nodes."""
        return self._run(self._async.add_edge(from_id, to_id, edge_type, properties))

    def get_edges(
        self,
        node_id: str,
        direction: str = "both",
    ) -> list[dict[str, Any]]:
        """Get edges connected to a node."""
        return self._run(self._async.get_edges(node_id, direction))

    def delete_edge(self, from_id: str, to_id: str, edge_type: str) -> None:
        """Delete an edge between two nodes."""
        return self._run(self._async.delete_edge(from_id, to_id, edge_type))

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def batch_create_nodes(self, nodes: list[dict[str, Any]]) -> list[str]:
        """Create multiple nodes."""
        return self._run(self._async.batch_create_nodes(nodes))

    def batch_query(self, queries: list[str]) -> list[list[dict[str, Any]]]:
        """Execute multiple Cypher queries."""
        return self._run(self._async.batch_query(queries))

    # ------------------------------------------------------------------
    # Standing Queries
    # ------------------------------------------------------------------

    def create_standing_query(
        self,
        pattern: StandingQueryPattern,
        name: str | None = None,
    ) -> str:
        """Register a new Standing Query.

        Returns the query ID.
        """
        return self._run(self._async.create_standing_query(pattern, name))

    def list_standing_queries(self) -> list[dict[str, Any]]:
        """List all registered Standing Queries."""
        return self._run(self._async.list_standing_queries())

    def delete_standing_query(self, query_id: str) -> None:
        """Delete a Standing Query by ID."""
        return self._run(self._async.delete_standing_query(query_id))

    # ------------------------------------------------------------------
    # Health & Stats
    # ------------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        """Check server health."""
        return self._run(self._async.health())

    def stats(self) -> dict[str, Any]:
        """Get server statistics."""
        return self._run(self._async.stats())

    # ------------------------------------------------------------------
    # Convenience: ingest
    # ------------------------------------------------------------------

    def ingest_file(self, path: str, id_field: str = "id") -> dict[str, Any]:
        """Start a file ingest pipeline."""
        return self._run(self._async.ingest_file(path, id_field))

    # ------------------------------------------------------------------
    # Vector operations
    # ------------------------------------------------------------------

    def vector_index(self, qid: str, vector: list[float]) -> dict[str, Any]:
        """Index a vector for a node.

        Args:
            qid: Node identifier string (will be hex-encoded).
            vector: Embedding vector (list of floats).

        Returns:
            Dict with ``status``, ``qid``, and ``index_size``.
        """
        return self._run(self._async.vector_index(qid, vector))

    def vector_search(self, vector: list[float], k: int = 10) -> dict[str, Any]:
        """Search for k-nearest neighbors of a vector.

        Args:
            vector: Query embedding vector.
            k: Number of nearest neighbors to return (default 10).

        Returns:
            Dict with ``query``, ``k``, and ``neighbors``.
        """
        return self._run(self._async.vector_search(vector, k))

    def vector_get(self, qid: str) -> dict[str, Any] | None:
        """Get the vector associated with a node.

        Args:
            qid: Node identifier string (will be hex-encoded).

        Returns:
            Dict with ``qid`` and ``vector``, or ``None`` if not found.
        """
        return self._run(self._async.vector_get(qid))

    def vector_delete(self, qid: str) -> dict[str, Any]:
        """Delete the vector associated with a node.

        Args:
            qid: Node identifier string (will be hex-encoded).

        Returns:
            Dict with ``status``, ``qid``, and ``index_size``.
        """
        return self._run(self._async.vector_delete(qid))

    # ------------------------------------------------------------------
    # Materialized Views
    # ------------------------------------------------------------------

    def create_materialized_view(self, definition: dict[str, Any]) -> dict[str, Any]:
        """Create a new materialized view.

        Args:
            definition: View definition dict with ``name``, ``query``,
                optional ``refresh_mode``, and optional ``schema``.

        Returns:
            Dict with ``view_id``, ``name``, and ``status``.
        """
        return self._run(self._async.create_materialized_view(definition))

    def list_materialized_views(self) -> list[dict[str, Any]]:
        """List all materialized views.

        Returns:
            List of view dicts.
        """
        return self._run(self._async.list_materialized_views())

    def get_materialized_view(self, view_id: str) -> dict[str, Any]:
        """Get a materialized view definition by ID.

        Args:
            view_id: The materialized view identifier.

        Returns:
            Dict with view details.
        """
        return self._run(self._async.get_materialized_view(view_id))

    def delete_materialized_view(self, view_id: str) -> dict[str, Any]:
        """Delete a materialized view by ID.

        Args:
            view_id: The materialized view identifier.

        Returns:
            Dict with ``status`` and ``view_id``.
        """
        return self._run(self._async.delete_materialized_view(view_id))

    def query_materialized_view(
        self,
        view_id: str,
        limit: int | None = None,
        column: str | None = None,
        value: str | None = None,
    ) -> dict[str, Any]:
        """Query data from a materialized view.

        Args:
            view_id: The materialized view identifier.
            limit: Optional maximum number of rows.
            column: Optional column name for filtered query.
            value: Optional value to match (requires *column*).

        Returns:
            Dict with ``view_id``, ``rows``, and ``count``.
        """
        return self._run(
            self._async.query_materialized_view(view_id, limit, column, value)
        )

    def refresh_materialized_view(self, view_id: str) -> dict[str, Any]:
        """Manually refresh a materialized view.

        Args:
            view_id: The materialized view identifier.

        Returns:
            Dict with ``status``, ``view_id``, and ``query``.
        """
        return self._run(self._async.refresh_materialized_view(view_id))

    # ------------------------------------------------------------------
    # User-Defined Functions (UDF)
    # ------------------------------------------------------------------

    def register_udf(
        self,
        name: str,
        code: str,
        language: str = "native",
    ) -> dict[str, Any]:
        """Register a new user-defined function.

        Args:
            name: UDF name.
            code: Function code (format depends on *language*).
            language: ``"native"`` (default), ``"wasm"``, or ``"python"``.

        Returns:
            Dict with ``status``, ``name``, and ``language``.
        """
        return self._run(self._async.register_udf(name, code, language))

    def list_udfs(self) -> list[dict[str, Any]]:
        """List all registered UDFs.

        Returns:
            List of dicts with ``name`` and ``language``.
        """
        return self._run(self._async.list_udfs())

    def execute_udf(self, name: str, input: Any) -> dict[str, Any]:
        """Execute a UDF by name.

        Args:
            name: UDF name.
            input: Input value (any JSON-serializable value).

        Returns:
            Dict with ``result`` and ``name``.
        """
        return self._run(self._async.execute_udf(name, input))

    def delete_udf(self, name: str) -> dict[str, Any]:
        """Delete a UDF by name.

        Args:
            name: UDF name.

        Returns:
            Dict with ``status`` and ``name``.
        """
        return self._run(self._async.delete_udf(name))

    # ------------------------------------------------------------------
    # Recipes
    # ------------------------------------------------------------------

    def create_recipe(self, recipe: dict[str, Any]) -> dict[str, Any]:
        """Create a new recipe.

        Args:
            recipe: Recipe definition dict with ``name``, optional
                ``description``, ``steps``, and optional ``trigger``.

        Returns:
            Dict with ``status``, ``name``, and ``recipe_count``.
        """
        return self._run(self._async.create_recipe(recipe))

    def list_recipes(self) -> list[dict[str, Any]]:
        """List all recipes.

        Returns:
            List of recipe summary dicts.
        """
        return self._run(self._async.list_recipes())

    def get_recipe(self, name: str) -> dict[str, Any]:
        """Get a recipe by name.

        Args:
            name: Recipe name.

        Returns:
            Dict with recipe details.
        """
        return self._run(self._async.get_recipe(name))

    def delete_recipe(self, name: str) -> dict[str, Any]:
        """Delete a recipe by name.

        Args:
            name: Recipe name.

        Returns:
            Dict with ``status`` and ``name``.
        """
        return self._run(self._async.delete_recipe(name))

    def execute_recipe(self, name: str) -> dict[str, Any]:
        """Execute a recipe by name.

        Args:
            name: Recipe name.

        Returns:
            Dict with ``run_id``, ``recipe``, ``status``, and ``result``.
        """
        return self._run(self._async.execute_recipe(name))

    # ------------------------------------------------------------------
    # SQL & Explain
    # ------------------------------------------------------------------

    def sql(self, query: str) -> dict[str, Any]:
        """Execute a SQL query.

        Args:
            query: SQL query string.

        Returns:
            Dict with ``columns``, ``rows``, ``row_count``, etc.
        """
        return self._run(self._async.sql(query))

    def explain(self, query: str, analyze: bool = False) -> dict[str, Any]:
        """Generate an execution plan for a Cypher query.

        Args:
            query: Cypher query string.
            analyze: If ``True``, also execute and include actual stats.

        Returns:
            Dict with ``plan``, ``estimated_cost``, ``explanation``, etc.
        """
        return self._run(self._async.explain(query, analyze))

    # ------------------------------------------------------------------
    # Cluster, Storage & System
    # ------------------------------------------------------------------

    def cluster_stats(self) -> dict[str, Any]:
        """Get cluster statistics.

        Returns:
            Dict with cluster-level metrics.
        """
        return self._run(self._async.cluster_stats())

    def storage_status(self) -> dict[str, Any]:
        """Get storage tier statistics.

        Returns:
            Dict with ``backend``, ``total_objects``, tier counts, etc.
        """
        return self._run(self._async.storage_status())

    def storage_migrate(self) -> dict[str, Any]:
        """Manually trigger cold storage migration.

        Returns:
            Dict with ``status`` and ``migrated_objects``.
        """
        return self._run(self._async.storage_migrate())

    def system_info(self) -> dict[str, Any]:
        """Get system information.

        Returns:
            Dict with ``version``, ``num_shards``, etc.
        """
        return self._run(self._async.system_info())
