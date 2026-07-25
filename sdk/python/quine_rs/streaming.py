"""WebSocket streaming clients for Nexora-RS.

Provides two subscription classes built on the ``websockets`` library:

* :class:`StandingQuerySubscription` — receive real-time notifications
  when a Standing Query matches.
* :class:`CypherStreamSubscription` — stream Cypher query results over
  a WebSocket connection.

Example (Standing Query)::

    import asyncio
    from nexora_rs import AsyncNexoraClient
    from nexora_rs.streaming import StandingQuerySubscription

    async def main():
        async with AsyncNexoraClient("http://localhost:8080") as client:
            sq_id = await client.create_standing_query(
                {"type": "PropertyFilter", "key": "speed",
                 "condition": {"type": "GreaterThan", "value": 100}},
                name="fast_alert",
            )
            sub = StandingQuerySubscription("ws://localhost:8080", sq_id)
            await sub.start(lambda data: print(f"Match! {data}"))
            # ... later
            await sub.stop()

    asyncio.run(main())

Example (Cypher Stream)::

    import asyncio
    from nexora_rs.streaming import CypherStreamSubscription

    async def main():
        sub = CypherStreamSubscription("ws://localhost:8080")
        await sub.start("MATCH (n) RETURN n LIMIT 10")
        async for row in sub:
            print(row)

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, AsyncIterator, Callable, Optional

import websockets
from websockets.asyncio.client import connect

from .exceptions import (
    ConnectionError as NexoraConnectionError,
    NexoraError,
)

logger = logging.getLogger(__name__)

# Type alias for the callback — can be sync or async.
ResultCallback = Callable[[dict[str, Any]], Any]


class StandingQuerySubscription:
    """Subscribe to Standing Query results via WebSocket.

    Connects to the ``/api/v2/ws/sq/{query_id}`` endpoint and receives
    real-time notifications whenever the Standing Query matches new data.

    Args:
        ws_url: WebSocket base URL (e.g. ``ws://localhost:8080``).
        query_id: The Standing Query UUID to subscribe to.
        api_key: Optional Bearer token for authentication.
    """

    def __init__(
        self,
        ws_url: str,
        query_id: str,
        api_key: Optional[str] = None,
    ) -> None:
        self.ws_url = ws_url.rstrip("/")
        self.query_id = query_id
        self.api_key = api_key
        self._ws = None
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._queue: asyncio.Queue[dict[str, Any]] | None = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> StandingQuerySubscription:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _build_url(self) -> str:
        return f"{self.ws_url}/api/v2/ws/sq/{self.query_id}"

    def _build_headers(self) -> dict[str, str] | None:
        if self.api_key:
            return {"Authorization": f"Bearer {self.api_key}"}
        return None

    # ------------------------------------------------------------------
    # Start / Stop
    # ------------------------------------------------------------------

    async def start(self, callback: ResultCallback) -> None:
        """Start receiving Standing Query results.

        Calls *callback* for each message received from the server.
        The callback may be a sync or async callable.

        This method runs until :meth:`stop` is called, the connection
        closes, or an error occurs.

        Args:
            callback: Function invoked with each result dict.
        """
        self._running = True
        url = self._build_url()
        headers = self._build_headers()

        try:
            async with connect(url, additional_headers=headers) as ws:
                self._ws = ws
                async for raw in ws:
                    if not self._running:
                        break
                    await self._handle_message(raw, callback)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            if self._running:
                logger.error("WebSocket error: %s", exc)
        finally:
            self._running = False
            self._ws = None

    async def stop(self) -> None:
        """Stop receiving results and close the WebSocket connection."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # ------------------------------------------------------------------
    # Async iterator
    # ------------------------------------------------------------------

    async def __aiter__(self) -> AsyncIterator[dict[str, Any]]:
        """Iterate over results as an async iterator.

        Yields each result dict as it arrives from the server.
        """
        url = self._build_url()
        headers = self._build_headers()
        self._running = True

        try:
            async with connect(url, additional_headers=headers) as ws:
                self._ws = ws
                async for raw in ws:
                    if not self._running:
                        break
                    data = self._parse_message(raw)
                    if data is not None:
                        yield data
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            if self._running:
                raise NexoraConnectionError(f"WebSocket error: {exc}") from exc
        finally:
            self._running = False
            self._ws = None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _handle_message(self, raw: str | bytes, callback: ResultCallback) -> None:
        """Parse a WebSocket message and invoke the callback."""
        data = self._parse_message(raw)
        if data is None:
            return
        try:
            result = callback(data)
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.exception("Error in subscription callback")

    @staticmethod
    def _parse_message(raw: str | bytes) -> dict[str, Any] | None:
        """Parse a raw WebSocket message into a dict."""
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Received non-JSON WebSocket message: %s", raw)
            return None
        if not isinstance(data, dict):
            return None
        return data

    @property
    def is_running(self) -> bool:
        """Whether the subscription is currently active."""
        return self._running


class CypherStreamSubscription:
    """Stream Cypher query results via WebSocket.

    Connects to the ``/api/v2/ws/query`` endpoint and sends Cypher
    queries, receiving results as streaming messages.

    Args:
        ws_url: WebSocket base URL (e.g. ``ws://localhost:8080``).
        api_key: Optional Bearer token for authentication.
    """

    def __init__(
        self,
        ws_url: str,
        api_key: Optional[str] = None,
    ) -> None:
        self.ws_url = ws_url.rstrip("/")
        self.api_key = api_key
        self._ws = None
        self._running = False
        self._task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> CypherStreamSubscription:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    def _build_url(self) -> str:
        return f"{self.ws_url}/api/v2/ws/query"

    def _build_headers(self) -> dict[str, str] | None:
        if self.api_key:
            return {"Authorization": f"Bearer {self.api_key}"}
        return None

    # ------------------------------------------------------------------
    # Start / Stop
    # ------------------------------------------------------------------

    async def start(
        self,
        cypher: str,
        callback: ResultCallback,
        query_id: str = "q",
    ) -> None:
        """Start streaming Cypher query results.

        Sends the query to the server and calls *callback* for each
        batch of results received.

        Args:
            cypher: Cypher query string to execute.
            callback: Function invoked with each result dict.
            query_id: Optional client-side query identifier.
        """
        self._running = True
        url = self._build_url()
        headers = self._build_headers()

        try:
            async with connect(url, additional_headers=headers) as ws:
                self._ws = ws
                # Send the query
                request = json.dumps({"query": cypher, "queryId": query_id})
                await ws.send(request)
                # Receive results
                async for raw in ws:
                    if not self._running:
                        break
                    await self._handle_message(raw, callback)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            if self._running:
                logger.error("WebSocket error: %s", exc)
        finally:
            self._running = False
            self._ws = None

    async def stop(self) -> None:
        """Stop the stream and close the WebSocket connection."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    # ------------------------------------------------------------------
    # Async iterator
    # ------------------------------------------------------------------

    async def stream(
        self,
        cypher: str,
        query_id: str = "q",
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream Cypher results as an async iterator.

        Args:
            cypher: Cypher query string to execute.
            query_id: Optional client-side query identifier.

        Yields:
            Each result message dict from the server.
        """
        url = self._build_url()
        headers = self._build_headers()
        self._running = True

        try:
            async with connect(url, additional_headers=headers) as ws:
                self._ws = ws
                request = json.dumps({"query": cypher, "queryId": query_id})
                await ws.send(request)
                async for raw in ws:
                    if not self._running:
                        break
                    data = self._parse_message(raw)
                    if data is not None:
                        yield data
                        # Stop after QueryFinished
                        if data.get("type") == "QueryFinished":
                            break
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            if self._running:
                raise NexoraConnectionError(f"WebSocket error: {exc}") from exc
        finally:
            self._running = False
            self._ws = None

    def __aiter__(self) -> AsyncIterator[dict[str, Any]]:  # type: ignore[override]
        """Direct iteration is not supported.

        Use :meth:`stream` with a Cypher query string instead::

            async for row in sub.stream("MATCH (n) RETURN n"):
                ...
        """
        raise TypeError(
            "Use sub.stream(cypher) for async iteration "
            "instead of iterating over the subscription directly."
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _handle_message(self, raw: str | bytes, callback: ResultCallback) -> None:
        """Parse a WebSocket message and invoke the callback."""
        data = self._parse_message(raw)
        if data is None:
            return
        try:
            result = callback(data)
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.exception("Error in stream callback")
        # Stop on QueryFinished
        if isinstance(data, dict) and data.get("type") == "QueryFinished":
            self._running = False

    @staticmethod
    def _parse_message(raw: str | bytes) -> dict[str, Any] | None:
        """Parse a raw WebSocket message into a dict."""
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            logger.warning("Received non-JSON WebSocket message: %s", raw)
            return None
        if not isinstance(data, dict):
            return None
        return data

    @property
    def is_running(self) -> bool:
        """Whether the stream is currently active."""
        return self._running
