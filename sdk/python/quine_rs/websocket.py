"""
WebSocket subscription client for Nexora-RS Standing Query results.

Connects to the ``/api/v2/ws/sq/{id}`` WebSocket endpoint to receive
real-time notifications when a Standing Query matches.

Example::

    import asyncio
    from nexora_rs import AsyncNexoraClient, StandingQuerySubscription

    async def main():
        async with AsyncNexoraClient("http://localhost:8080") as client:
            sq_id = await client.create_standing_query(
                {"type": "PropertyFilter", "key": "speed",
                 "condition": {"type": "GreaterThan", "value": 100}},
                name="fast_cars",
            )

            ws_url = "ws://localhost:8080"
            async with StandingQuerySubscription(ws_url, sq_id) as sub:
                async def on_result(data):
                    print(f"Match! count={data['match_count']}")

                await sub.subscribe(on_result)

    asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

import aiohttp

from .exceptions import (
    AuthenticationError,
    ConnectionError as NexoraConnectionError,
    NexoraError,
    TimeoutError as NexoraTimeoutError,
)

logger = logging.getLogger(__name__)

# Type alias for the async callback
ResultCallback = Callable[[dict[str, Any]], Awaitable[None]]


class StandingQuerySubscription:
    """Subscribe to Standing Query results via WebSocket.

    Args:
        ws_url: WebSocket base URL (e.g. ``ws://localhost:8080``).
        query_id: The Standing Query UUID to subscribe to.
        api_key: Optional Bearer token for authentication.
    """

    def __init__(
        self,
        ws_url: str,
        query_id: str,
        api_key: str | None = None,
    ) -> None:
        self.ws_url = ws_url.rstrip("/")
        self.query_id = query_id
        self.api_key = api_key
        self._session: aiohttp.ClientSession | None = None
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._running = False

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> StandingQuerySubscription:
        await self._connect()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.unsubscribe()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------

    async def _connect(self) -> None:
        """Establish the WebSocket connection."""
        if self._session is None or self._session.closed:
            headers: dict[str, str] = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            self._session = aiohttp.ClientSession()
        ws_url = f"{self.ws_url}/api/v2/ws/sq/{self.query_id}"
        try:
            self._ws = await self._session.ws_connect(ws_url, headers=headers)
        except aiohttp.ClientConnectorError as exc:
            raise NexoraConnectionError(
                f"Cannot connect to WebSocket {ws_url}: {exc}"
            ) from exc
        except aiohttp.WSServerHandshakeError as exc:
            if exc.status in (401, 403):
                raise AuthenticationError(
                    f"WebSocket auth failed: {exc}", status_code=exc.status
                ) from exc
            raise NexoraConnectionError(
                f"WebSocket handshake failed: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Subscribe / Unsubscribe
    # ------------------------------------------------------------------

    async def subscribe(self, on_result: ResultCallback) -> None:
        """Start receiving Standing Query results.

        Calls ``on_result`` for each message received from the server.
        This method blocks until :meth:`unsubscribe` is called, the
        connection closes, or an error occurs.

        Args:
            on_result: Async callback invoked with each result dict.
        """
        if self._ws is None:
            await self._connect()

        self._running = True
        assert self._ws is not None

        try:
            async for msg in self._ws:
                if not self._running:
                    break

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except json.JSONDecodeError:
                        logger.warning("Received non-JSON WebSocket message: %s", msg.data)
                        continue

                    try:
                        await on_result(data)
                    except Exception:
                        logger.exception("Error in on_result callback")

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise NexoraConnectionError(
                        f"WebSocket error: {self._ws.exception()}"
                    )
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                    break
        finally:
            self._running = False

    async def unsubscribe(self) -> None:
        """Stop receiving results and close the WebSocket connection."""
        self._running = False
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._session and not self._session.closed:
            await self._session.close()
        self._ws = None
        self._session = None

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        """Whether the subscription is currently active."""
        return self._running
