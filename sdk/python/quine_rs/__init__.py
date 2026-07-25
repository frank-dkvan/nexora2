"""
Nexora-RS Python Driver SDK
==========================

A Python client library for the Nexora-RS streaming graph database,
supporting both synchronous and asynchronous usage patterns.

Quick Start (sync)::

    from nexora_rs import NexoraClient

    with NexoraClient("http://localhost:8080") as client:
        client.set_property("alice", "name", "Alice")
        results = client.query("MATCH (n) RETURN n LIMIT 10")

Async::

    import asyncio
    from nexora_rs import AsyncNexoraClient

    async def main():
        async with AsyncNexoraClient("http://localhost:8080") as client:
            await client.set_property("alice", "name", "Alice")
            results = await client.query("MATCH (n) RETURN n LIMIT 10")

    asyncio.run(main())
"""

from .async_client import AsyncNexoraClient
from .client import NexoraClient
from .exceptions import (
    AuthenticationError,
    ConnectionError,
    NodeNotFoundError,
    QueryError,
    NexoraError,
    TimeoutError,
)
from .streaming import CypherStreamSubscription
from .types import (
    Edge,
    Node,
    QueryResult,
    StandingQueryInfo,
    StandingQueryPattern,
)
from .websocket import StandingQuerySubscription

__version__ = "0.2.0"

__all__ = [
    # Clients
    "AsyncNexoraClient",
    "NexoraClient",
    "StandingQuerySubscription",
    "CypherStreamSubscription",
    # Types
    "Node",
    "Edge",
    "QueryResult",
    "StandingQueryInfo",
    "StandingQueryPattern",
    # Exceptions
    "NexoraError",
    "ConnectionError",
    "QueryError",
    "NodeNotFoundError",
    "AuthenticationError",
    "TimeoutError",
    # Metadata
    "__version__",
]
