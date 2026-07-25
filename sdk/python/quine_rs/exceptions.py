"""
Exception hierarchy for the Nexora-RS Python SDK.

All exceptions raised by the SDK derive from :class:`NexoraError`, allowing
callers to catch every SDK-specific error with a single ``except`` clause.
"""

from __future__ import annotations

from typing import Any, Optional


class NexoraError(Exception):
    """Base exception for all Nexora-RS SDK errors.

    Attributes:
        message: Human-readable error description.
        status_code: HTTP status code from the server response, if applicable.
        response_body: Raw response body from the server, if available.
    """

    def __init__(
        self,
        message: str = "",
        *,
        status_code: Optional[int] = None,
        response_body: Any = None,
    ) -> None:
        self.message = message
        self.status_code = status_code
        self.response_body = response_body
        detail = f" (HTTP {status_code})" if status_code else ""
        super().__init__(f"{message}{detail}")


class ConnectionError(NexoraError):
    """Raised when the SDK cannot connect to the Nexora-RS server.

    This typically indicates a network issue: the server is not running,
    the URL is wrong, or a firewall is blocking the connection.
    """


class QueryError(NexoraError):
    """Raised when a Cypher query fails on the server side.

    The server returned an error response, typically HTTP 400 Bad Request,
    indicating a malformed query or semantic error.
    """


class NodeNotFoundError(NexoraError):
    """Raised when a node lookup fails because the node does not exist.

    The server returned HTTP 404 or the property response indicated
    ``not_found: true``.
    """


class AuthenticationError(NexoraError):
    """Raised when authentication fails.

    The server returned HTTP 401 Unauthorized or HTTP 403 Forbidden,
    indicating an invalid or missing API key / auth token.
    """


class TimeoutError(NexoraError):
    """Raised when a request exceeds the configured timeout.

    The ``timeout`` parameter on :class:`~nexora_rs.async_client.AsyncNexoraClient`
    controls how long each request may take before this error is raised.
    """
