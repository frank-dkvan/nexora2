"""
Type definitions for the Nexora-RS Python SDK.

Provides dataclasses and TypedDicts used throughout the SDK for type-safe
interactions with the graph database.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, TypedDict


@dataclass
class Node:
    """Represents a graph node.

    Attributes:
        id: The node identifier (hex-encoded string used by the server).
        labels: List of labels attached to the node (e.g. ``["Person"]``).
        properties: Dictionary of property key-value pairs.
    """

    id: str
    labels: list[str] = field(default_factory=list)
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class Edge:
    """Represents a directed edge between two nodes.

    Attributes:
        from_id: Source node ID (hex-encoded).
        to_id: Target node ID (hex-encoded).
        edge_type: The edge type label (e.g. ``"KNOWS"``).
        properties: Dictionary of edge property key-value pairs.
    """

    from_id: str
    to_id: str
    edge_type: str
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryResult:
    """Result of a Cypher query execution.

    Attributes:
        columns: Column names returned by the query.
        rows: List of row dictionaries, keyed by column name.
        duration_ms: Query execution time in milliseconds (if reported).
    """

    columns: list[str]
    rows: list[dict[str, Any]]
    duration_ms: float = 0.0


@dataclass
class StandingQueryInfo:
    """Metadata for a registered Standing Query.

    Attributes:
        id: Unique identifier assigned by the server.
        name: Human-readable name.
        match_count: Number of times this query has matched.
    """

    id: str
    name: str
    match_count: int = 0


# ---------------------------------------------------------------------------
# Standing Query pattern TypedDicts
# ---------------------------------------------------------------------------

class FilterCondition(TypedDict, total=False):
    """A filter condition for a Standing Query pattern.

    Attributes:
        type: One of ``GreaterThan``, ``LessThan``, ``Equals``,
            ``Contains``, ``Exists``, ``IsNull``, ``IsNotNull``.
        value: Threshold value for comparison conditions.
    """

    type: str
    value: Any


class StandingQueryPattern(TypedDict, total=False):
    """Pattern specification for a Standing Query.

    For a ``PropertyFilter`` pattern:

    >>> pattern: StandingQueryPattern = {
    ...     "type": "PropertyFilter",
    ...     "key": "speed",
    ...     "condition": {"type": "GreaterThan", "value": 100},
    ... }

    For a ``LabelFilter`` pattern:

    >>> pattern: StandingQueryPattern = {
    ...     "type": "LabelFilter",
    ...     "labels": ["Person"],
    ... }

    Attributes:
        type: ``"PropertyFilter"`` or ``"LabelFilter"``.
        key: Property key to monitor (required for ``PropertyFilter``).
        condition: Filter condition (required for ``PropertyFilter``).
        labels: List of labels to match (required for ``LabelFilter``).
    """

    type: str
    key: str
    condition: FilterCondition
    labels: list[str]


# Direction literal type for edge queries
EdgeDirection = Literal["out", "in", "both"]
