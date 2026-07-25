import React, { useEffect, useRef, useState, useCallback } from 'react';
import { DataSet, Network, Node, Edge } from 'vis-network/standalone';
import { api } from '../lib';
import { EdgeResponse, NodePropertyResponse, EdgeItem } from '../types';

function hexEncode(s: string): string {
  let r = '';
  for (let i = 0; i < s.length; i++) r += s.charCodeAt(i).toString(16).padStart(2, '0');
  return r;
}

function hexDecode(hex: string): string {
  try {
    let r = '';
    for (let i = 0; i < hex.length; i += 2) r += String.fromCharCode(parseInt(hex.substring(i, i + 2), 16));
    return r;
  } catch {
    return hex;
  }
}

const EDGE_COLORS: Record<string, string> = {
  KNOWS: '#4ade80',
  REPORTS_TO: '#f97316',
  LOCATED_IN: '#38bdf8',
  MONITORED_BY: '#a78bfa',
  DEFAULT: '#94a3b8',
};

const NODE_LABEL_KEYS = ['name', 'id', 'label', 'type', 'status'];

function findLabel(props: Record<string, unknown>, qidHex: string): string {
  for (const k of NODE_LABEL_KEYS) {
    if (props[k] !== undefined && props[k] !== null) return String(props[k]);
  }
  return hexDecode(qidHex).substring(0, 20) || qidHex.substring(0, 8);
}

function findColor(props: Record<string, unknown>): string {
  if (props.status === 'active') return '#4ade80';
  if (props.status === 'inactive') return '#f87171';
  if (props.status === 'warning') return '#fbbf24';
  if (props.speed !== undefined && Number(props.speed) > 100) return '#f97316';
  if (props.type === 'Person') return '#38bdf8';
  return '#a78bfa';
}

interface VisNodeInfo {
  id: string;          // hex-encoded NexoraId
  label: string;
  color: string;
  expanded: boolean;
  properties: Record<string, unknown>;
}

export const GraphBrowserPage: React.FC = () => {
  const containerRef = useRef<HTMLDivElement>(null);
  const networkRef = useRef<Network | null>(null);
  const nodesRef = useRef<DataSet<Node>>(new DataSet());
  const edgesRef = useRef<DataSet<Edge>>(new DataSet());
  const visNodesRef = useRef<Map<string, VisNodeInfo>>(new Map());

  const [nodeIdInput, setNodeIdInput] = useState('');
  const [selectedNode, setSelectedNode] = useState<VisNodeInfo | null>(null);
  const [contextMenu, setContextMenu] = useState<{ x: number; y: number; nodeId: string } | null>(null);
  const [history, setHistory] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Set property form
  const [propKey, setPropKey] = useState('');
  const [propVal, setPropVal] = useState('');
  // Add edge form
  const [edgeType, setEdgeType] = useState('KNOWS');
  const [edgeTarget, setEdgeTarget] = useState('');

  // Advanced search panel state
  const [searchMode, setSearchMode] = useState<'quick' | 'filter' | 'cypher' | 'sql'>('quick');
  const [searchExpanded, setSearchExpanded] = useState(false);
  const [quickSearch, setQuickSearch] = useState('');
  // Filter mode
  const [filterKey, setFilterKey] = useState('name');
  const [filterOp, setFilterOp] = useState<'equals' | 'contains' | 'gt' | 'lt'>('contains');
  const [filterValue, setFilterValue] = useState('');
  // Script mode
  const [cypherQuery, setCypherQuery] = useState('MATCH (n) WHERE n.name CONTAINS $term RETURN n LIMIT 20');
  const [sqlQuery, setSqlQuery] = useState('SELECT * FROM nodes WHERE name LIKE \'%term%\' LIMIT 20');
  const [searchResults, setSearchResults] = useState<Array<{ id: string; label: string; props: Record<string, unknown> }>>([]);
  const [searching, setSearching] = useState(false);

  // Format ID for display
  const displayId = (hexId: string) => {
    const decoded = hexDecode(hexId);
    return decoded.length < hexId.length ? hexId : hexId.substring(0, 12);
  };

  // Initialize vis-network
  useEffect(() => {
    if (!containerRef.current) return;

    const options = {
      physics: {
        solver: 'forceAtlas2Based' as const,
        forceAtlas2Based: {
          gravitationalConstant: -30,
          centralGravity: 0.005,
          springLength: 100,
          springConstant: 0.08,
          damping: 0.4,
          avoidOverlap: 0.5,
        },
        stabilization: { iterations: 100 },
      },
      interaction: {
        hover: true,
        tooltipDelay: 200,
        navigationButtons: true,
        keyboard: true,
      },
      edges: {
        smooth: { enabled: true, type: 'continuous' as const, roundness: 0.3 },
        arrows: { to: { enabled: true, scaleFactor: 0.7 } },
        font: { size: 9, color: '#94a3b8' as string },
      },
      nodes: {
        shape: 'dot' as const,
        size: 14,
        font: { size: 11, color: '#e2e8f0', face: 'sans-serif' },
        borderWidth: 2,
        shadow: { enabled: true, size: 6 },
      },
    };

    const network = new Network(containerRef.current, {
      nodes: nodesRef.current,
      edges: edgesRef.current,
    }, options);

    networkRef.current = network;

    // Double-click: expand node
    network.on('doubleClick', (params) => {
      if (params.nodes.length > 0) {
        const nodeId = params.nodes[0] as string;
        loadNodeEdges(nodeId);
      }
    });

    // Right-click: context menu
    network.on('oncontext', (params) => {
      params.event.preventDefault();
      if (params.nodes.length > 0) {
        const nodeId = params.nodes[0] as string;
        setContextMenu({ x: params.event.pageX, y: params.event.pageY, nodeId });
      } else {
        setContextMenu(null);
      }
    });

    // Click: select node
    network.on('click', (params) => {
      if (params.nodes.length > 0) {
        const nodeId = params.nodes[0] as string;
        const info = visNodesRef.current.get(nodeId);
        if (info) setSelectedNode(info);
      }
      setContextMenu(null);
    });

    // Click on blank area: deselect
    network.on('deselectNode', () => {
      // Keep selectedNode a bit longer for UX
    });

    // Cleanup
    return () => {
      network.destroy();
    };
  }, []);

  // Load a node and its edges
  const loadNodeAndEdges = useCallback(async (rawId: string) => {
    const hexId = hexEncode(rawId);
    setLoading(true);
    setError(null);
    try {
      // Fetch edges
      const edgeRes = await api.get<EdgeResponse>(`/api/v2/graph/node/${hexId}/edges`);
      const edges: EdgeItem[] = edgeRes.edges || [];

      // Add source node if new
      if (!visNodesRef.current.has(hexId)) {
        const nodeInfo = await api.get<NodePropertyResponse>(`/api/v2/graph/node/${hexId}/property/name`).catch(() => ({ value: undefined }));
        // Try multiple property keys
        const props: Record<string, unknown> = {};
        for (const k of ['name', 'type', 'status', 'speed', 'label']) {
          const r = await api.get<NodePropertyResponse>(`/api/v2/graph/node/${hexId}/property/${k}`).catch(() => ({ value: undefined } as unknown as NodePropertyResponse));
          if (r && r.value !== undefined && r.value !== null) props[k] = r.value;
        }

        const visNode: VisNodeInfo = {
          id: hexId,
          label: findLabel(props, hexId),
          color: findColor(props),
          expanded: true,
          properties: props,
        };
        visNodesRef.current.set(hexId, visNode);

        nodesRef.current.update({
          id: hexId,
          label: visNode.label,
          title: `<b>${visNode.label}</b><br/>${Object.entries(props).map(([k, v]) => `${k}: ${JSON.stringify(v)}`).join('<br/>')}`,
          color: { background: visNode.color, border: '#1e293b' },
          borderWidth: selectedNode?.id === hexId ? 3 : 2,
        });
      }

      // Add target nodes + edges
      for (const edge of edges) {
        if (!visNodesRef.current.has(edge.other)) {
          // Lightweight target node
          const props: Record<string, unknown> = {};
          for (const k of ['name', 'type', 'status']) {
            const r = await api.get<NodePropertyResponse>(`/api/v2/graph/node/${edge.other}/property/${k}`).catch(() => ({ value: undefined } as unknown as NodePropertyResponse));
            if (r && r.value !== undefined && r.value !== null) props[k] = r.value;
          }
          const visNode: VisNodeInfo = {
            id: edge.other,
            label: findLabel(props, edge.other),
            color: findColor(props),
            expanded: false,
            properties: props,
          };
          visNodesRef.current.set(edge.other, visNode);
          nodesRef.current.add({
            id: edge.other,
            label: visNode.label,
            title: `<b>${visNode.label}</b><br/>${Object.entries(props).map(([k, v]) => `${k}: ${JSON.stringify(v)}`).join('<br/>')}`,
            color: { background: visNode.color, border: '#1e293b' },
            size: 10,
          });
        }

        // Add edge
        const edgeColor = EDGE_COLORS[edge.edge_type] || EDGE_COLORS.DEFAULT;
        const isOut = edge.direction === 'out';
        edgesRef.current.add({
          id: `${hexId}-${edge.edge_type}-${edge.other}`,
          from: isOut ? hexId : edge.other,
          to: isOut ? edge.other : hexId,
          label: edge.edge_type,
          color: { color: edgeColor, opacity: 0.7 },
          arrows: { to: { enabled: true } },
        });
      }

      setHistory((prev) => [...prev, rawId].slice(-20));
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
  }, [selectedNode]);

  // Load edges for an already-visible node
  const loadNodeEdges = useCallback(async (hexId: string) => {
    setLoading(true);
    try {
      const info = visNodesRef.current.get(hexId);
      if (info) {
        info.expanded = true;
        setSelectedNode(info);
      }

      const edgeRes = await api.get<EdgeResponse>(`/api/v2/graph/node/${hexId}/edges`);
      const edges: EdgeItem[] = edgeRes.edges || [];

      for (const edge of edges) {
        if (!visNodesRef.current.has(edge.other)) {
          const props: Record<string, unknown> = {};
          for (const k of ['name', 'type', 'status']) {
            const r = await api.get<NodePropertyResponse>(`/api/v2/graph/node/${edge.other}/property/${k}`).catch(() => ({ value: undefined } as unknown as NodePropertyResponse));
            if (r && r.value !== undefined && r.value !== null) props[k] = r.value;
          }
          const visNode: VisNodeInfo = {
            id: edge.other,
            label: findLabel(props, edge.other),
            color: findColor(props),
            expanded: false,
            properties: props,
          };
          visNodesRef.current.set(edge.other, visNode);
          nodesRef.current.add({
            id: edge.other,
            label: visNode.label,
            title: `<b>${visNode.label}</b><br/>${Object.entries(props).map(([k, v]) => `${k}: ${JSON.stringify(v)}`).join('<br/>')}`,
            color: { background: visNode.color, border: '#1e293b' },
            size: 10,
          });
        }

        const edgeColor = EDGE_COLORS[edge.edge_type] || EDGE_COLORS.DEFAULT;
        const edgeId = `${hexId}-${edge.edge_type}-${edge.other}`;
        if (!edgesRef.current.get(edgeId)) {
          const isOut = edge.direction === 'out';
          edgesRef.current.add({
            id: edgeId,
            from: isOut ? hexId : edge.other,
            to: isOut ? edge.other : hexId,
            label: edge.edge_type,
            color: { color: edgeColor, opacity: 0.7 },
            arrows: { to: { enabled: true } },
          });
        }
      }
    } catch (e) {
      // Ignore edge load errors
    } finally {
      setLoading(false);
    }
  }, []);

  // Search and load a new node
  const searchNode = () => {
    const id = nodeIdInput.trim();
    if (!id) return;
    loadNodeAndEdges(id);
    setNodeIdInput('');
  };

  // Advanced search execution
  const executeSearch = async () => {
    setSearching(true);
    setSearchResults([]);
    try {
      if (searchMode === 'quick') {
        // Quick search: try to match against common properties
        const term = quickSearch.trim();
        if (!term) return;

        // For demo: search by loading the node directly if it looks like an ID
        if (term.length > 3) {
          try {
            await loadNodeAndEdges(term);
          } catch {
            setError(`No node found matching "${term}"`);
          }
        }
      } else if (searchMode === 'filter') {
        // Filter search: property-based filtering
        // This would need a backend endpoint like /api/v2/graph/search
        const response = await api.post('/api/v2/graph/search', {
          property: filterKey,
          operator: filterOp,
          value: filterValue,
          limit: 20,
        });
        // Assume response: { nodes: [{ id: hex, properties: {...} }] }
        const nodes = (response as any).nodes || [];
        setSearchResults(nodes.map((n: any) => ({
          id: n.id,
          label: findLabel(n.properties || {}, n.id),
          props: n.properties || {},
        })));
      } else if (searchMode === 'cypher') {
        // Cypher query - use correct endpoint
        const result = await api.post('/api/v2/query/cypher', { query: cypherQuery });
        // Backend returns: { columns: [...], rows: [[{...}], [{...}]], ... }
        const resultData = result as any;
        const rows = resultData.rows || [];

        if (rows.length === 0) {
          setError('No results found');
          return;
        }

        // Each row is an array of values - extract first element (usually the node)
        setSearchResults(rows.map((row: any[], idx: number) => {
          // row[0] is the first column value (e.g., the node from RETURN n)
          const nodeData = Array.isArray(row) ? row[0] : row;

          if (!nodeData || typeof nodeData !== 'object') {
            return {
              id: `node-${idx}`,
              label: String(nodeData || 'Unknown'),
              props: {},
            };
          }

          // nodeData has { id: hex, label: "Node", name: hex, ...properties }
          const props: Record<string, unknown> = { ...nodeData };
          delete props.id;
          delete props.label;

          return {
            id: nodeData.id || `node-${idx}`,
            label: nodeData.name || nodeData.label || nodeData.id || 'Unknown',
            props: props,
          };
        }));
      } else if (searchMode === 'sql') {
        // SQL query - use correct endpoint
        const result = await api.post('/api/v2/query/sql', { query: sqlQuery });
        const resultData = result as any;
        const rows = resultData.rows || [];

        if (rows.length === 0) {
          setError('No results found');
          return;
        }

        // Same format as Cypher: rows is array of arrays
        setSearchResults(rows.map((row: any[], idx: number) => {
          const nodeData = Array.isArray(row) ? row[0] : row;

          if (!nodeData || typeof nodeData !== 'object') {
            return {
              id: `node-${idx}`,
              label: String(nodeData || 'Unknown'),
              props: {},
            };
          }

          const props: Record<string, unknown> = { ...nodeData };
          delete props.id;
          delete props.label;

          return {
            id: nodeData.id || `node-${idx}`,
            label: nodeData.name || nodeData.label || nodeData.id || 'Unknown',
            props: props,
          };
        }));
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Search failed');
    } finally {
      setSearching(false);
    }
  };

  // Load a node from search results
  const loadFromSearchResult = (nodeId: string) => {
    loadNodeAndEdges(hexDecode(nodeId));
    setSearchExpanded(false);
  };

  // Set property on selected node
  const setProperty = async () => {
    if (!selectedNode || !propKey.trim()) return;
    let val: unknown = propVal.trim();
    if (!isNaN(Number(val))) val = Number(val);
    await api.put(`/api/v2/graph/node/${selectedNode.id}/property/${propKey.trim()}`, { value: val });

    // Refresh node properties
    const props: Record<string, unknown> = { ...selectedNode.properties };
    props[propKey.trim()] = val;
    const updated: VisNodeInfo = { ...selectedNode, properties: props, label: findLabel(props, selectedNode.id), color: findColor(props) };
    visNodesRef.current.set(selectedNode.id, updated);
    setSelectedNode(updated);
    nodesRef.current.update({
      id: selectedNode.id,
      label: updated.label,
      color: { background: updated.color, border: '#1e293b' },
    });
    setPropVal('');
  };

  // Add edge from selected node
  const addEdge = async () => {
    if (!selectedNode || !edgeType.trim() || !edgeTarget.trim()) return;
    const targetHex = hexEncode(edgeTarget.trim());
    await api.post(`/api/v2/graph/node/${selectedNode.id}/edges`, {
      edge_type: edgeType.trim(),
      target: targetHex,
      direction: 'out',
    });

    // Add the target node + edge immediately
    if (!visNodesRef.current.has(targetHex)) {
      const visNode: VisNodeInfo = {
        id: targetHex,
        label: hexDecode(targetHex).substring(0, 20) || displayId(targetHex),
        color: '#a78bfa',
        expanded: false,
        properties: {},
      };
      visNodesRef.current.set(targetHex, visNode);
      nodesRef.current.add({
        id: targetHex,
        label: visNode.label,
        color: { background: visNode.color, border: '#1e293b' },
        size: 10,
      });
    }

    const edgeColor = EDGE_COLORS[edgeType.trim()] || EDGE_COLORS.DEFAULT;
    const edgeId = `${selectedNode.id}-${edgeType.trim()}-${targetHex}`;
    edgesRef.current.add({
      id: edgeId,
      from: selectedNode.id,
      to: targetHex,
      label: edgeType.trim(),
      color: { color: edgeColor, opacity: 0.7 },
      arrows: { to: { enabled: true } },
    });
    setEdgeTarget('');
  };

  // Context menu actions
  const focusNode = (nodeId: string) => {
    networkRef.current?.focus(nodeId, { scale: 1.5 });
    setContextMenu(null);
  };

  const expandFromMenu = (nodeId: string) => {
    loadNodeEdges(nodeId);
    setContextMenu(null);
  };

  const removeNode = (nodeId: string) => {
    visNodesRef.current.delete(nodeId);
    nodesRef.current.remove(nodeId);
    edgesRef.current.remove(
      edgesRef.current.get().filter((e) => (e.from === nodeId || e.to === nodeId) as boolean).map((e) => e.id as string)
    );
    if (selectedNode?.id === nodeId) setSelectedNode(null);
    setContextMenu(null);
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 52px)' }}>
      {/* Toolbar */}
      <div className="border-bottom px-3 py-2 d-flex align-items-center gap-2 bg-body-tertiary">
        <h5 className="mb-0 fw-semibold me-2">Graph Browser</h5>
        <input
          type="text"
          className="form-control form-control-sm"
          style={{ width: 200, fontFamily: 'monospace' }}
          placeholder="Node ID"
          value={nodeIdInput}
          onChange={(e) => setNodeIdInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') searchNode();
            if (e.key === 'Escape') setNodeIdInput('');
          }}
        />
        <button className="btn btn-sm btn-outline-primary" onClick={searchNode} disabled={loading}>
          {loading ? 'Loading...' : 'Search'}
        </button>
        <button
          className={`btn btn-sm ${searchExpanded ? 'btn-primary' : 'btn-outline-secondary'}`}
          onClick={() => setSearchExpanded(!searchExpanded)}
        >
          <i className="bi bi-funnel" /> Advanced Search
        </button>
        {history.length > 0 && (
          <select
            className="form-select form-select-sm"
            style={{ width: 200 }}
            value=""
            onChange={(e) => {
              if (e.target.value) loadNodeAndEdges(e.target.value);
            }}
          >
            <option value="">History ({history.length})</option>
            {[...history].reverse().map((id) => (
              <option key={id} value={id}>{id}</option>
            ))}
          </select>
        )}
        <div className="flex-grow-1" />
        <small className="text-secondary">
          {visNodesRef.current.size} nodes • {edgesRef.current.length} edges
        </small>
      </div>

      {/* Advanced Search Panel */}
      {searchExpanded && (
        <div className="border-bottom bg-body-tertiary px-3 py-2">
          <div className="d-flex gap-2 mb-2">
            <button
              className={`btn btn-sm ${searchMode === 'quick' ? 'btn-primary' : 'btn-outline-secondary'}`}
              onClick={() => setSearchMode('quick')}
            >
              Quick Search
            </button>
            <button
              className={`btn btn-sm ${searchMode === 'filter' ? 'btn-primary' : 'btn-outline-secondary'}`}
              onClick={() => setSearchMode('filter')}
            >
              Filter
            </button>
            <button
              className={`btn btn-sm ${searchMode === 'cypher' ? 'btn-primary' : 'btn-outline-secondary'}`}
              onClick={() => setSearchMode('cypher')}
            >
              Cypher
            </button>
            <button
              className={`btn btn-sm ${searchMode === 'sql' ? 'btn-primary' : 'btn-outline-secondary'}`}
              onClick={() => setSearchMode('sql')}
            >
              SQL
            </button>
          </div>

          {/* Quick Search Mode */}
          {searchMode === 'quick' && (
            <div>
              <div className="input-group input-group-sm mb-2">
                <input
                  type="text"
                  className="form-control"
                  placeholder="Search by name, type, or any property..."
                  value={quickSearch}
                  onChange={(e) => setQuickSearch(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') executeSearch();
                  }}
                />
                <button className="btn btn-primary" onClick={executeSearch} disabled={searching}>
                  {searching ? 'Searching...' : 'Search'}
                </button>
              </div>
              <div className="d-flex gap-2 flex-wrap">
                <small className="text-secondary align-self-center">Quick:</small>
                {['Alice', 'Bob', 'Person', 'Zone', 'Forklift'].map((term) => (
                  <button
                    key={term}
                    className="btn btn-sm btn-outline-secondary"
                    style={{ fontSize: '0.75rem', padding: '2px 8px' }}
                    onClick={() => {
                      setQuickSearch(term);
                      setTimeout(() => executeSearch(), 100);
                    }}
                  >
                    {term}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Filter Mode */}
          {searchMode === 'filter' && (
            <div className="row g-2">
              <div className="col-auto">
                <select
                  className="form-select form-select-sm"
                  value={filterKey}
                  onChange={(e) => setFilterKey(e.target.value)}
                >
                  <option value="name">name</option>
                  <option value="type">type</option>
                  <option value="status">status</option>
                  <option value="speed">speed</option>
                  <option value="label">label</option>
                </select>
              </div>
              <div className="col-auto">
                <select
                  className="form-select form-select-sm"
                  value={filterOp}
                  onChange={(e) => setFilterOp(e.target.value as any)}
                >
                  <option value="equals">equals</option>
                  <option value="contains">contains</option>
                  <option value="gt">greater than</option>
                  <option value="lt">less than</option>
                </select>
              </div>
              <div className="col">
                <input
                  type="text"
                  className="form-control form-control-sm"
                  placeholder="value"
                  value={filterValue}
                  onChange={(e) => setFilterValue(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') executeSearch();
                  }}
                />
              </div>
              <div className="col-auto">
                <button className="btn btn-sm btn-primary" onClick={executeSearch} disabled={searching}>
                  {searching ? 'Searching...' : 'Search'}
                </button>
              </div>
            </div>
          )}

          {/* Cypher Mode */}
          {searchMode === 'cypher' && (
            <div>
              <textarea
                className="form-control form-control-sm font-monospace mb-2"
                rows={3}
                placeholder="MATCH (n) WHERE n.name CONTAINS $term RETURN n LIMIT 20"
                value={cypherQuery}
                onChange={(e) => setCypherQuery(e.target.value)}
                style={{ fontSize: '0.85rem' }}
              />
              <div className="d-flex gap-2 mb-2 flex-wrap">
                <button className="btn btn-sm btn-primary" onClick={executeSearch} disabled={searching}>
                  {searching ? 'Executing...' : 'Execute Cypher'}
                </button>
                <button
                  className="btn btn-sm btn-outline-secondary"
                  onClick={() => setCypherQuery('MATCH (n) RETURN n LIMIT 20')}
                >
                  All Nodes
                </button>
                <button
                  className="btn btn-sm btn-outline-secondary"
                  onClick={() => setCypherQuery('MATCH (n)-[r]->(m) RETURN n, r, m LIMIT 20')}
                >
                  With Edges
                </button>
                <button
                  className="btn btn-sm btn-outline-secondary"
                  onClick={() => setCypherQuery('MATCH (n:Person) WHERE n.age > 30 RETURN n')}
                >
                  Filter by Type
                </button>
              </div>
              <small className="text-secondary">
                Use <code className="bg-body-secondary px-1">$term</code> for parameter substitution
              </small>
            </div>
          )}

          {/* SQL Mode */}
          {searchMode === 'sql' && (
            <div>
              <textarea
                className="form-control form-control-sm font-monospace mb-2"
                rows={3}
                placeholder="SELECT * FROM nodes WHERE name LIKE '%term%' LIMIT 20"
                value={sqlQuery}
                onChange={(e) => setSqlQuery(e.target.value)}
                style={{ fontSize: '0.85rem' }}
              />
              <div className="d-flex gap-2 mb-2 flex-wrap">
                <button className="btn btn-sm btn-primary" onClick={executeSearch} disabled={searching}>
                  {searching ? 'Executing...' : 'Execute SQL'}
                </button>
                <button
                  className="btn btn-sm btn-outline-secondary"
                  onClick={() => setSqlQuery("SELECT * FROM nodes LIMIT 20")}
                >
                  All Nodes
                </button>
                <button
                  className="btn btn-sm btn-outline-secondary"
                  onClick={() => setSqlQuery("SELECT * FROM nodes WHERE type = 'Person' LIMIT 20")}
                >
                  By Type
                </button>
                <button
                  className="btn btn-sm btn-outline-secondary"
                  onClick={() => setSqlQuery("SELECT n.*, COUNT(e.*) as edge_count FROM nodes n LEFT JOIN edges e ON n.id = e.source GROUP BY n.id LIMIT 20")}
                >
                  With Edge Count
                </button>
              </div>
              <small className="text-secondary">
                Query graph data using SQL syntax (translated to Cypher internally)
              </small>
            </div>
          )}

          {/* Search Results */}
          {searchResults.length > 0 && (
            <div className="mt-2 border rounded bg-body" style={{ maxHeight: 200, overflow: 'auto' }}>
              <table className="table table-sm table-hover mb-0">
                <thead className="sticky-top bg-body-tertiary">
                  <tr>
                    <th className="small">Label</th>
                    <th className="small">ID</th>
                    <th className="small">Properties</th>
                    <th className="small">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {searchResults.map((result, idx) => (
                    <tr key={idx}>
                      <td className="small">{result.label}</td>
                      <td className="small font-monospace text-secondary">{displayId(result.id)}</td>
                      <td className="small text-secondary">{Object.keys(result.props).join(', ')}</td>
                      <td>
                        <button
                          className="btn btn-sm btn-outline-primary"
                          style={{ fontSize: '0.7rem', padding: '1px 6px' }}
                          onClick={() => loadFromSearchResult(result.id)}
                        >
                          Load
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {error && (
        <div className="alert alert-danger m-2 py-1 small" role="alert">
          {error}
          <button className="btn-close btn-sm float-end" onClick={() => setError(null)} />
        </div>
      )}

      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        {/* Graph canvas */}
        <div style={{ flex: 1, position: 'relative', background: '#0f172a' }}>
          <div ref={containerRef} style={{ width: '100%', height: '100%' }} />

          {/* Context menu */}
          {contextMenu && (
            <div
              className="bg-body border rounded shadow-sm py-1"
              style={{
                position: 'fixed',
                left: contextMenu.x,
                top: contextMenu.y,
                zIndex: 1000,
                minWidth: 160,
                fontSize: '0.8rem',
              }}
            >
              <button
                className="dropdown-item py-1 px-3"
                onClick={() => focusNode(contextMenu.nodeId)}
              >
                <i className="cil-zoom me-2" /> Focus Node
              </button>
              <button
                className="dropdown-item py-1 px-3"
                onClick={() => expandFromMenu(contextMenu.nodeId)}
              >
                <i className="cil-spreadsheet me-2" /> Expand Edges
              </button>
              <button
                className="dropdown-item py-1 px-3"
                onClick={() => {
                  const hex = contextMenu.nodeId;
                  navigator.clipboard.writeText(hex);
                  setContextMenu(null);
                }}
              >
                <i className="cil-copy me-2" /> Copy ID
              </button>
              <hr className="my-1" />
              <button
                className="dropdown-item py-1 px-3 text-danger"
                onClick={() => removeNode(contextMenu.nodeId)}
              >
                <i className="cil-x me-2" /> Remove from View
              </button>
            </div>
          )}
        </div>

        {/* Right panel: Selected node details */}
        <div style={{ width: 320, borderLeft: '1px solid var(--cui-border-color)', overflow: 'auto' }}>
          {selectedNode ? (
            <div className="p-3">
              <h6 className="d-flex align-items-center gap-2 mb-2">
                <span
                  className="d-inline-block rounded-circle"
                  style={{ width: 10, height: 10, background: selectedNode.color }}
                />
                {selectedNode.label}
              </h6>
              <small className="text-secondary d-block mb-2 font-monospace" style={{ wordBreak: 'break-all' }}>
                {selectedNode.id}
              </small>

              <hr />
              <h6 className="small text-secondary text-uppercase">Properties</h6>
              {Object.keys(selectedNode.properties).length === 0 ? (
                <p className="small text-secondary">No properties loaded.</p>
              ) : (
                <table className="table table-sm small mb-0">
                  <tbody>
                    {Object.entries(selectedNode.properties).map(([k, v]) => (
                      <tr key={k}>
                        <td className="font-monospace text-secondary" style={{ width: '40%' }}>{k}</td>
                        <td>{JSON.stringify(v)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}

              <hr />
              <h6 className="small text-secondary text-uppercase">Set Property</h6>
              <div className="input-group input-group-sm mb-2">
                <input type="text" className="form-control" placeholder="key" value={propKey}
                  onChange={(e) => setPropKey(e.target.value)} style={{ maxWidth: 100 }} />
                <input type="text" className="form-control" placeholder="value" value={propVal}
                  onChange={(e) => setPropVal(e.target.value)} />
                <button className="btn btn-outline-primary" onClick={setProperty}>Set</button>
              </div>

              <hr />
              <h6 className="small text-secondary text-uppercase">Add Edge</h6>
              <div className="input-group input-group-sm mb-2">
                <input type="text" className="form-control" placeholder="type" value={edgeType}
                  onChange={(e) => setEdgeType(e.target.value)} style={{ maxWidth: 100 }} />
                <input type="text" className="form-control" placeholder="target id" value={edgeTarget}
                  onChange={(e) => setEdgeTarget(e.target.value)} />
                <button className="btn btn-outline-primary" onClick={addEdge}>Add</button>
              </div>
            </div>
          ) : (
            <div className="p-3 text-secondary small">
              <p>Double-click a node to expand its edges.</p>
              <p>Right-click for context menu.</p>
              <p>Use the search bar to load a starting node.</p>
              <hr />
              <strong>Edge type colors:</strong>
              <div className="mt-2">
                {Object.entries(EDGE_COLORS).map(([type, color]) => (
                  <div key={type} className="d-flex align-items-center gap-1 mb-1">
                    <span className="d-inline-block" style={{ width: 12, height: 3, background: color }} />
                    <span className="font-monospace">{type}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
