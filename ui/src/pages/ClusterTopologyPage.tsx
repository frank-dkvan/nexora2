import React, { useState, useEffect, useRef } from 'react';
import { api } from '../lib';
import { ClusterStatsResponse, ClusterNodeInfo, ShardInfo } from '../types';

const STATUS_COLORS: Record<string, string> = {
  alive: '#4ade80',
  dead: '#f87171',
  suspected: '#fbbf24',
};

export const ClusterTopologyPage: React.FC = () => {
  const [stats, setStats] = useState<ClusterStatsResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const svgRef = useRef<SVGSVGElement>(null);

  const loadStats = async () => {
    try {
      const res = await api.get<ClusterStatsResponse>('/api/v2/cluster/stats');
      setStats(res);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Cluster stats unavailable');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    loadStats();
    const id = setInterval(loadStats, 5000);
    return () => clearInterval(id);
  }, []);

  // Calculate node positions in a circle
  const nodes = stats?.nodes || [];
  const nodePositions = new Map<string, { x: number; y: number }>();
  const centerX = 250;
  const centerY = 200;
  const radius = 130;
  nodes.forEach((node, i) => {
    const angle = (i / Math.max(nodes.length, 1)) * 2 * Math.PI - Math.PI / 2;
    nodePositions.set(node.node_id, {
      x: centerX + radius * Math.cos(angle),
      y: centerY + radius * Math.sin(angle),
    });
  });

  const localNode = nodes.find((n) => n.is_local);

  return (
    <div>
      <div className="border-bottom px-3 py-2 d-flex align-items-center justify-content-between sticky-top bg-body-tertiary">
        <h5 className="mb-0 fw-semibold">
          <i className="bi bi-diagram-3 me-2"></i>
          Cluster Topology
        </h5>
        <button className="btn btn-sm btn-outline-secondary" onClick={loadStats}>Refresh</button>
      </div>

      <div className="p-3">
        {error && (
          <div className="alert alert-warning py-2 small" role="alert">
            <i className="bi bi-exclamation-triangle me-1"></i>
            {error}
            <div className="mt-1">Cluster mode may not be enabled. Start the server with <code>--cluster</code> flag.</div>
          </div>
        )}

        {loading && !stats ? (
          <div className="text-secondary p-3">Loading cluster stats...</div>
        ) : stats ? (
          <>
            {/* Summary cards */}
            <div className="row g-2 mb-3">
              <div className="col-sm-3">
                <div className="card">
                  <div className="card-body text-center">
                    <small className="text-secondary text-uppercase" style={{ fontSize: '0.65rem' }}>Total Nodes</small>
                    <div className="fs-4 fw-bold">{stats.total_nodes}</div>
                  </div>
                </div>
              </div>
              <div className="col-sm-3">
                <div className="card">
                  <div className="card-body text-center">
                    <small className="text-secondary text-uppercase" style={{ fontSize: '0.65rem' }}>Alive</small>
                    <div className="fs-4 fw-bold text-success">{stats.alive_nodes}</div>
                  </div>
                </div>
              </div>
              <div className="col-sm-3">
                <div className="card">
                  <div className="card-body text-center">
                    <small className="text-secondary text-uppercase" style={{ fontSize: '0.65rem' }}>Shards</small>
                    <div className="fs-4 fw-bold text-info">{stats.shards?.length || 0}</div>
                  </div>
                </div>
              </div>
              <div className="col-sm-3">
                <div className="card">
                  <div className="card-body text-center">
                    <small className="text-secondary text-uppercase" style={{ fontSize: '0.65rem' }}>Local Shards</small>
                    <div className="fs-4 fw-bold text-warning">{stats.local_shard_count}</div>
                  </div>
                </div>
              </div>
            </div>

            <div className="row">
              {/* Topology visualization */}
              <div className="col-lg-7">
                <div className="card">
                  <div className="card-header">Topology</div>
                  <div className="card-body">
                    <svg ref={svgRef} viewBox="0 0 500 400" style={{ width: '100%', maxWidth: 500, margin: '0 auto', display: 'block' }}>
                      {/* Connection lines between nodes */}
                      {nodes.map((node, i) => {
                        const pos = nodePositions.get(node.node_id);
                        if (!pos) return null;
                        return nodes.slice(i + 1).map((other, j) => {
                          const opos = nodePositions.get(other.node_id);
                          if (!opos) return null;
                          const bothAlive = node.status === 'alive' && other.status === 'alive';
                          return (
                            <line
                              key={`${i}-${j}`}
                              x1={pos.x} y1={pos.y}
                              x2={opos.x} y2={opos.y}
                              stroke={bothAlive ? '#cbd5e1' : '#fecaca'}
                              strokeWidth={1}
                              strokeDasharray={bothAlive ? 'none' : '4 2'}
                              opacity={0.5}
                            />
                          );
                        });
                      })}

                      {/* Nodes */}
                      {nodes.map((node) => {
                        const pos = nodePositions.get(node.node_id);
                        if (!pos) return null;
                        const color = STATUS_COLORS[node.status] || '#94a3b8';
                        return (
                          <g key={node.node_id}>
                            {node.is_local && (
                              <circle cx={pos.x} cy={pos.y} r={28} fill="none" stroke="#0d6efd" strokeWidth={2} strokeDasharray="3 2" opacity={0.5} />
                            )}
                            <circle cx={pos.x} cy={pos.y} r={22} fill={color} opacity={0.85} stroke="#fff" strokeWidth={2} />
                            <text x={pos.x} y={pos.y + 2} textAnchor="middle" fill="#1e293b" fontSize={10} fontWeight="bold">
                              {node.node_id.substring(0, 6)}
                            </text>
                            <text x={pos.x} y={pos.y + 38} textAnchor="middle" fill="#64748b" fontSize={8}>
                              {node.status}
                            </text>
                          </g>
                        );
                      })}

                      {nodes.length === 0 && (
                        <text x={250} y={200} textAnchor="middle" fill="#94a3b8" fontSize={14}>
                          No cluster nodes
                        </text>
                      )}
                    </svg>
                  </div>
                </div>
              </div>

              {/* Node details */}
              <div className="col-lg-5">
                <div className="card mb-2">
                  <div className="card-header">Nodes</div>
                  <div className="card-body p-0">
                    <table className="table table-sm mb-0">
                      <thead>
                        <tr>
                          <th>Node</th>
                          <th>Address</th>
                          <th>Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {nodes.map((n) => (
                          <tr key={n.node_id}>
                            <td className="small font-monospace">
                              {n.is_local && <span className="badge bg-primary me-1" style={{ fontSize: '0.6rem' }}>LOCAL</span>}
                              {n.node_id.substring(0, 12)}
                            </td>
                            <td className="small text-secondary font-monospace">{n.address}</td>
                            <td>
                              <span className="d-inline-flex align-items-center gap-1">
                                <span style={{ width: 8, height: 8, borderRadius: '50%', display: 'inline-block', background: STATUS_COLORS[n.status] }} />
                                <small>{n.status}</small>
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                {/* Shard table */}
                {stats.shards && stats.shards.length > 0 && (
                  <div className="card">
                    <div className="card-header">Shards</div>
                    <div className="card-body p-0">
                      <table className="table table-sm mb-0">
                        <thead>
                          <tr>
                            <th>Shard</th>
                            <th>Owner</th>
                            <th>Followers</th>
                            <th>Epoch</th>
                          </tr>
                        </thead>
                        <tbody>
                          {stats.shards.map((s) => (
                            <tr key={s.shard_id}>
                              <td className="small fw-bold">#{s.shard_id}</td>
                              <td className="small font-monospace">{s.owner_node.substring(0, 8)}...</td>
                              <td className="small">
                                {s.follower_nodes?.length || 0}
                                {s.follower_nodes?.map((f) => f.substring(0, 6)).join(', ')}
                              </td>
                              <td className="small text-secondary">{s.epoch}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </>
        ) : null}
      </div>
    </div>
  );
};
