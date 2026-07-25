import React, { useState } from 'react';
import { api } from '../lib';
import { ExplainPlanNode } from '../types';

const EXPLAIN_TEMPLATES = [
  'MATCH (n) RETURN n LIMIT 10',
  'MATCH (n:Person) WHERE n.age > 30 RETURN n.name, n.age',
  'MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name',
  'MATCH (n) WHERE n.speed > 50 RETURN n ORDER BY n.speed DESC',
];

// Color map for operators
const OPERATOR_COLORS: Record<string, string> = {
  Scan: '#3b82f6',
  Filter: '#f59e0b',
  Limit: '#10b981',
  Project: '#8b5cf6',
  Join: '#ef4444',
  Expand: '#06b6d4',
  Sort: '#ec4899',
  Aggregate: '#14b8a6',
  Create: '#22c55e',
  Delete: '#dc2626',
  Set: '#eab308',
};

export const ExplainPage: React.FC = () => {
  const [query, setQuery] = useState(EXPLAIN_TEMPLATES[0]);
  const [plan, setPlan] = useState<ExplainPlanNode | null>(null);
  const [isReadOnly, setIsReadOnly] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);

  const explain = async () => {
    setRunning(true);
    setError(null);
    setPlan(null);
    try {
      const res = await api.post<{ plan: ExplainPlanNode; query: string; is_read_only: boolean }>(
        '/api/v2/query/explain',
        { query },
      );
      setPlan(res.plan);
      setIsReadOnly(res.is_read_only);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
    } finally {
      setRunning(false);
    }
  };

  // Flatten the plan tree for table view
  const flattenPlan = (node: ExplainPlanNode, depth: number = 0, rows: Array<{ node: ExplainPlanNode; depth: number }> = []): Array<{ node: ExplainPlanNode; depth: number }> => {
    rows.push({ node, depth });
    if (node.children) {
      node.children.forEach((c) => flattenPlan(c, depth + 1, rows));
    }
    return rows;
  };

  const flatRows = plan ? flattenPlan(plan) : [];

  return (
    <div>
      <div className="border-bottom px-3 py-2 sticky-top bg-body-tertiary">
        <h5 className="mb-0 fw-semibold">
          <i className="bi bi-diagram-2 me-2"></i>
          Query Plan (EXPLAIN)
        </h5>
      </div>

      <div className="p-3">
        {/* Query input */}
        <div className="card mb-3">
          <div className="card-body">
            <div className="d-flex gap-2 mb-2 flex-wrap">
              {EXPLAIN_TEMPLATES.map((t, i) => (
                <button key={i} className="btn btn-sm btn-outline-secondary" onClick={() => setQuery(t)}>
                  {t.length > 35 ? t.substring(0, 35) + '...' : t}
                </button>
              ))}
            </div>
            <textarea
              className="form-control font-monospace"
              rows={3}
              style={{ fontSize: '0.85rem' }}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
                  e.preventDefault();
                  explain();
                }
              }}
            />
            <div className="d-flex align-items-center gap-2 mt-2">
              <button className="btn btn-primary btn-sm" onClick={explain} disabled={running}>
                {running ? (
                  <>
                    <span className="spinner-border spinner-border-sm me-1" />
                    Analyzing...
                  </>
                ) : (
                  'Explain'
                )}
              </button>
              {plan && (
                <span className={`badge ${isReadOnly ? 'bg-success' : 'bg-warning text-dark'}`}>
                  {isReadOnly ? 'Read-Only' : 'Write Operation'}
                </span>
              )}
            </div>
          </div>
        </div>

        {error && (
          <div className="alert alert-danger py-2 small" role="alert">
            {error}
          </div>
        )}

        {/* Plan visualization */}
        {plan && (
          <>
            {/* Tree view */}
            <div className="card mb-3">
              <div className="card-header">Execution Tree</div>
              <div className="card-body">
                <PlanTreeNode node={plan} />
              </div>
            </div>

            {/* Table view */}
            <div className="card">
              <div className="card-header">Plan Details</div>
              <div className="card-body p-0">
                <table className="table table-sm table-striped mb-0">
                  <thead>
                    <tr>
                      <th style={{ width: 40 }}>#</th>
                      <th>Operator</th>
                      <th>Est. Rows</th>
                      <th>Details</th>
                    </tr>
                  </thead>
                  <tbody>
                    {flatRows.map((r, i) => {
                      const color = OPERATOR_COLORS[r.node.operator] || '#6b7280';
                      return (
                        <tr key={i}>
                          <td className="text-secondary small">{i + 1}</td>
                          <td>
                            <span style={{ paddingLeft: r.depth * 20 }}>
                              {r.depth > 0 && <span className="text-secondary me-1">└─</span>}
                              <span className="badge" style={{ background: color, fontSize: '0.7rem' }}>
                                {r.node.operator}
                              </span>
                            </span>
                          </td>
                          <td className="small text-end font-monospace">
                            {r.node.estimated_rows?.toLocaleString() || '—'}
                          </td>
                          <td className="small font-monospace">
                            {r.node.details ? JSON.stringify(r.node.details) : '—'}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          </>
        )}

        {!plan && !error && !running && (
          <p className="text-secondary">Enter a query and click Explain to see the execution plan.</p>
        )}
      </div>
    </div>
  );
};

// Recursive tree node component
const PlanTreeNode: React.FC<{ node: ExplainPlanNode; isLast?: boolean; prefix?: string }> = ({
  node,
  isLast = true,
  prefix = '',
}) => {
  const color = OPERATOR_COLORS[node.operator] || '#6b7280';
  const hasChildren = node.children && node.children.length > 0;

  return (
    <div className="d-flex align-items-start" style={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>
      <span className="text-secondary" style={{ whiteSpace: 'pre' }}>
        {prefix}{isLast ? '└─ ' : '├─ '}
      </span>
      <div className="flex-grow-1">
        <span className="badge me-1" style={{ background: color, fontSize: '0.7rem' }}>
          {node.operator}
        </span>
        <small className="text-secondary me-2">rows: {node.estimated_rows?.toLocaleString() || '?'}</small>
        {node.details && (
          <small className="text-secondary">
            {Object.entries(node.details).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(', ')}
          </small>
        )}
        {hasChildren && (
          <div className="mt-1">
            {node.children!.map((child, i) => (
              <PlanTreeNode
                key={i}
                node={child}
                isLast={i === node.children!.length - 1}
                prefix={prefix + (isLast ? '   ' : '│  ')}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
};
