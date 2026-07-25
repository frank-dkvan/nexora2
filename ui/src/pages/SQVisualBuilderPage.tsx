import React, { useState } from 'react';
import { api } from '../lib';
import { SqPattern, SqPatternCondition } from '../types';

type NodeType = 'PropertyFilter' | 'LabelFilter' | 'EdgePattern' | 'And' | 'Or' | 'Not';

interface BuilderNode {
  id: string;
  type: NodeType;
  key?: string;
  conditionType?: SqPatternCondition['type'];
  conditionValue?: string;
  labels?: string;
  edgeType?: string;
  edgeTarget?: string;
}

const NODE_ICONS: Record<NodeType, string> = {
  PropertyFilter: 'bi-filter',
  LabelFilter: 'bi-tag',
  EdgePattern: 'bi-arrow-right',
  And: 'bi-intersection',
  Or: 'bi-union',
  Not: 'bi-x-circle',
};

const NODE_COLORS: Record<NodeType, string> = {
  PropertyFilter: '#3b82f6',
  LabelFilter: '#8b5cf6',
  EdgePattern: '#10b981',
  And: '#f59e0b',
  Or: '#ef4444',
  Not: '#6b7280',
};

const CONDITION_TYPES: SqPatternCondition['type'][] = [
  'GreaterThan', 'LessThan', 'Equals', 'Contains', 'Exists', 'StartsWith',
];

let nodeCounter = 0;

export const SQVisualBuilderPage: React.FC = () => {
  const [nodes, setNodes] = useState<BuilderNode[]>([
    { id: 'n0', type: 'PropertyFilter', key: 'speed', conditionType: 'GreaterThan', conditionValue: '100' },
  ]);
  const [name, setName] = useState('');
  const [msg, setMsg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const addNode = (type: NodeType) => {
    const id = `n${++nodeCounter}`;
    const newNode: BuilderNode = { id, type };
    if (type === 'PropertyFilter') {
      newNode.key = 'name';
      newNode.conditionType = 'Equals';
      newNode.conditionValue = '';
    } else if (type === 'LabelFilter') {
      newNode.labels = 'Person';
    } else if (type === 'EdgePattern') {
      newNode.edgeType = 'KNOWS';
      newNode.edgeTarget = '';
    } else if (type === 'And' || type === 'Or') {
      // Composite nodes - no extra fields
    }
    setNodes([...nodes, newNode]);
  };

  const removeNode = (id: string) => {
    setNodes(nodes.filter((n) => n.id !== id));
  };

  const updateNode = (id: string, field: keyof BuilderNode, value: string) => {
    setNodes(nodes.map((n) => (n.id === id ? { ...n, [field]: value } : n)));
  };

  const buildPattern = (nodes: BuilderNode[]): SqPattern => {
    if (nodes.length === 0) {
      throw new Error('At least one pattern node is required');
    }
    if (nodes.length === 1) {
      return buildSingleNode(nodes[0]);
    }
    // Multiple nodes = AND chain
    const [first, ...rest] = nodes;
    if (rest.length === 0) return buildSingleNode(first);
    return {
      type: 'And',
      patterns: [buildSingleNode(first), buildPattern(rest)],
    };
  };

  const buildSingleNode = (n: BuilderNode): SqPattern => {
    switch (n.type) {
      case 'PropertyFilter':
        if (!n.key?.trim()) throw new Error('Property key is required');
        return {
          type: 'PropertyFilter',
          key: n.key.trim(),
          condition: {
            type: n.conditionType || 'Equals',
            value: ['GreaterThan', 'LessThan'].includes(n.conditionType || '')
              ? parseFloat(n.conditionValue || '0') || 0
              : n.conditionValue || '',
          },
        };
      case 'LabelFilter':
        return {
          type: 'LabelFilter',
          labels: (n.labels || '').split(',').map((l) => l.trim()).filter(Boolean),
        };
      case 'EdgePattern':
        return {
          type: 'EdgePattern',
          key: n.edgeType?.trim() || 'KNOWS',
        } as SqPattern;
      case 'And':
        return { type: 'And', patterns: [] };
      case 'Or':
        return { type: 'Or', patterns: [] };
      case 'Not':
        return { type: 'Not', patterns: [] };
      default:
        throw new Error(`Unknown node type: ${n.type}`);
    }
  };

  const register = async () => {
    setBusy(true);
    setError(null);
    setMsg(null);
    try {
      const pattern = buildPattern(nodes);
      const n = name.trim() || `sq-${Date.now()}`;
      const res = await api.post<{ id: string }>('/api/v2/standing-query', { name: n, pattern });
      setMsg(`Registered standing query: ${res.id}`);
      setName('');
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
    } finally {
      setBusy(false);
    }
  };

  // Generate JSON preview
  const jsonPreview = (() => {
    try {
      return JSON.stringify(buildPattern(nodes), null, 2);
    } catch (e) {
      return `Error: ${e instanceof Error ? e.message : 'Invalid'}`;
    }
  })();

  return (
    <div>
      <div className="border-bottom px-3 py-2 sticky-top bg-body-tertiary">
        <h5 className="mb-0 fw-semibold">
          <i className="bi bi-diagram-2 me-2"></i>
          SQ Visual Builder
          <small className="ms-2 text-secondary" style={{ fontSize: '0.7rem' }}>Drag-free pattern builder</small>
        </h5>
      </div>

      <div className="p-3">
        <div className="row">
          {/* Builder canvas */}
          <div className="col-lg-8">
            <div className="card mb-3">
              <div className="card-header d-flex justify-content-between align-items-center">
                <span>Pattern Nodes</span>
                <div className="dropdown">
                  <button className="btn btn-sm btn-outline-primary dropdown-toggle" data-bs-toggle="dropdown">
                    <i className="bi bi-plus me-1"></i>Add Node
                  </button>
                  <ul className="dropdown-menu">
                    {(['PropertyFilter', 'LabelFilter', 'EdgePattern', 'And', 'Or', 'Not'] as NodeType[]).map((t) => (
                      <li key={t}>
                        <button className="dropdown-item small" onClick={() => addNode(t)}>
                          <i className={`${NODE_ICONS[t]} me-2`} style={{ color: NODE_COLORS[t] }}></i>
                          {t}
                        </button>
                      </li>
                    ))}
                  </ul>
                </div>
              </div>
              <div className="card-body">
                {nodes.length === 0 ? (
                  <p className="text-secondary text-center py-3">
                    No pattern nodes yet. Click "Add Node" to start building.
                  </p>
                ) : (
                  <div className="d-flex flex-column gap-2">
                    {nodes.map((n, i) => (
                      <div key={n.id} className="border rounded p-2" style={{ borderLeft: `4px solid ${NODE_COLORS[n.type]}` }}>
                        <div className="d-flex align-items-center justify-content-between mb-1">
                          <span className="badge" style={{ background: NODE_COLORS[n.type], fontSize: '0.7rem' }}>
                            <i className={`${NODE_ICONS[n.type]} me-1`}></i>
                            {i > 0 && <small className="me-1">AND</small>}
                            {n.type}
                          </span>
                          <button className="btn btn-sm btn-outline-danger py-0 px-1" onClick={() => removeNode(n.id)}>
                            <i className="bi bi-x"></i>
                          </button>
                        </div>

                        {/* Node-specific fields */}
                        {n.type === 'PropertyFilter' && (
                          <div className="row g-1">
                            <div className="col-sm-4">
                              <input type="text" className="form-control form-control-sm" placeholder="property key"
                                value={n.key || ''} onChange={(e) => updateNode(n.id, 'key', e.target.value)} />
                            </div>
                            <div className="col-sm-3">
                              <select className="form-select form-select-sm" value={n.conditionType || 'Equals'}
                                onChange={(e) => updateNode(n.id, 'conditionType', e.target.value)}>
                                {CONDITION_TYPES.map((c) => <option key={c}>{c}</option>)}
                              </select>
                            </div>
                            {n.conditionType !== 'Exists' && (
                              <div className="col-sm-4">
                                <input type="text" className="form-control form-control-sm" placeholder="value"
                                  value={n.conditionValue || ''} onChange={(e) => updateNode(n.id, 'conditionValue', e.target.value)} />
                              </div>
                            )}
                          </div>
                        )}

                        {n.type === 'LabelFilter' && (
                          <input type="text" className="form-control form-control-sm" placeholder="labels (comma-separated)"
                            value={n.labels || ''} onChange={(e) => updateNode(n.id, 'labels', e.target.value)} />
                        )}

                        {n.type === 'EdgePattern' && (
                          <div className="row g-1">
                            <div className="col-sm-4">
                              <input type="text" className="form-control form-control-sm" placeholder="edge type"
                                value={n.edgeType || ''} onChange={(e) => updateNode(n.id, 'edgeType', e.target.value)} />
                            </div>
                            <div className="col-sm-7">
                              <input type="text" className="form-control form-control-sm" placeholder="target node (optional)"
                                value={n.edgeTarget || ''} onChange={(e) => updateNode(n.id, 'edgeTarget', e.target.value)} />
                            </div>
                          </div>
                        )}

                        {(n.type === 'And' || n.type === 'Or' || n.type === 'Not') && (
                          <small className="text-secondary">Composite node — combines all preceding nodes</small>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>

            {/* Register */}
            <div className="card">
              <div className="card-body d-flex gap-2 align-items-end">
                <div className="flex-grow-1">
                  <label className="form-label small text-secondary">Standing Query Name</label>
                  <input type="text" className="form-control form-control-sm" placeholder="my-sq"
                    value={name} onChange={(e) => setName(e.target.value)} />
                </div>
                <button className="btn btn-primary btn-sm" onClick={register} disabled={busy || nodes.length === 0}>
                  {busy ? 'Registering...' : 'Register SQ'}
                </button>
              </div>
              {msg && <div className="px-3 pb-2 small text-success">{msg}</div>}
              {error && <div className="px-3 pb-2 small text-danger">{error}</div>}
            </div>
          </div>

          {/* JSON preview */}
          <div className="col-lg-4">
            <div className="card">
              <div className="card-header">Pattern JSON Preview</div>
              <div className="card-body">
                <pre className="small font-monospace p-2 bg-light rounded" style={{ maxHeight: 500, overflow: 'auto' }}>
                  {jsonPreview}
                </pre>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};
