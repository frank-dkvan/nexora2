import React, { useState, useEffect, useCallback } from 'react';
import { api } from '../lib';
import { MvDefinition } from '../types';

const DATA_TYPES = ['Integer', 'Float', 'String', 'Boolean', 'Timestamp'];
const REFRESH_MODES = ['Immediate', 'Incremental', 'Manual'];

export const MaterializedViewPage: React.FC = () => {
  const [views, setViews] = useState<MvDefinition[]>([]);
  const [loading, setLoading] = useState(true);
  const [msg, setMsg] = useState<string | null>(null);
  const [selectedView, setSelectedView] = useState<MvDefinition | null>(null);
  const [viewData, setViewData] = useState<Record<string, unknown>[]>([]);
  const [dataLoading, setDataLoading] = useState(false);

  // Create form
  const [name, setName] = useState('');
  const [refreshMode, setRefreshMode] = useState('Immediate');
  const [columns, setColumns] = useState<{ name: string; data_type: string }[]>([
    { name: 'id', data_type: 'String' },
    { name: 'count', data_type: 'Integer' },
  ]);

  const loadViews = useCallback(async () => {
    try {
      const res = await api.get<{ views: MvDefinition[] }>('/api/v2/materialized-views');
      setViews(res.views || []);
    } catch {
      // ignore
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadViews();
    const id = setInterval(loadViews, 15000);
    return () => clearInterval(id);
  }, [loadViews]);

  const createView = async () => {
    setMsg(null);
    try {
      const n = name.trim() || `mv-${Date.now()}`;
      await api.post('/api/v2/materialized-views', {
        name: n,
        columns: columns.filter((c) => c.name.trim()),
        refresh_mode: refreshMode,
      });
      setMsg(`Created view: ${n}`);
      setName('');
      loadViews();
    } catch (e) {
      setMsg(`Error: ${e instanceof Error ? e.message : 'Unknown'}`);
    }
  };

  const dropView = async (id: string) => {
    if (!confirm(`Drop view ${id}?`)) return;
    try {
      await api.del(`/api/v2/materialized-views/${id}`);
      if (selectedView?.view_id === id) {
        setSelectedView(null);
        setViewData([]);
      }
      loadViews();
    } catch (e) {
      setMsg(`Error: ${e instanceof Error ? e.message : 'Unknown'}`);
    }
  };

  const refreshView = async (id: string) => {
    try {
      await api.post(`/api/v2/materialized-views/${id}/refresh`);
      setMsg(`Refresh triggered for ${id}`);
      loadViews();
    } catch (e) {
      setMsg(`Error: ${e instanceof Error ? e.message : 'Unknown'}`);
    }
  };

  const queryView = async (view: MvDefinition) => {
    setSelectedView(view);
    setDataLoading(true);
    setViewData([]);
    try {
      const res = await api.get<{ rows: Record<string, unknown>[] }>(
        `/api/v2/materialized-views/${view.view_id}/data`,
      );
      setViewData(res.rows || []);
    } catch {
      // ignore
    } finally {
      setDataLoading(false);
    }
  };

  const addColumn = () => setColumns([...columns, { name: '', data_type: 'String' }]);
  const removeColumn = (i: number) => setColumns(columns.filter((_, idx) => idx !== i));
  const updateColumn = (i: number, field: 'name' | 'data_type', val: string) => {
    const next = [...columns];
    next[i] = { ...next[i], [field]: val };
    setColumns(next);
  };

  const dataCols = selectedView?.columns?.map((c) => c.name) || [];

  return (
    <div>
      <div className="border-bottom px-3 py-2 d-flex align-items-center justify-content-between sticky-top bg-body-tertiary">
        <h5 className="mb-0 fw-semibold">
          <i className="bi bi-table me-2"></i>
          Materialized Views
        </h5>
        <button className="btn btn-sm btn-outline-secondary" onClick={loadViews}>Refresh</button>
      </div>

      <div className="p-3">
        {/* Create form */}
        <div className="card mb-3">
          <div className="card-header">Create Materialized View</div>
          <div className="card-body">
            <div className="row g-2 align-items-end mb-2">
              <div className="col-sm-4">
                <label className="form-label small text-secondary">View Name</label>
                <input
                  type="text"
                  className="form-control form-control-sm"
                  placeholder="my-view"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                />
              </div>
              <div className="col-sm-3">
                <label className="form-label small text-secondary">Refresh Mode</label>
                <select
                  className="form-select form-select-sm"
                  value={refreshMode}
                  onChange={(e) => setRefreshMode(e.target.value)}
                >
                  {REFRESH_MODES.map((m) => <option key={m}>{m}</option>)}
                </select>
              </div>
            </div>

            {/* Columns editor */}
            <div className="mb-2">
              <small className="text-secondary d-block mb-1">Columns</small>
              {columns.map((col, i) => (
                <div key={i} className="row g-1 mb-1">
                  <div className="col-sm-5">
                    <input
                      type="text"
                      className="form-control form-control-sm"
                      placeholder="column name"
                      value={col.name}
                      onChange={(e) => updateColumn(i, 'name', e.target.value)}
                    />
                  </div>
                  <div className="col-sm-4">
                    <select
                      className="form-select form-select-sm"
                      value={col.data_type}
                      onChange={(e) => updateColumn(i, 'data_type', e.target.value)}
                    >
                      {DATA_TYPES.map((t) => <option key={t}>{t}</option>)}
                    </select>
                  </div>
                  <div className="col-sm-1">
                    <button
                      className="btn btn-sm btn-outline-danger w-100"
                      onClick={() => removeColumn(i)}
                      disabled={columns.length <= 1}
                    >
                      <i className="bi bi-dash"></i>
                    </button>
                  </div>
                </div>
              ))}
              <button className="btn btn-sm btn-outline-secondary mt-1" onClick={addColumn}>
                <i className="bi bi-plus me-1"></i>Add Column
              </button>
            </div>

            <button className="btn btn-primary btn-sm" onClick={createView}>Create View</button>
            {msg && (
              <div className={`mt-2 small ${msg.startsWith('Error') ? 'text-danger' : 'text-success'}`}>
                {msg}
              </div>
            )}
          </div>
        </div>

        {/* Views list */}
        <div className="card mb-3">
          <div className="card-header">Registered Views</div>
          <div className="card-body p-0">
            {loading ? (
              <div className="p-3 text-secondary">Loading...</div>
            ) : views.length === 0 ? (
              <div className="p-3 text-secondary">No materialized views registered.</div>
            ) : (
              <table className="table table-sm table-striped mb-0">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>ID</th>
                    <th>Mode</th>
                    <th>Rows</th>
                    <th>Linked SQ</th>
                    <th>Last Refresh</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {views.map((v) => (
                    <tr key={v.view_id} style={{ cursor: 'pointer' }}
                      className={selectedView?.view_id === v.view_id ? 'table-active' : ''}>
                      <td className="small fw-semibold" onClick={() => queryView(v)}>{v.name}</td>
                      <td className="small text-secondary font-monospace" onClick={() => queryView(v)}>
                        {v.view_id.substring(0, 12)}...
                      </td>
                      <td onClick={() => queryView(v)}>
                        <span className="badge bg-info text-dark">{v.refresh_mode}</span>
                      </td>
                      <td className="small text-end" onClick={() => queryView(v)}>
                        {v.row_count?.toLocaleString() || 0}
                      </td>
                      <td className="small" onClick={() => queryView(v)}>
                        {v.linked_sq_id ? (
                          <span className="font-monospace text-secondary">{v.linked_sq_id.substring(0, 8)}...</span>
                        ) : (
                          <span className="text-secondary">—</span>
                        )}
                      </td>
                      <td className="small text-secondary" onClick={() => queryView(v)}>
                        {v.last_refreshed_at || 'Never'}
                      </td>
                      <td>
                        <div className="btn-group btn-group-sm">
                          <button className="btn btn-outline-primary" style={{ fontSize: '0.7rem' }}
                            onClick={() => refreshView(v.view_id)} title="Refresh">
                            <i className="bi bi-arrow-clockwise"></i>
                          </button>
                          <button className="btn btn-outline-danger" style={{ fontSize: '0.7rem' }}
                            onClick={() => dropView(v.view_id)} title="Drop">
                            <i className="bi bi-trash"></i>
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        {/* View data */}
        {selectedView && (
          <div className="card">
            <div className="card-header d-flex justify-content-between align-items-center">
              <span>Data: {selectedView.name}</span>
              <small className="text-secondary">{viewData.length} rows</small>
            </div>
            <div className="card-body p-0">
              {dataLoading ? (
                <div className="p-3 text-secondary">Loading data...</div>
              ) : viewData.length === 0 ? (
                <div className="p-3 text-secondary">No data in this view.</div>
              ) : (
                <div style={{ maxHeight: 400, overflow: 'auto' }}>
                  <table className="table table-sm table-striped mb-0">
                    <thead>
                      <tr>
                        {dataCols.map((c) => <th key={c} className="small">{c}</th>)}
                      </tr>
                    </thead>
                    <tbody>
                      {viewData.map((row, i) => (
                        <tr key={i}>
                          {dataCols.map((c) => (
                            <td key={c} className="small font-monospace">
                              {row[c] === null || row[c] === undefined
                                ? <em className="text-secondary">null</em>
                                : typeof row[c] === 'object'
                                  ? JSON.stringify(row[c])
                                  : String(row[c])}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};
