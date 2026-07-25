import React, { useState } from 'react';
import { api } from '../lib';
import { VectorSearchResult } from '../types';

const SAMPLE_VECTORS: Record<string, number[]> = {
  'red':    [0.9, 0.1, 0.05, 0.0],
  'blue':   [0.1, 0.1, 0.9, 0.0],
  'green':  [0.1, 0.9, 0.1, 0.0],
  'warm':   [0.8, 0.6, 0.2, 0.1],
  'cool':   [0.1, 0.3, 0.7, 0.5],
};

export const VectorSearchPage: React.FC = () => {
  const [qid, setQid] = useState('');
  const [vectorInput, setVectorInput] = useState('0.1, 0.2, 0.3, 0.4');
  const [k, setK] = useState(5);
  const [results, setResults] = useState<VectorSearchResult[]>([]);
  const [indexMsg, setIndexMsg] = useState<string | null>(null);
  const [searchMsg, setSearchMsg] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const parseVector = (s: string): number[] => {
    return s.split(/[,\s]+/).filter(Boolean).map((v) => {
      const n = parseFloat(v);
      if (isNaN(n)) throw new Error(`Invalid number: ${v}`);
      return n;
    });
  };

  const doIndex = async () => {
    setBusy(true);
    setError(null);
    setIndexMsg(null);
    try {
      const vec = parseVector(vectorInput);
      const id = qid.trim() || `vec-${Date.now()}`;
      await api.post('/api/v2/vector/index', { qid: id, vector: vec });
      setIndexMsg(`Indexed node ${id} with ${vec.length}D vector`);
      setQid('');
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
    } finally {
      setBusy(false);
    }
  };

  const doSearch = async () => {
    setBusy(true);
    setError(null);
    setSearchMsg(null);
    setResults([]);
    try {
      const vec = parseVector(vectorInput);
      const res = await api.post<{ results: VectorSearchResult[] }>('/api/v2/vector/search', {
        vector: vec,
        k,
      });
      setResults(res.results || []);
      setSearchMsg(`Found ${res.results?.length || 0} nearest neighbors`);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
    } finally {
      setBusy(false);
    }
  };

  const doDelete = async (id: string) => {
    if (!confirm(`Delete vector for node ${id}?`)) return;
    try {
      await api.del(`/api/v2/vector/node/${id}`);
      setResults(results.filter((r) => r.qid !== id));
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
    }
  };

  return (
    <div>
      <div className="border-bottom px-3 py-2 sticky-top bg-body-tertiary">
        <h5 className="mb-0 fw-semibold">
          <i className="bi bi-diagram-2 me-2"></i>
          Vector Search
          <small className="ms-2 text-secondary" style={{ fontSize: '0.7rem' }}>HNSW k-NN</small>
        </h5>
      </div>

      <div className="p-3">
        {/* Input card */}
        <div className="card mb-3">
          <div className="card-header">Vector Input</div>
          <div className="card-body">
            <div className="row g-2 align-items-end">
              <div className="col-sm-3">
                <label className="form-label small text-secondary">Node ID (optional for index)</label>
                <input
                  type="text"
                  className="form-control form-control-sm"
                  placeholder="auto-generated"
                  value={qid}
                  onChange={(e) => setQid(e.target.value)}
                />
              </div>
              <div className="col-sm-5">
                <label className="form-label small text-secondary">Vector (comma-separated)</label>
                <input
                  type="text"
                  className="form-control form-control-sm font-monospace"
                  value={vectorInput}
                  onChange={(e) => setVectorInput(e.target.value)}
                />
              </div>
              <div className="col-sm-2">
                <label className="form-label small text-secondary">k (neighbors)</label>
                <input
                  type="number"
                  className="form-control form-control-sm"
                  min={1}
                  max={100}
                  value={k}
                  onChange={(e) => setK(parseInt(e.target.value) || 5)}
                />
              </div>
              <div className="col-sm-2 d-flex gap-1">
                <button className="btn btn-sm btn-outline-primary flex-grow-1" onClick={doIndex} disabled={busy}>
                  Index
                </button>
                <button className="btn btn-sm btn-primary flex-grow-1" onClick={doSearch} disabled={busy}>
                  {busy ? '...' : 'Search'}
                </button>
              </div>
            </div>

            {/* Quick fill samples */}
            <div className="mt-2 d-flex gap-1 flex-wrap align-items-center">
              <small className="text-secondary me-1">Quick fill:</small>
              {Object.entries(SAMPLE_VECTORS).map(([name, vec]) => (
                <button
                  key={name}
                  className="btn btn-sm btn-outline-light text-secondary border"
                  style={{ fontSize: '0.7rem' }}
                  onClick={() => setVectorInput(vec.join(', '))}
                >
                  {name}
                </button>
              ))}
            </div>

            {indexMsg && <div className="mt-2 small text-success">{indexMsg}</div>}
            {searchMsg && <div className="mt-2 small text-info">{searchMsg}</div>}
            {error && (
              <div className="mt-2 alert alert-danger py-1 small" role="alert">
                {error}
              </div>
            )}
          </div>
        </div>

        {/* Results */}
        {results.length > 0 && (
          <div className="card">
            <div className="card-header d-flex justify-content-between align-items-center">
              <span>Search Results</span>
              <small className="text-secondary">{results.length} neighbors found</small>
            </div>
            <div className="card-body p-0">
              <table className="table table-sm table-striped mb-0">
                <thead>
                  <tr>
                    <th style={{ width: 40 }}>#</th>
                    <th>Node ID</th>
                    <th>Distance</th>
                    <th>Similarity</th>
                    <th style={{ width: 80 }}>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((r, i) => {
                    const similarity = Math.max(0, 1 - r.distance).toFixed(4);
                    const simPct = parseFloat(similarity) * 100;
                    return (
                      <tr key={r.qid}>
                        <td className="text-secondary small">{i + 1}</td>
                        <td className="small font-monospace">{r.qid}</td>
                        <td className="small font-monospace">{r.distance.toFixed(6)}</td>
                        <td>
                          <div className="d-flex align-items-center gap-2">
                            <div className="progress flex-grow-1" style={{ height: 16, minWidth: 60 }}>
                              <div
                                className={`progress-bar ${simPct > 70 ? 'bg-success' : simPct > 40 ? 'bg-warning' : 'bg-danger'}`}
                                style={{ width: `${simPct}%` }}
                              />
                            </div>
                            <small className="text-secondary" style={{ minWidth: 45 }}>
                              {similarity}
                            </small>
                          </div>
                        </td>
                        <td>
                          <button
                            className="btn btn-sm btn-outline-danger"
                            style={{ fontSize: '0.7rem' }}
                            onClick={() => doDelete(r.qid)}
                          >
                            Del
                          </button>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {results.length === 0 && !busy && !error && (
          <p className="text-secondary">
            Index a vector or search for nearest neighbors. Use the quick-fill buttons for sample vectors.
          </p>
        )}
      </div>
    </div>
  );
};
