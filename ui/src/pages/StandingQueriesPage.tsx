import React, { useState, useEffect, useCallback } from 'react';
import { api } from '../lib';
import { useWebSocket } from '../hooks';
import { StandingQueryItem, StandingQueryList, SqPatternCondition, SqPattern } from '../types';

/// A live match event pushed over the `/api/v2/ws/sq` WebSocket.
interface SqMatchEvent {
  type?: string;
  sq_id?: string;
  match_count?: number;
}

export const StandingQueriesPage: React.FC = () => {
  const [sqs, setSqs] = useState<StandingQueryItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [msg, setMsg] = useState<string | null>(null);
  // Register form
  const [name, setName] = useState('');
  const [key, setKey] = useState('speed');
  const [condType, setCondType] = useState<SqPatternCondition['type']>('GreaterThan');
  const [condVal, setCondVal] = useState('100');

  const loadSQs = useCallback(async () => {
    try {
      const result = await api.get<StandingQueryList>('/api/v2/standing-query');
      setSqs(result.standing_queries || []);
    } catch {
      // ignore
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadSQs();
    // Slow poll as a fallback: refreshes the list membership (adds/deletes)
    // and reconciles counts if the socket ever drops. Live counts arrive via WS.
    const id = setInterval(loadSQs, 30000);
    return () => clearInterval(id);
  }, [loadSQs]);

  // Live match-count updates: the ws/sq stream forwards every SQ event; we
  // patch the matching row's count in place so the table updates without a
  // full reload.
  const onSqEvent = useCallback((data: unknown) => {
    const ev = data as SqMatchEvent;
    if (!ev || !ev.sq_id || typeof ev.match_count !== 'number') return;
    setSqs((prev) =>
      prev.map((sq) =>
        sq.id === ev.sq_id ? { ...sq, match_count: ev.match_count as number } : sq,
      ),
    );
  }, []);
  const { ready: liveReady } = useWebSocket('/api/v2/ws/sq', onSqEvent);

  const register = async () => {
    const n = name.trim() || `sq-${Date.now()}`;
    const pattern: SqPattern = {
      type: 'PropertyFilter',
      key: key.trim(),
      condition: {
        type: condType,
        value: ['GreaterThan', 'LessThan'].includes(condType)
          ? parseFloat(condVal) || 0
          : condVal.trim(),
      },
    };

    try {
      const result = await api.post<{ id: string }>('/api/v2/standing-query', { name: n, pattern });
      setMsg(`Registered: ${result.id}`);
      setName(''); setCondVal('100');
      loadSQs();
    } catch (e) {
      setMsg(`Error: ${e instanceof Error ? e.message : 'Unknown'}`);
    }
  };

  const deleteSQ = async (id: string) => {
    if (!confirm(`Delete standing query ${id}?`)) return;
    await api.del(`/api/v2/standing-query/${id}`);
    loadSQs();
  };

  return (
    <div>
      <div className="border-bottom px-3 py-2 d-flex align-items-center justify-content-between sticky-top bg-body-tertiary">
        <h5 className="mb-0 fw-semibold">Standing Queries</h5>
        <div className="d-flex align-items-center gap-2">
          <span className={`badge ${liveReady ? 'bg-success' : 'bg-secondary'}`}
            title={liveReady ? 'Live match counts via WebSocket' : 'Live feed disconnected; counts refresh on poll'}>
            {liveReady ? 'Live' : 'Polling'}
          </span>
          <button className="btn btn-sm btn-outline-secondary" onClick={loadSQs}>
            Refresh
          </button>
        </div>
      </div>

      <div className="p-3">
        {/* Register form */}
        <div className="card mb-3">
          <div className="card-header">Register Standing Query</div>
          <div className="card-body">
            <div className="row g-2 align-items-end">
              <div className="col-sm-3">
                <label className="form-label small text-secondary">Name</label>
                <input type="text" className="form-control form-control-sm" placeholder="Name" value={name}
                  onChange={(e) => setName(e.target.value)} />
              </div>
              <div className="col-sm-2">
                <label className="form-label small text-secondary">Property Key</label>
                <input type="text" className="form-control form-control-sm" value={key}
                  onChange={(e) => setKey(e.target.value)} />
              </div>
              <div className="col-sm-2">
                <label className="form-label small text-secondary">Condition</label>
                <select className="form-select form-select-sm" value={condType}
                  onChange={(e) => setCondType(e.target.value as SqPatternCondition['type'])}>
                  <option>GreaterThan</option>
                  <option>LessThan</option>
                  <option>Equals</option>
                  <option>Contains</option>
                  <option>Exists</option>
                  <option>StartsWith</option>
                </select>
              </div>
              <div className="col-sm-2">
                <label className="form-label small text-secondary">Threshold</label>
                <input type="text" className="form-control form-control-sm" value={condVal}
                  onChange={(e) => setCondVal(e.target.value)} />
              </div>
              <div className="col-sm-3">
                <button className="btn btn-primary btn-sm w-100" onClick={register}>Register</button>
              </div>
            </div>
            {msg && <div className={`mt-2 small ${msg.startsWith('Error') ? 'text-danger' : 'text-success'}`}>{msg}</div>}
          </div>
        </div>

        {/* SQ list */}
        <div className="card">
          <div className="card-body p-0">
            {loading ? (
              <div className="p-3 text-secondary">Loading...</div>
            ) : sqs.length === 0 ? (
              <div className="p-3 text-secondary">No standing queries registered.</div>
            ) : (
              <table className="table table-sm mb-0">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>ID</th>
                    <th>Matches</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {sqs.map((sq) => (
                    <tr key={sq.id}>
                      <td className="small font-monospace">{sq.name}</td>
                      <td className="small text-secondary font-monospace">{sq.id.substring(0, 12)}...</td>
                      <td>
                        <span className={`badge ${sq.match_count > 0 ? 'bg-success' : 'bg-secondary'}`}>
                          {sq.match_count}
                        </span>
                      </td>
                      <td>
                        <button className="btn btn-sm btn-outline-danger" style={{ fontSize: '0.7rem' }}
                          onClick={() => deleteSQ(sq.id)}>Delete</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};
