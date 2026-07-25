import React, { useState } from 'react';
import { api } from '../lib';
import { CypherResponse } from '../types';
import { CypherHighlighter } from '../components/CypherHighlighter';

const QUERY_TEMPLATES = [
  'MATCH (n) RETURN n LIMIT 10',
  'MATCH (n) WHERE n.speed > 50 RETURN n.name, n.speed ORDER BY n.speed DESC',
  'CREATE (n:Test {name: "demo", value: 42})',
  'MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name',
  'MATCH (n) WHERE n.label = "Person" RETURN n.name, n.age LIMIT 20',
];

export const CypherPage: React.FC = () => {
  const [query, setQuery] = useState(QUERY_TEMPLATES[0]);
  const [columns, setColumns] = useState<string[]>([]);
  const [rows, setRows] = useState<Array<Array<unknown>>>([]);
  const [error, setError] = useState<string | null>(null);
  const [running, setRunning] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);

  const runQuery = async () => {
    setRunning(true);
    setError(null);
    setMsg(null);
    setColumns([]);
    setRows([]);
    try {
      const result = await api.post<CypherResponse>('/api/v2/query/cypher', { query });
      if (result.error) {
        setError(result.error);
      } else if (result.columns && result.rows) {
        setColumns(result.columns);
        setRows(result.rows);
        setMsg(`${result.rows.length} row${result.rows.length !== 1 ? 's' : ''} returned`);
      } else if (result.message) {
        setMsg(result.message);
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unknown error');
    } finally {
      setRunning(false);
    }
  };

  return (
    <div>
      <div className="border-bottom px-3 py-2 sticky-top bg-body-tertiary">
        <h5 className="mb-0 fw-semibold">Cypher Query</h5>
      </div>

      <div className="p-3">
        <div className="card mb-3">
          <div className="card-body">
            <div className="d-flex gap-2 mb-2 flex-wrap">
              {QUERY_TEMPLATES.map((t, i) => (
                <button key={i} className="btn btn-sm btn-outline-secondary" onClick={() => setQuery(t)}>
                  {t.length > 35 ? t.substring(0, 35) + '...' : t}
                </button>
              ))}
            </div>
            <CypherHighlighter
              value={query}
              onChange={setQuery}
              onRun={runQuery}
              rows={4}
              placeholder="Enter Cypher query... (Ctrl+Enter to run)"
            />
            <div className="d-flex align-items-center gap-2 mt-2">
              <button className="btn btn-primary btn-sm" onClick={runQuery} disabled={running}>
                {running ? (
                  <>
                    <span className="spinner-border spinner-border-sm me-1" role="status" />
                    Running...
                  </>
                ) : (
                  'Execute'
                )}
              </button>
              {msg && <small className="text-success">{msg}</small>}
            </div>
          </div>
        </div>

        {error && (
          <div className="alert alert-danger py-2 small" role="alert">
            <strong>Error:</strong> {error}
          </div>
        )}

        {columns.length > 0 && (
          <div className="card">
            <div className="card-body p-0">
              <div style={{ maxHeight: 500, overflow: 'auto' }}>
                <table className="table table-sm table-striped mb-0">
                  <thead>
                    <tr>
                      <th style={{ width: 40 }}>#</th>
                      {columns.map((c) => (
                        <th key={c} className="small">{c}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row, ri) => (
                      <tr key={ri}>
                        <td className="text-secondary small">{ri + 1}</td>
                        {row.map((cell, ci) => (
                          <td key={ci} className="small font-monospace">{cell === null ? <em className="text-secondary">null</em> : JSON.stringify(cell)}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {!error && columns.length === 0 && !running && !msg && (
          <p className="text-secondary">Enter a Cypher query and click Execute (or Ctrl+Enter).</p>
        )}
      </div>
    </div>
  );
};
