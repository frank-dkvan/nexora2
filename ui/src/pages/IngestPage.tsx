import React, { useState, useEffect } from 'react';
import { api } from '../lib';
import { FileIngestResponse } from '../types';

export const IngestPage: React.FC = () => {
  const [path, setPath] = useState('');
  const [idField, setIdField] = useState('id');
  const [loading, setLoading] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [ingests, setIngests] = useState<FileIngestResponse[]>([]);

  // CRUD quick-test
  const [crudQid, setCrudQid] = useState('test-node');
  const [crudKey, setCrudKey] = useState('');
  const [crudVal, setCrudVal] = useState('');
  const [crudResult, setCrudResult] = useState('');

  const hexEncode = (s: string) => {
    let r = '';
    for (let i = 0; i < s.length; i++) r += s.charCodeAt(i).toString(16).padStart(2, '0');
    return r;
  };

  const startIngest = async () => {
    if (!path.trim()) return;
    setLoading(true);
    setMsg(null);
    try {
      const result = await api.post<FileIngestResponse>('/api/v2/ingest/file', {
        path: path.trim(),
        id_field: idField.trim() || 'id',
      });
      setMsg(`Started: ${JSON.stringify(result)}`);
      setIngests((prev) => [...prev, result]);
    } catch (e) {
      setMsg(`Error: ${e instanceof Error ? e.message : 'Unknown'}`);
    } finally {
      setLoading(false);
    }
  };

  const crudSet = async () => {
    const q = hexEncode(crudQid.trim());
    const k = crudKey.trim();
    let v: unknown = crudVal.trim();
    if (!isNaN(Number(v))) v = Number(v);
    try {
      const r = await api.put(`/api/v2/graph/node/${q}/property/${k}`, { value: v });
      setCrudResult(JSON.stringify(r, null, 2));
    } catch (e) {
      setCrudResult(`Error: ${e instanceof Error ? e.message : 'Unknown'}`);
    }
  };

  const crudGet = async () => {
    const q = hexEncode(crudQid.trim());
    const k = crudKey.trim();
    try {
      const r = await api.get(`/api/v2/graph/node/${q}/property/${k}`);
      setCrudResult(JSON.stringify(r, null, 2));
    } catch (e) {
      setCrudResult(`Error: ${e instanceof Error ? e.message : 'Unknown'}`);
    }
  };

  return (
    <div>
      <div className="border-bottom px-3 py-2 sticky-top bg-body-tertiary">
        <h5 className="mb-0 fw-semibold">Data Ingest</h5>
      </div>

      <div className="p-3">
        {/* File Ingest */}
        <div className="card mb-3">
          <div className="card-header">File Ingest</div>
          <div className="card-body">
            <div className="input-group input-group-sm">
              <input type="text" className="form-control" placeholder="File path (JSON lines)" value={path}
                onChange={(e) => setPath(e.target.value)} style={{ flex: 3 }} />
              <input type="text" className="form-control" placeholder="ID field" value={idField}
                onChange={(e) => setIdField(e.target.value)} style={{ flex: 1, maxWidth: 100 }} />
              <button className="btn btn-primary" onClick={startIngest} disabled={loading}>
                {loading ? 'Starting...' : 'Start'}
              </button>
            </div>
            {msg && <div className={`mt-2 small ${msg.startsWith('Error') ? 'text-danger' : 'text-success'}`}>{msg}</div>}
          </div>
        </div>

        {/* CRUD quick test */}
        <div className="card">
          <div className="card-header">Quick CRUD Test</div>
          <div className="card-body">
            <div className="input-group input-group-sm mb-2">
              <input type="text" className="form-control" placeholder="Node ID" value={crudQid}
                onChange={(e) => setCrudQid(e.target.value)} style={{ maxWidth: 140 }} />
              <input type="text" className="form-control" placeholder="Key" value={crudKey}
                onChange={(e) => setCrudKey(e.target.value)} style={{ maxWidth: 100 }} />
              <input type="text" className="form-control" placeholder="Value" value={crudVal}
                onChange={(e) => setCrudVal(e.target.value)} style={{ maxWidth: 120 }} />
              <button className="btn btn-outline-primary" onClick={crudSet}>Set</button>
              <button className="btn btn-outline-secondary" onClick={crudGet}>Get</button>
            </div>
            {crudResult && (
              <pre className="bg-body-secondary p-2 rounded small mb-0" style={{ maxHeight: 150, overflow: 'auto' }}>
                {crudResult}
              </pre>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};
