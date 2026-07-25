import React from 'react';
import { usePolling } from '../hooks';
import { HealthResponse, StandingQueryList, SystemInfoResponse } from '../types';
import { api } from '../lib';

const Card: React.FC<{ title: string; value: string | number; green?: boolean; style?: React.CSSProperties }> = ({
  title,
  value,
  green,
  style,
}) => (
  <div className="card" style={style}>
    <div className="card-body">
      <h6 className="card-subtitle mb-1 text-secondary text-uppercase" style={{ fontSize: '0.7rem' }}>
        {title}
      </h6>
      <div className={`fs-3 fw-bold ${green ? 'text-success' : 'text-info'}`}>
        {value}
      </div>
    </div>
  </div>
);

export const DashboardPage: React.FC = () => {
  const { data: health } = usePolling<HealthResponse>('/api/v2/health', 5000);
  const { data: sqList } = usePolling<StandingQueryList>('/api/v2/standing-query', 10000);
  const { data: sysInfo } = usePolling<SystemInfoResponse>('/api/v2/system/info', 30000);

  return (
    <div>
      <div className="border-bottom px-3 py-2 d-flex align-items-center justify-content-between sticky-top bg-body-tertiary">
        <h5 className="mb-0 fw-semibold">Dashboard</h5>
        {health && (
          <small className="text-secondary">
            Up {Math.floor((health.uptime_seconds ?? 0) / 60)}m {(health.uptime_seconds ?? 0) % 60}s
          </small>
        )}
      </div>

      <div className="p-3">
        <div className="row g-3 mb-3">
          <div className="col-sm-6 col-lg-3">
            <Card title="Active Nodes" value={health?.active_nodes ?? '-'} />
          </div>
          <div className="col-sm-6 col-lg-3">
            <Card title="Shards" value={health?.shards ?? '-'} />
          </div>
          <div className="col-sm-6 col-lg-3">
            <Card title="Standing Queries" value={sqList?.standing_queries?.length ?? '-'} green />
          </div>
          <div className="col-sm-6 col-lg-3">
            <Card title="Mode" value={health?.mode ?? '-'} />
          </div>
        </div>

        <div className="row g-3 mb-3">
          <div className="col-sm-6">
            <div className="card">
              <div className="card-body">
                <h6 className="card-subtitle mb-2 text-secondary">System Info</h6>
                {sysInfo ? (
                  <dl className="row mb-0 small">
                    <dt className="col-5 text-secondary">Version</dt>
                    <dd className="col-7">{sysInfo.version}</dd>
                    <dt className="col-5 text-secondary">Num Shards</dt>
                    <dd className="col-7">{sysInfo.num_shards}</dd>
                    <dt className="col-5 text-secondary">Max Nodes/Shard</dt>
                    <dd className="col-7">{sysInfo.max_nodes_per_shard}</dd>
                    <dt className="col-5 text-secondary">RocksDB Path</dt>
                    <dd className="col-7 text-truncate">{sysInfo.rocksdb_path || '(in-memory)'}</dd>
                  </dl>
                ) : (
                  <p className="text-secondary small">Loading...</p>
                )}
              </div>
            </div>
          </div>
          <div className="col-sm-6">
            <div className="card">
              <div className="card-body">
                <h6 className="card-subtitle mb-2 text-secondary">Quick Start</h6>
                <pre className="bg-body-secondary p-2 rounded small mb-0" style={{ maxHeight: 140, overflow: 'auto' }}>
{`curl -X PUT http://localhost:8080/api/v2/graph/node/$(echo -n n1|xxd -p)/property/name \\
  -H "Content-Type: application/json" -d '{"value":"test"}'

curl -X POST http://localhost:8080/api/v2/standing-query \\
  -H "Content-Type: application/json" \\
  -d '{"name":"alert","pattern":{"type":"PropertyFilter","key":"speed","condition":{"type":"GreaterThan","value":100}}}'`}
                </pre>
              </div>
            </div>
          </div>
        </div>

        {sqList && sqList.standing_queries && sqList.standing_queries.length > 0 && (
          <div className="card">
            <div className="card-body">
              <h6 className="card-subtitle mb-2 text-secondary">Standing Queries</h6>
              <table className="table table-sm mb-0">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Matches</th>
                  </tr>
                </thead>
                <tbody>
                  {sqList.standing_queries.map((sq) => (
                    <tr key={sq.id}>
                      <td>{sq.name}</td>
                      <td>
                        <span className={`badge ${sq.match_count > 0 ? 'bg-success' : 'bg-secondary'}`}>
                          {sq.match_count}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};
