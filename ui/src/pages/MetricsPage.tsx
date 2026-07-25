import React, { useCallback, useState } from 'react';
import { usePolling, useWebSocket } from '../hooks';
import { HealthResponse, MetricsResponse } from '../types';

/// Live metrics snapshot pushed over the `ws/metrics` WebSocket.
interface WsMetrics {
  type: 'Metrics';
  active_nodes: number;
  standing_queries: number;
  events_total: number;
  sq_matches_total: number;
  errors_total: number;
  wal_append_total: number;
  queries_total: number;
  slow_queries_total: number;
}

export const MetricsPage: React.FC = () => {
  // Health still polls (cheap, and carries shard count for the gauge).
  const { data: health } = usePolling<HealthResponse>('/api/v2/health', 5000);
  // Metrics prefer the real-time WebSocket; fall back to the last HTTP poll
  // until the first frame arrives (or if the socket never connects).
  const { data: metricsPoll } = usePolling<MetricsResponse>('/api/v2/metrics', 5000);
  const [live, setLive] = useState<WsMetrics | null>(null);

  const onMessage = useCallback((msg: unknown) => {
    const m = msg as Partial<WsMetrics>;
    if (m && m.type === 'Metrics') {
      setLive(m as WsMetrics);
    }
  }, []);
  const { ready } = useWebSocket('/api/v2/ws/metrics', onMessage);

  const activeNodes = live?.active_nodes ?? health?.active_nodes ?? 0;
  const sqCount = live?.standing_queries ?? health?.standing_queries ?? 0;
  const totalEvents = live?.events_total ?? metricsPoll?.events_total ?? 0;
  const errors = live?.errors_total ?? metricsPoll?.errors_total ?? 0;
  const sqMatches = live?.sq_matches_total ?? metricsPoll?.sq_matches_total ?? 0;
  const walAppends = live?.wal_append_total ?? metricsPoll?.wal_append_total ?? 0;

  return (
    <div>
      <div className="border-bottom px-3 py-2 sticky-top bg-body-tertiary d-flex align-items-center justify-content-between">
        <h5 className="mb-0 fw-semibold">Metrics</h5>
        <span className={`badge ${ready ? 'bg-success' : 'bg-secondary'}`}>
          <i className={`bi ${ready ? 'bi-broadcast' : 'bi-arrow-repeat'} me-1`}></i>
          {ready ? 'Live' : 'Polling'}
        </span>
      </div>

      <div className="p-3">
        <div className="row g-3 mb-3">
          <div className="col-sm-6 col-lg-3">
            <div className="card">
              <div className="card-body text-center">
                <h6 className="card-subtitle mb-1 text-secondary text-uppercase" style={{ fontSize: '0.7rem' }}>
                  Active Nodes
                </h6>
                <div className="fs-3 fw-bold text-info">{activeNodes}</div>
              </div>
            </div>
          </div>
          <div className="col-sm-6 col-lg-3">
            <div className="card">
              <div className="card-body text-center">
                <h6 className="card-subtitle mb-1 text-secondary text-uppercase" style={{ fontSize: '0.7rem' }}>
                  Events Total
                </h6>
                <div className="fs-3 fw-bold text-success">{totalEvents.toLocaleString()}</div>
              </div>
            </div>
          </div>
          <div className="col-sm-6 col-lg-3">
            <div className="card">
              <div className="card-body text-center">
                <h6 className="card-subtitle mb-1 text-secondary text-uppercase" style={{ fontSize: '0.7rem' }}>
                  SQ Matches
                </h6>
                <div className="fs-3 fw-bold text-warning">{sqMatches.toLocaleString()}</div>
              </div>
            </div>
          </div>
          <div className="col-sm-6 col-lg-3">
            <div className="card">
              <div className="card-body text-center">
                <h6 className="card-subtitle mb-1 text-secondary text-uppercase" style={{ fontSize: '0.7rem' }}>
                  Errors
                </h6>
                <div className={`fs-3 fw-bold ${errors > 0 ? 'text-danger' : 'text-success'}`}>
                  {errors}
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="row g-3">
          <div className="col-md-6">
            <div className="card">
              <div className="card-header">Node Activity</div>
              <div className="card-body">
                <div className="mb-2">
                  <div className="d-flex justify-content-between small mb-1">
                    <span>Active Nodes / Shards</span>
                    <span className="text-secondary">{activeNodes} active</span>
                  </div>
                  <div className="progress" style={{ height: 20 }}>
                    <div
                      className="progress-bar bg-info"
                      style={{
                        width: `${Math.min(100, (activeNodes / Math.max(1, health?.shards ?? 1)) * 100)}%`,
                      }}
                    >
                      {activeNodes}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <div className="col-md-6">
            <div className="card">
              <div className="card-header">Standing Queries</div>
              <div className="card-body">
                <div className="mb-2">
                  <div className="d-flex justify-content-between small mb-1">
                    <span>Active SQs</span>
                    <span className="text-secondary">{sqCount} registered</span>
                  </div>
                  <div className="progress" style={{ height: 20 }}>
                    <div
                      className="progress-bar bg-success"
                      style={{ width: `${Math.min(100, sqCount * 10)}%` }}
                    >
                      {sqCount}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="card mt-3">
          <div className="card-header">WAL Operations</div>
          <div className="card-body">
            <dl className="row mb-0 small">
              <dt className="col-sm-3 text-secondary">Total Appends</dt>
              <dd className="col-sm-3">{walAppends.toLocaleString()}</dd>
              <dt className="col-sm-3 text-secondary">Avg Append Latency</dt>
              <dd className="col-sm-3">{(metricsPoll?.wal_append_avg_us ?? 0) > 0 ? `${metricsPoll?.wal_append_avg_us} μs` : 'N/A'}</dd>
            </dl>
          </div>
        </div>
      </div>
    </div>
  );
};
