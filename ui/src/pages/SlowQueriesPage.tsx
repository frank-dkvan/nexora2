import React from 'react';
import { usePolling } from '../hooks';
import { SlowQueryStats } from '../types';

/// Slow-query monitoring page.
///
/// Backed by `GET /api/v2/admin/slow-queries`, which reports aggregate slow-query
/// counters (per-query detail is emitted to the tracing log at target
/// `slow_query`). This page visualizes the ratio and highlights when the slow
/// fraction crosses a warning band.
export const SlowQueriesPage: React.FC = () => {
  const { data, error, loading } = usePolling<SlowQueryStats>(
    '/api/v2/admin/slow-queries',
    5000,
  );

  const total = data?.total_queries ?? 0;
  const slow = data?.slow_queries ?? 0;
  const ratio = data?.slow_query_ratio ?? 0;
  const threshold = data?.threshold_ms ?? 1000;

  // Warning bands: <1% healthy, 1-5% watch, >5% alert.
  const ratioClass = ratio > 5 ? 'text-danger' : ratio >= 1 ? 'text-warning' : 'text-success';
  const barClass = ratio > 5 ? 'bg-danger' : ratio >= 1 ? 'bg-warning' : 'bg-success';

  return (
    <div>
      <div className="border-bottom px-3 py-2 sticky-top bg-body-tertiary d-flex align-items-center justify-content-between">
        <h5 className="mb-0 fw-semibold">
          <i className="bi bi-hourglass-split me-2"></i>
          Slow Queries
        </h5>
        <span className="badge bg-secondary">threshold: {threshold} ms</span>
      </div>

      <div className="p-3">
        {error && (
          <div className="alert alert-warning py-2 small" role="alert">
            <i className="bi bi-exclamation-triangle me-1"></i>
            {error}
          </div>
        )}

        {loading && !data ? (
          <div className="text-secondary p-3">Loading slow-query stats…</div>
        ) : (
          <>
            <div className="row g-3 mb-3">
              <div className="col-sm-4">
                <div className="card">
                  <div className="card-body text-center">
                    <small className="text-secondary text-uppercase" style={{ fontSize: '0.65rem' }}>
                      Total Queries
                    </small>
                    <div className="fs-3 fw-bold">{total.toLocaleString()}</div>
                  </div>
                </div>
              </div>
              <div className="col-sm-4">
                <div className="card">
                  <div className="card-body text-center">
                    <small className="text-secondary text-uppercase" style={{ fontSize: '0.65rem' }}>
                      Slow Queries
                    </small>
                    <div className={`fs-3 fw-bold ${slow > 0 ? 'text-warning' : 'text-success'}`}>
                      {slow.toLocaleString()}
                    </div>
                  </div>
                </div>
              </div>
              <div className="col-sm-4">
                <div className="card">
                  <div className="card-body text-center">
                    <small className="text-secondary text-uppercase" style={{ fontSize: '0.65rem' }}>
                      Slow Ratio
                    </small>
                    <div className={`fs-3 fw-bold ${ratioClass}`}>{ratio}%</div>
                  </div>
                </div>
              </div>
            </div>

            <div className="card mb-3">
              <div className="card-header">Slow-query fraction</div>
              <div className="card-body">
                <div className="d-flex justify-content-between small mb-1">
                  <span>
                    {slow.toLocaleString()} of {total.toLocaleString()} queries exceeded {threshold} ms
                  </span>
                  <span className={ratioClass}>{ratio}%</span>
                </div>
                <div className="progress" style={{ height: 20 }}>
                  <div
                    className={`progress-bar ${barClass}`}
                    role="progressbar"
                    style={{ width: `${Math.min(100, Math.max(2, ratio))}%` }}
                    aria-valuenow={ratio}
                    aria-valuemin={0}
                    aria-valuemax={100}
                  >
                    {ratio}%
                  </div>
                </div>
                <div className="small text-secondary mt-2">
                  {ratio > 5
                    ? 'Alert: slow-query fraction is high (>5%). Investigate query plans and indexing.'
                    : ratio >= 1
                      ? 'Watch: slow-query fraction is elevated (1–5%).'
                      : 'Healthy: slow-query fraction is low (<1%).'}
                </div>
              </div>
            </div>

            {data?.note && (
              <div className="alert alert-info py-2 small mb-0" role="note">
                <i className="bi bi-info-circle me-1"></i>
                {data.note}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
};
