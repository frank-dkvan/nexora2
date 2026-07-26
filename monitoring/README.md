# Nexora 2.0 Production Monitoring - Grafana Dashboard

This directory contains Grafana dashboard templates and Prometheus alert rules for monitoring Nexora 2.0 in production.

## Quick Start

### 1. Import Dashboards

```bash
# Copy dashboard JSON to your Grafana provisioning directory
cp grafana-dashboards/*.json /etc/grafana/provisioning/dashboards/

# Or import via Grafana UI:
# Dashboard → Import → Upload JSON file
```

### 2. Configure Prometheus Alerts

```bash
# Add to your prometheus.yml
cp prometheus-alerts/nexora-alerts.yml /etc/prometheus/rules/

# Add to prometheus.yml:
rule_files:
  - "/etc/prometheus/rules/nexora-alerts.yml"

# Reload Prometheus
curl -X POST http://localhost:9090/-/reload
```

### 3. Verify Metrics

```bash
# Check that Nexora metrics are being scraped
curl http://localhost:8080/metrics | grep deepstreaming

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets | jq '.data.activeTargets[] | select(.labels.job=="nexora")'
```

## Dashboard Overview

### 1. `nexora-overview.json` - System Overview Dashboard

**Panels:**
- Active Nodes (gauge)
- Query Rate (graph)
- Error Rate (graph)
- Replication Lag (graph) **NEW**
- Failover Events (counter) **NEW**
- Catch-up Status (state timeline) **NEW**

**Use Case:** Executive summary, status page

### 2. `nexora-replication.json` - Replication Health Dashboard **NEW**

**Panels:**
- Replication Lag per Shard (heatmap)
- Failover Success Rate (gauge)
- Catch-up Duration (histogram)
- WAL Apply Rate (graph)
- Follower Count (graph)
- Commit Index vs Last Applied (dual-axis graph)

**Use Case:** SRE troubleshooting, capacity planning

### 3. `nexora-performance.json` - Performance Dashboard

**Panels:**
- Query Latency (percentiles: p50, p95, p99)
- WAL Append Latency
- Slow Query Count
- Events Ingested per Second
- Standing Query Match Rate

**Use Case:** Performance optimization, SLA monitoring

## Alert Rules

### Critical Alerts (PagerDuty)

| Alert | Threshold | Description |
|-------|-----------|-------------|
| `NexoraHighReplicationLag` | lag > 5000ms for 2min | Follower falling behind |
| `NexoraFailoverFailed` | 2+ failures in 5min | Failover mechanism broken |
| `NexoraClusterDown` | 0 active nodes for 1min | Complete outage |
| `NexoraCatchupStuck` | in_progress=1 for 30min | Recovery stalled |

### Warning Alerts (Slack)

| Alert | Threshold | Description |
|-------|-----------|-------------|
| `NexoraModerateReplicationLag` | lag > 1000ms for 5min | Minor lag buildup |
| `NexoraSlowQueries` | >10/min for 5min | Query performance degraded |
| `NexoraHighErrorRate` | >5% error rate for 5min | Elevated errors |

## Metrics Reference

### New Production Metrics

```promql
# Replication lag (milliseconds)
deepstreaming_replication_lag_ms

# Failover events
deepstreaming_failover_total
deepstreaming_failover_success_total

# Catch-up status
deepstreaming_catch_up_duration_ms
deepstreaming_catch_up_in_progress  # 0=idle, 1=active
```

### Example Queries

**Failover success rate (last 1h):**
```promql
rate(deepstreaming_failover_success_total[1h]) 
/ 
rate(deepstreaming_failover_total[1h])
```

**Average catch-up duration (last 24h):**
```promql
avg_over_time(deepstreaming_catch_up_duration_ms[24h]) / 1000
```

**Replication lag 95th percentile:**
```promql
histogram_quantile(0.95, deepstreaming_replication_lag_ms)
```

## Directory Structure

```
monitoring/
├── README.md                           # This file
├── grafana-dashboards/
│   ├── nexora-overview.json           # System overview
│   ├── nexora-replication.json        # Replication health (NEW)
│   └── nexora-performance.json        # Query performance
├── prometheus-alerts/
│   ├── nexora-alerts.yml              # Alert rules
│   └── recording-rules.yml            # Pre-computed metrics
└── examples/
    ├── docker-compose.monitoring.yml  # Full monitoring stack
    └── prometheus.yml                 # Prometheus config example
```

## Full Monitoring Stack (Docker Compose)

See `examples/docker-compose.monitoring.yml` for a complete monitoring stack including:
- Prometheus
- Grafana
- Alertmanager
- Node Exporter

```bash
cd examples
docker-compose -f docker-compose.monitoring.yml up -d
```

Access:
- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090
- Alertmanager: http://localhost:9093

## Troubleshooting

### Metrics Not Appearing

```bash
# 1. Check Nexora is exposing metrics
curl http://localhost:8080/metrics | grep deepstreaming_replication_lag_ms

# 2. Check Prometheus scrape config
curl http://localhost:9090/api/v1/targets

# 3. Check Prometheus logs
docker logs prometheus
```

### Alerts Not Firing

```bash
# Check alert rules are loaded
curl http://localhost:9090/api/v1/rules | jq '.data.groups[].rules[] | select(.name | contains("Nexora"))'

# Check alert state
curl http://localhost:9090/api/v1/alerts
```

## Production Best Practices

1. **Retention**: Set Prometheus retention to at least 30 days for replication metrics
2. **Scrape Interval**: Use 15s for replication lag (default: 1min)
3. **Alerting**: Route critical alerts to PagerDuty, warnings to Slack
4. **Dashboards**: Pin `nexora-replication.json` to your NOC display
5. **Recording Rules**: Pre-compute failover success rate for fast queries

## Next Steps

After setting up monitoring:
1. Establish SLOs based on baseline metrics (e.g., p99 lag < 2s)
2. Configure Alertmanager routing to your on-call system
3. Set up weekly reports using Grafana's scheduled snapshots
4. Enable long-term storage (Thanos/Cortex) for capacity planning

## Related Documentation

- [Production Readiness Assessment](../docs/DASHBOARD_PRODUCTION_ASSESSMENT.md)
- [Operational Runbook](../docs/ops/RUNBOOK.md)
- [Metrics API Documentation](../docs/architecture/API_ENDPOINT_REFERENCE.md#metrics-endpoints)
