//! Prometheus metrics endpoint for operational observability.
//!
//! Exposes replication health, heartbeat status, shard health, 2PC transaction
//! statistics to a Prometheus scraper via a `/metrics` HTTP endpoint.

use crate::replica_writer::ReplicationMetrics;
use std::sync::Arc;

/// Prometheus metrics exporter
pub struct PrometheusExporter {
    replication_metrics: Arc<ReplicationMetrics>,
}

impl PrometheusExporter {
    pub fn new(replication_metrics: Arc<ReplicationMetrics>) -> Self {
        Self {
            replication_metrics,
        }
    }

    /// Generate Prometheus text format metrics
    pub fn export(&self) -> String {
        let repl = self.replication_metrics.snapshot();

        let mut output = String::new();

        // Replication metrics
        output.push_str("# HELP nexora_replication_attempts_total Total replication attempts\n");
        output.push_str("# TYPE nexora_replication_attempts_total counter\n");
        output.push_str(&format!(
            "nexora_replication_attempts_total {}\n",
            repl.attempts
        ));

        output.push_str("# HELP nexora_replication_quorum_ok_total Successful quorum writes\n");
        output.push_str("# TYPE nexora_replication_quorum_ok_total counter\n");
        output.push_str(&format!(
            "nexora_replication_quorum_ok_total {}\n",
            repl.quorum_ok
        ));

        output.push_str("# HELP nexora_replication_quorum_failed_total Failed quorum writes\n");
        output.push_str("# TYPE nexora_replication_quorum_failed_total counter\n");
        output.push_str(&format!(
            "nexora_replication_quorum_failed_total {}\n",
            repl.quorum_failed
        ));

        output.push_str(
            "# HELP nexora_replication_follower_nacks_total Follower acknowledgment failures\n",
        );
        output.push_str("# TYPE nexora_replication_follower_nacks_total counter\n");
        output.push_str(&format!(
            "nexora_replication_follower_nacks_total {}\n",
            repl.follower_nacks
        ));

        output.push_str("# HELP nexora_replication_missing_acks_total Missing acknowledgments\n");
        output.push_str("# TYPE nexora_replication_missing_acks_total counter\n");
        output.push_str(&format!(
            "nexora_replication_missing_acks_total {}\n",
            repl.missing_acks_total
        ));

        // Replication health ratio
        let health_ratio = if repl.attempts > 0 {
            repl.quorum_ok as f64 / repl.attempts as f64
        } else {
            1.0
        };
        output
            .push_str("# HELP nexora_replication_health_ratio Ratio of successful quorum writes\n");
        output.push_str("# TYPE nexora_replication_health_ratio gauge\n");
        output.push_str(&format!(
            "nexora_replication_health_ratio {:.4}\n",
            health_ratio
        ));

        // Process info
        output.push_str("# HELP nexora_build_info Build information\n");
        output.push_str("# TYPE nexora_build_info gauge\n");
        output.push_str(&format!(
            "nexora_build_info{{version=\"{}\",commit=\"{}\"}} 1\n",
            env!("CARGO_PKG_VERSION"),
            option_env!("GIT_COMMIT").unwrap_or("unknown")
        ));

        output
    }
}

/// HTTP server for Prometheus metrics endpoint
pub struct MetricsServer {
    exporter: Arc<PrometheusExporter>,
    bind_addr: String,
}

impl MetricsServer {
    pub fn new(exporter: Arc<PrometheusExporter>, bind_addr: String) -> Self {
        Self {
            exporter,
            bind_addr,
        }
    }

    /// Start the metrics HTTP server (blocking, run in dedicated thread)
    pub fn serve(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let server = tiny_http::Server::http(&self.bind_addr).map_err(
            |e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(std::io::Error::other(e)) },
        )?;

        tracing::info!(
            "Prometheus metrics endpoint listening on http://{}/metrics",
            self.bind_addr
        );

        for request in server.incoming_requests() {
            let url = request.url().to_string();

            if url == "/metrics" {
                let metrics = self.exporter.export();
                let response = tiny_http::Response::from_string(metrics).with_header(
                    tiny_http::Header::from_bytes(
                        &b"Content-Type"[..],
                        &b"text/plain; version=0.0.4"[..],
                    )
                    .unwrap(),
                );
                let _ = request.respond(response);
            } else if url == "/health" {
                let response = tiny_http::Response::from_string("OK");
                let _ = request.respond(response);
            } else {
                let response = tiny_http::Response::from_string("Not Found").with_status_code(404);
                let _ = request.respond(response);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_export_format() {
        let metrics = Arc::new(ReplicationMetrics::default());
        metrics
            .attempts
            .store(100, std::sync::atomic::Ordering::Relaxed);
        metrics
            .quorum_ok
            .store(95, std::sync::atomic::Ordering::Relaxed);
        metrics
            .quorum_failed
            .store(5, std::sync::atomic::Ordering::Relaxed);

        let exporter = PrometheusExporter::new(metrics);
        let output = exporter.export();

        assert!(output.contains("nexora_replication_attempts_total 100"));
        assert!(output.contains("nexora_replication_quorum_ok_total 95"));
        assert!(output.contains("nexora_replication_quorum_failed_total 5"));
        assert!(output.contains("nexora_replication_health_ratio"));
    }
}
