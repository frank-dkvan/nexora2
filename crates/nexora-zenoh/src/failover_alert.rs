//! E3: Failover alerting hooks.
//!
//! Prometheus `/metrics` already exposes counters for scraping, but a scrape is
//! pull-based and lossy for discrete, ops-critical events: a failover fires,
//! completes, or *fails*, and an operator needs a push (webhook / PagerDuty)
//! rather than noticing a counter tick minutes later. [`FailoverAlertSink`] is
//! the push side — the auto-failover coordinator emits a [`FailoverEvent`] at
//! each transition and every registered sink delivers it out-of-band.
//!
//! Sinks are best-effort and must never block or panic the failover path: a
//! slow/broken alert endpoint cannot be allowed to stall promotion. The
//! coordinator fires alerts after the state change, and sink errors are logged,
//! not propagated.

use std::sync::Arc;

/// A discrete failover-related event worth pushing to operators.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum FailoverEvent {
    /// A shard owner was detected as failed; failover is about to start.
    OwnerFailedDetected { shard_id: usize, owner: String },
    /// A follower was successfully promoted to owner for a shard.
    PromotionSucceeded {
        shard_id: usize,
        new_owner: String,
        /// Whether the promotion went through the safe catch-up path (A1.3).
        caught_up: bool,
    },
    /// A failover attempt failed — the shard may be unavailable. Highest urgency.
    PromotionFailed { shard_id: usize, reason: String },
    /// A node's health dropped to degraded (elevated latency, not yet failed).
    NodeDegraded { node: String },
}

impl FailoverEvent {
    /// Severity label for routing/formatting (e.g. PagerDuty priority).
    pub fn severity(&self) -> AlertSeverity {
        match self {
            FailoverEvent::PromotionFailed { .. } => AlertSeverity::Critical,
            FailoverEvent::OwnerFailedDetected { .. } => AlertSeverity::Warning,
            FailoverEvent::PromotionSucceeded { .. } => AlertSeverity::Info,
            FailoverEvent::NodeDegraded { .. } => AlertSeverity::Warning,
        }
    }

    /// A one-line human-readable summary for the alert body.
    pub fn summary(&self) -> String {
        match self {
            FailoverEvent::OwnerFailedDetected { shard_id, owner } => {
                format!("shard {shard_id} owner '{owner}' detected failed; starting failover")
            }
            FailoverEvent::PromotionSucceeded {
                shard_id,
                new_owner,
                caught_up,
            } => {
                format!("shard {shard_id} promoted '{new_owner}' to owner (caught_up={caught_up})")
            }
            FailoverEvent::PromotionFailed { shard_id, reason } => {
                format!("shard {shard_id} failover FAILED: {reason}")
            }
            FailoverEvent::NodeDegraded { node } => format!("node '{node}' degraded"),
        }
    }
}

/// Alert urgency, mirroring common ops severities.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

/// A push destination for failover events. Implementations deliver out-of-band
/// (HTTP webhook, PagerDuty, log). Must be non-blocking-friendly and infallible
/// from the caller's view — return an error to be logged, never to abort
/// failover.
#[async_trait::async_trait]
pub trait FailoverAlertSink: Send + Sync {
    /// Deliver one event. Errors are logged by the caller, not propagated.
    async fn deliver(&self, event: &FailoverEvent) -> Result<(), String>;
    /// Name for logging.
    fn name(&self) -> &str;
}

/// Fans an event out to every registered sink concurrently, logging failures.
/// Used by the auto-failover coordinator after each state transition.
pub async fn emit_alert(sinks: &[Arc<dyn FailoverAlertSink>], event: FailoverEvent) {
    if sinks.is_empty() {
        return;
    }
    let futures = sinks.iter().map(|sink| {
        let event = event.clone();
        let sink = sink.clone();
        async move {
            if let Err(e) = sink.deliver(&event).await {
                tracing::warn!(sink = sink.name(), error = %e, event = ?event, "alert delivery failed");
            }
        }
    });
    futures::future::join_all(futures).await;
}

/// A [`FailoverAlertSink`] that logs events via `tracing` at severity-mapped
/// levels. Always available (no external dependency), useful as a default and
/// in tests.
pub struct LogAlertSink;

#[async_trait::async_trait]
impl FailoverAlertSink for LogAlertSink {
    async fn deliver(&self, event: &FailoverEvent) -> Result<(), String> {
        match event.severity() {
            AlertSeverity::Critical => tracing::error!(event = ?event, "{}", event.summary()),
            AlertSeverity::Warning => tracing::warn!(event = ?event, "{}", event.summary()),
            AlertSeverity::Info => tracing::info!(event = ?event, "{}", event.summary()),
        }
        Ok(())
    }
    fn name(&self) -> &str {
        "log"
    }
}

/// Build the JSON payload an HTTP webhook sink should POST for `event`.
///
/// The webhook *sink* itself lives in the app layer (nexora-app has the HTTP
/// client + `nexora-output::WebhookOutput`), so this core crate stays free of a
/// heavy HTTP dependency. The app constructs a sink that calls this to shape the
/// body, keeping the payload schema defined next to the event type.
pub fn webhook_payload(event: &FailoverEvent) -> serde_json::Value {
    serde_json::json!({
        "severity": event.severity(),
        "summary": event.summary(),
        "event": event,
        "ts": chrono::Utc::now().to_rfc3339(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingSink {
        count: Arc<AtomicUsize>,
        last: Arc<parking_lot::Mutex<Option<FailoverEvent>>>,
    }

    #[async_trait::async_trait]
    impl FailoverAlertSink for CountingSink {
        async fn deliver(&self, event: &FailoverEvent) -> Result<(), String> {
            self.count.fetch_add(1, Ordering::SeqCst);
            *self.last.lock() = Some(event.clone());
            Ok(())
        }
        fn name(&self) -> &str {
            "counting"
        }
    }

    struct FailingSink;
    #[async_trait::async_trait]
    impl FailoverAlertSink for FailingSink {
        async fn deliver(&self, _event: &FailoverEvent) -> Result<(), String> {
            Err("boom".into())
        }
        fn name(&self) -> &str {
            "failing"
        }
    }

    #[test]
    fn severity_and_summary_mapping() {
        let e = FailoverEvent::PromotionFailed {
            shard_id: 3,
            reason: "no source".into(),
        };
        assert_eq!(e.severity(), AlertSeverity::Critical);
        assert!(e.summary().contains("shard 3"));
        assert!(e.summary().contains("FAILED"));

        let ok = FailoverEvent::PromotionSucceeded {
            shard_id: 1,
            new_owner: "n2".into(),
            caught_up: true,
        };
        assert_eq!(ok.severity(), AlertSeverity::Info);
    }

    #[tokio::test]
    async fn emit_fans_out_to_all_sinks() {
        let count = Arc::new(AtomicUsize::new(0));
        let last = Arc::new(parking_lot::Mutex::new(None));
        let sink = Arc::new(CountingSink {
            count: count.clone(),
            last: last.clone(),
        });
        let sinks: Vec<Arc<dyn FailoverAlertSink>> = vec![sink.clone(), sink];
        let ev = FailoverEvent::NodeDegraded { node: "n1".into() };
        emit_alert(&sinks, ev.clone()).await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            2,
            "both sinks receive the event"
        );
        assert_eq!(*last.lock(), Some(ev));
    }

    #[tokio::test]
    async fn emit_survives_failing_sink() {
        // A failing sink must not panic or abort the fan-out; a healthy sink
        // alongside it still receives the event.
        let count = Arc::new(AtomicUsize::new(0));
        let last = Arc::new(parking_lot::Mutex::new(None));
        let good = Arc::new(CountingSink {
            count: count.clone(),
            last,
        });
        let sinks: Vec<Arc<dyn FailoverAlertSink>> = vec![Arc::new(FailingSink), good];
        emit_alert(
            &sinks,
            FailoverEvent::OwnerFailedDetected {
                shard_id: 0,
                owner: "x".into(),
            },
        )
        .await;
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "healthy sink still delivered"
        );
    }

    #[tokio::test]
    async fn empty_sinks_is_noop() {
        emit_alert(&[], FailoverEvent::NodeDegraded { node: "n".into() }).await;
        // No panic, returns immediately.
    }

    #[test]
    fn webhook_payload_has_severity_summary_and_event() {
        let e = FailoverEvent::PromotionFailed {
            shard_id: 7,
            reason: "boom".into(),
        };
        let p = webhook_payload(&e);
        assert_eq!(p["severity"], serde_json::json!("Critical"));
        assert!(p["summary"].as_str().unwrap().contains("shard 7"));
        assert!(
            p["event"].is_object(),
            "structured event included for machine routing"
        );
        assert!(p["ts"].is_string(), "timestamp present");
    }
}
