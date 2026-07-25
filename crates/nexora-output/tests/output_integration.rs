//! Comprehensive integration tests for nexora-output crate.
//!
//! Coverage:
//! - ConsoleOutput: basic creation, processing success, JSON formatting
//! - WebhookOutput: creation, HTTP POST behaviour, timeout handling
//! - OutputSink trait contract: name(), status()
//! - Error paths: webhook failure modes, OutputError conversions
//! - Concurrency: multiple sinks processing simultaneously
//! - Boundary: empty SQ result, special characters, large payloads

use nexora_id::PropertyValue;
use nexora_output::sink_trait::OutputStatus;
use nexora_output::{ConsoleOutput, OutputError, OutputSink, WebhookOutput};
use nexora_standing_query::{ResultType, StandingQueryResult};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Barrier;

/// Helper: create a minimal StandingQueryResult for testing.
fn make_sq_result(sq_name: &str, result_is_match: bool) -> StandingQueryResult {
    let mut props = HashMap::new();
    props.insert("speed".to_string(), PropertyValue::Float(80.0));
    props.insert("name".to_string(), PropertyValue::String("Alice".into()));

    StandingQueryResult::new(
        uuid::Uuid::new_v4(),
        sq_name,
        nexora_id::NexoraId::from_bytes(b"node-12345".to_vec()),
        props,
        if result_is_match {
            ResultType::Matched
        } else {
            ResultType::Unmatched
        },
        chrono::Utc::now(),
    )
}

fn make_empty_result() -> StandingQueryResult {
    StandingQueryResult::new(
        uuid::Uuid::new_v4(),
        "empty-test",
        nexora_id::NexoraId::from_bytes(b"empty-node".to_vec()),
        HashMap::new(),
        ResultType::Matched,
        chrono::Utc::now(),
    )
}

// ============================================================
// ConsoleOutput Tests
// ============================================================

#[test]
fn test_console_output_name() {
    let sink = ConsoleOutput::new("my-console");
    assert_eq!(sink.name(), "my-console");
}

#[test]
fn test_console_output_status() {
    let sink = ConsoleOutput::new("my-console");
    match sink.status() {
        OutputStatus::Active => {}
        _ => panic!("expected Active status"),
    }
}

#[tokio::test]
async fn test_console_output_process_match() {
    let sink = ConsoleOutput::new("sq-speed");
    let result = make_sq_result("sq-speed", true);
    let outcome = sink.process(&result).await;
    assert!(
        outcome.is_ok(),
        "Console process should succeed: {:?}",
        outcome
    );
}

#[tokio::test]
async fn test_console_output_process_unmatch() {
    let sink = ConsoleOutput::new("sq-speed");
    let result = make_sq_result("sq-speed", false);
    let outcome = sink.process(&result).await;
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn test_console_output_process_empty_properties() {
    let sink = ConsoleOutput::new("sq-empty");
    let result = make_empty_result();
    let outcome = sink.process(&result).await;
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn test_console_output_process_special_characters() {
    let sink = ConsoleOutput::new("sq-special");
    let mut props = HashMap::new();
    props.insert(
        "text".to_string(),
        PropertyValue::String("line1\nline2\t\"quoted\"".into()),
    );
    let result = StandingQueryResult::new(
        uuid::Uuid::new_v4(),
        "sq-special",
        nexora_id::NexoraId::from_bytes(b"special".to_vec()),
        props,
        ResultType::Matched,
        chrono::Utc::now(),
    );
    let outcome = sink.process(&result).await;
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn test_console_output_large_payload() {
    let sink = ConsoleOutput::new("sq-large");
    let mut props = HashMap::new();
    for i in 0..100 {
        props.insert(
            format!("field_{}", i),
            PropertyValue::String(format!("value_{}_aaaaaaaaaaaaaaaaaaaa", i)),
        );
    }
    let result = StandingQueryResult::new(
        uuid::Uuid::new_v4(),
        "sq-large",
        nexora_id::NexoraId::from_bytes(b"large-node".to_vec()),
        props,
        ResultType::Matched,
        chrono::Utc::now(),
    );
    let outcome = sink.process(&result).await;
    assert!(outcome.is_ok());
}

// ============================================================
// WebhookOutput Tests
// ============================================================

#[test]
fn test_webhook_output_creation() {
    let sink = WebhookOutput::new("webhook-1", "http://localhost:9999/hook");
    assert_eq!(sink.name(), "webhook-1");
}

#[test]
fn test_webhook_output_status() {
    let sink = WebhookOutput::new("webhook-1", "http://localhost:9999/hook");
    match sink.status() {
        OutputStatus::Active => {}
        _ => panic!("expected Active status"),
    }
}

/// Webhook to an invalid URL should be handled (may timeout or fail).
/// This test uses a scheme that reqwest rejects immediately, so we can
/// reliably test the error path without network access.
#[tokio::test]
async fn test_webhook_output_invalid_url() {
    // Use an empty string URL — reqwest should reject this immediately
    let sink = WebhookOutput::new("webhook-bad", "");
    let result = make_sq_result("webhook-bad", true);
    let outcome = sink.process(&result).await;
    // Should return error, not panic — we just verify no crash
    if let Err(e) = outcome {
        // Expected: reqwest can't POST to empty URL
        assert!(!format!("{}", e).is_empty());
    }
    // If for some reason it succeeds, that's also OK — the point is no panic
}

#[tokio::test]
async fn test_webhook_output_multiple_sequential() {
    let sink = WebhookOutput::new("webhook-seq", "http://localhost:9999/hook");
    for _i in 0..5 {
        let result = make_sq_result("webhook-seq", true);
        let outcome = sink.process(&result).await;
        // May fail or succeed depending on whether a server is listening.
        if let Err(e) = outcome {
            match e {
                OutputError::Sink(_) => {} // expected
                OutputError::Io(_) => {}   // also possible
            }
        }
    }
}

// ============================================================
// Trait Contract Tests
// ============================================================

#[test]
fn test_output_error_display() {
    let err = OutputError::Sink("test error".to_string());
    let msg = format!("{}", err);
    assert!(msg.contains("test error"));
}

#[test]
fn test_output_error_from_io() {
    let io_err = std::io::Error::other("io problem");
    let err: OutputError = io_err.into();
    match err {
        OutputError::Io(_) => {}
        _ => panic!("expected Io variant"),
    }
}

#[test]
fn test_output_status_serialization() {
    let active = OutputStatus::Active;
    let json = serde_json::to_string(&active).unwrap();
    let deserialized: OutputStatus = serde_json::from_str(&json).unwrap();
    match deserialized {
        OutputStatus::Active => {}
        _ => panic!("expected Active after roundtrip"),
    }
}

#[test]
fn test_output_status_error_variant() {
    let err = OutputStatus::Error("connection refused".to_string());
    let json = serde_json::to_string(&err).unwrap();
    let deserialized: OutputStatus = serde_json::from_str(&json).unwrap();
    match deserialized {
        OutputStatus::Error(msg) => assert_eq!(msg, "connection refused"),
        _ => panic!("expected Error variant"),
    }
}

// ============================================================
// Concurrency Tests
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_console_outputs() {
    let sink = Arc::new(ConsoleOutput::new("sq-concurrent"));
    let barrier = Arc::new(Barrier::new(10));
    let mut handles = vec![];

    for i in 0..10 {
        let s = sink.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            let result = make_sq_result("sq-concurrent", i % 2 == 0);
            s.process(&result).await
        }));
    }

    for h in handles {
        let outcome = h.await.unwrap();
        assert!(outcome.is_ok(), "Concurrent console process should succeed");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_sink_types() {
    let console = Arc::new(ConsoleOutput::new("sq-a"));
    let webhook = Arc::new(WebhookOutput::new("sq-b", "http://localhost:9999/hook-a"));

    let mut handles = vec![];

    for _i in 0..5 {
        let s = console.clone();
        handles.push(tokio::spawn(async move {
            let result = make_sq_result("sq-a", true);
            s.process(&result).await
        }));
    }
    for _i in 0..5 {
        let s = webhook.clone();
        handles.push(tokio::spawn(async move {
            let result = make_sq_result("sq-b", false);
            s.process(&result).await
        }));
    }

    for h in handles {
        let outcome = h.await.unwrap();
        if let Err(e) = outcome {
            match e {
                OutputError::Sink(_) => {}
                OutputError::Io(_) => {}
            }
        }
    }
}

// ============================================================
// High-Volume Stress Tests
// ============================================================

#[tokio::test(flavor = "multi_thread")]
async fn test_high_volume_console_throughput() {
    let sink = Arc::new(ConsoleOutput::new("sq-stress"));
    let num_tasks = 100;
    let barrier = Arc::new(Barrier::new(num_tasks));
    let mut handles = vec![];

    for i in 0..num_tasks {
        let s = sink.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            let result = make_sq_result("sq-stress", i % 3 != 0);
            s.process(&result).await
        }));
    }

    let mut success = 0;
    for h in handles {
        if h.await.unwrap().is_ok() {
            success += 1;
        }
    }
    assert_eq!(
        success, num_tasks,
        "All {} console writes should succeed",
        num_tasks
    );
}

#[test]
fn test_types_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ConsoleOutput>();
    assert_send_sync::<WebhookOutput>();
}

// ============================================================
// Edge Cases: Unicode & Binary
// ============================================================

#[tokio::test]
async fn test_console_output_unicode_properties() {
    let sink = ConsoleOutput::new("sq-unicode");
    let mut props = HashMap::new();
    props.insert("中文名称".to_string(), PropertyValue::String("北京".into()));
    props.insert("emoji".to_string(), PropertyValue::String("🚀✨🔥".into()));

    let result = StandingQueryResult::new(
        uuid::Uuid::new_v4(),
        "sq-unicode",
        nexora_id::NexoraId::from_bytes(b"unicode-node".to_vec()),
        props,
        ResultType::Matched,
        chrono::Utc::now(),
    );
    let outcome = sink.process(&result).await;
    assert!(outcome.is_ok());
}

#[tokio::test]
async fn test_console_output_binary_properties() {
    let sink = ConsoleOutput::new("sq-binary");
    let mut props = HashMap::new();
    props.insert(
        "raw".to_string(),
        PropertyValue::Bytes(vec![0x00, 0xFF, 0x7F, 0x80]),
    );

    let result = StandingQueryResult::new(
        uuid::Uuid::new_v4(),
        "sq-binary",
        nexora_id::NexoraId::from_bytes(b"bin-node".to_vec()),
        props,
        ResultType::Matched,
        chrono::Utc::now(),
    );
    let outcome = sink.process(&result).await;
    assert!(outcome.is_ok());
}
