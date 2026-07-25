//! Live MQTT smoke test — end-to-end publish → MqttSource.poll → graph.
//!
//! `#[ignore]` by default: requires a running MQTT broker (e.g. Mosquitto) and
//! is opted into with env vars, so it never blocks CI.
//!
//! Run against a local broker:
//! ```sh
//! docker run -d -p 1883:1883 eclipse-mosquitto:2 \
//!   mosquitto -c /mosquitto-no-auth.conf
//! MQTT_SMOKE_HOST=localhost cargo test -p nexora-stream --features mqtt \
//!   --test mqtt_smoke -- --ignored --nocapture
//! ```
//!
//! Verifies the push→poll adaptation for real: a published JSON message is
//! drained through the internal buffer, surfaced by `poll`, committed to the
//! graph via `GraphIngestHandler`, and read back.

#![cfg(feature = "mqtt")]

use nexora_core::{BatchDurability, GraphService, GraphServiceConfig, InMemoryPersistor};
use nexora_id::{NexoraId, PropertyValue};
use nexora_stream::{
    GraphIngestHandler, IngestHandler, IngestionSource, MqttSource, MqttSourceConfig,
};
use std::sync::Arc;
use std::time::Duration;

fn broker() -> Option<(String, u16)> {
    let host = std::env::var("MQTT_SMOKE_HOST").ok()?;
    let port = std::env::var("MQTT_SMOKE_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(1883);
    Some((host, port))
}

#[tokio::test]
#[ignore = "requires a live MQTT broker; set MQTT_SMOKE_HOST to run"]
async fn mqtt_publish_reaches_graph() {
    let Some((host, port)) = broker() else {
        eprintln!("MQTT_SMOKE_HOST not set — skipping");
        return;
    };

    let topic = format!("nexora/smoke/{}", std::process::id());
    let source = MqttSource::new(MqttSourceConfig {
        host: host.clone(),
        port,
        client_id: format!("nexora-smoke-{}", std::process::id()),
        topics: vec![topic.clone()],
        qos: 1,
        id_field: "id".into(),
        event_time_field: None,
        max_batch: 64,
        buffer_capacity: 1024,
    });
    source.connect().await.expect("connect");

    // Publish a message with an independent client.
    {
        use rumqttc::{AsyncClient, MqttOptions, QoS};
        let mut opts = MqttOptions::new(
            format!("nexora-smoke-pub-{}", std::process::id()),
            host,
            port,
        );
        opts.set_keep_alive(Duration::from_secs(10));
        let (pub_client, mut pub_loop) = AsyncClient::new(opts, 16);
        // Drive the publisher event loop in the background.
        let _pl = tokio::spawn(async move { while pub_loop.poll().await.is_ok() {} });
        // Give the subscriber a moment to establish the subscription.
        tokio::time::sleep(Duration::from_millis(300)).await;
        pub_client
            .publish(
                &topic,
                QoS::AtLeastOnce,
                false,
                br#"{"id":"mqtt-node","temp":21,"unit":"C"}"#.to_vec(),
            )
            .await
            .expect("publish");
    }

    // Poll until the record shows up (bounded retries).
    let graph = Arc::new(GraphService::new(
        GraphServiceConfig {
            num_shards: 2,
            max_nodes_per_shard: 1000,
            node_channel_size: 64,
        },
        Arc::new(InMemoryPersistor::new()),
    ));
    let handler = GraphIngestHandler::new(graph.clone(), BatchDurability::WaitDurable);

    let mut got = false;
    for _ in 0..50 {
        if let Some(batch) = source.poll().await.expect("poll") {
            handler.handle_batch(&batch).await.expect("handle");
            got = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(got, "no MQTT batch received within timeout");

    let qid = NexoraId::from_bytes(b"mqtt-node".to_vec());
    assert_eq!(
        graph.get_property(&qid, "temp").await.unwrap(),
        Some(PropertyValue::Integer(21)),
        "published temp must reach the graph"
    );
    assert_eq!(
        graph.get_property(&qid, "unit").await.unwrap(),
        Some(PropertyValue::String("C".into()))
    );

    source.close().await.expect("close");
}
