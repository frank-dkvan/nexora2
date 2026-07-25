//! A0: materialized-view definitions and rows must survive a restart.
//!
//! Before A0, `MaterializedViewManager::with_rocksdb` opened the DB but never
//! read it back, so `mv:def:*`/`mv:data:*` on disk were orphaned and every view
//! vanished on restart. These tests reopen the same RocksDB path with a fresh
//! manager instance (simulating a process restart) and assert the definitions,
//! rows, and refresh-mode changes come back.

use nexora_core::materialized_view::{
    ColumnDef, DataType, MaterializedRow, MaterializedViewManager, RefreshMode,
};
use nexora_id::PropertyValue;
use std::collections::HashMap;

fn temp_dir() -> std::path::PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("nexora-mv-restart-{}", uuid::Uuid::new_v4()));
    p
}

#[tokio::test]
async fn mv_definitions_and_rows_survive_restart() {
    let dir = temp_dir();

    let (view_id, mv_name) = {
        let mgr = MaterializedViewManager::with_rocksdb(&dir).unwrap();
        let id = mgr
            .create_view(
                "high_speed".to_string(),
                "MATCH (n:Forklift) WHERE n.speed > 100 RETURN n".to_string(),
                vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::String,
                    },
                    ColumnDef {
                        name: "speed".to_string(),
                        data_type: DataType::Float,
                    },
                ],
                RefreshMode::Incremental,
            )
            .await
            .unwrap();

        let mut values = HashMap::new();
        values.insert("id".to_string(), PropertyValue::String("f001".into()));
        values.insert("speed".to_string(), PropertyValue::Float(150.0));
        mgr.upsert_row(
            &id,
            MaterializedRow {
                key: "f001".to_string(),
                values,
                version: 1,
                updated_at: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();

        (id, "high_speed".to_string())
    }; // manager dropped here — simulates process shutdown

    // Reopen: fresh manager over the same path.
    let mgr2 = MaterializedViewManager::with_rocksdb(&dir).unwrap();

    // Definition survived.
    let view = mgr2.get_view(&view_id).await;
    assert!(view.is_some(), "view definition should survive restart");
    let view = view.unwrap();
    assert_eq!(view.name, mv_name);
    assert_eq!(view.schema.len(), 2);

    // Lookup by name works (index rebuilt).
    assert_eq!(
        mgr2.find_view_by_name(&mv_name).await,
        Some(view_id.clone())
    );

    // Row survived.
    let row = mgr2.get_row(&view_id, "f001").await.unwrap();
    assert!(row.is_some(), "row should survive restart");
    assert_eq!(
        row.unwrap().values.get("speed"),
        Some(&PropertyValue::Float(150.0))
    );

    // Secondary index rebuilt from loaded rows.
    let by_speed = mgr2
        .query_by_column(&view_id, "speed", &PropertyValue::Float(150.0))
        .await
        .unwrap();
    assert_eq!(
        by_speed.len(),
        1,
        "secondary index should be rebuilt on load"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn mv_refresh_mode_change_survives_restart() {
    let dir = temp_dir();

    let view_id = {
        let mgr = MaterializedViewManager::with_rocksdb(&dir).unwrap();
        let id = mgr
            .create_view(
                "v".to_string(),
                "MATCH (n) RETURN n".to_string(),
                vec![],
                RefreshMode::Incremental,
            )
            .await
            .unwrap();
        mgr.set_refresh_mode(&id, RefreshMode::Manual)
            .await
            .unwrap();
        id
    };

    let mgr2 = MaterializedViewManager::with_rocksdb(&dir).unwrap();
    let view = mgr2.get_view(&view_id).await.unwrap();
    assert_eq!(
        view.refresh_mode,
        RefreshMode::Manual,
        "refresh-mode change should survive restart"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn mv_drop_is_durable_across_restart() {
    let dir = temp_dir();

    let view_id = {
        let mgr = MaterializedViewManager::with_rocksdb(&dir).unwrap();
        let id = mgr
            .create_view(
                "v".to_string(),
                "MATCH (n) RETURN n".to_string(),
                vec![],
                RefreshMode::Incremental,
            )
            .await
            .unwrap();
        mgr.drop_view(&id).await.unwrap();
        id
    };

    let mgr2 = MaterializedViewManager::with_rocksdb(&dir).unwrap();
    assert!(
        mgr2.get_view(&view_id).await.is_none(),
        "dropped view must not reappear after restart"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
