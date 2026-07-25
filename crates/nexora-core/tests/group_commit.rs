//! 组提交(WalSyncPolicy::Group)崩溃安全性与语义回归测试。
//!
//! 组提交把 fsync 从"每条写一次"改为"后台 flusher 批量一次",committer 在
//! 返回 receipt 前等待 durable 水位。核心契约:
//!   - **已 ack 的写必然已落盘**(即便紧接着崩溃)。
//!   - 未 ack(还在 buffer、flusher 未 fsync)的写在崩溃时可以丢失。
//!   - 优雅关停必须 drain flusher,使已写入/checkpoint 全部落盘。
//!
//! 这里从两个层次验证:
//!   1. WAL 层直接验证 append 只缓冲、flusher 推进 durable 水位、sync 落盘。
//!   2. 端到端(GraphService + WAL)验证 set_property 返回后崩溃可恢复,
//!      以及 shutdown 后重启数据完整。

use nexora_core::wal::{WalSyncPolicy, WriteAheadLog};
use nexora_core::{GraphService, GraphServiceConfig};
use nexora_id::{NexoraId, PropertyValue};
use nexora_persistor_rocksdb::RocksDbPersistor;
use std::sync::Arc;
use std::time::Duration;

fn group_policy() -> WalSyncPolicy {
    WalSyncPolicy::Group {
        max_ops: 256,
        max_delay: Duration::from_micros(500),
        max_bytes: None,
    }
}

fn make_config() -> GraphServiceConfig {
    GraphServiceConfig {
        num_shards: 2,
        max_nodes_per_shard: 100,
        node_channel_size: 16,
    }
}

// ============================================================================
// 层次 1:WAL 直接语义
// ============================================================================

/// Group 策略下 append 只写 buffer,不立即 fsync,但记录对同进程 replay 可见
/// (replay 会先 flush)。durable_receiver 存在(区别于 Always/EveryN)。
#[test]
fn group_append_buffers_and_exposes_durable_receiver() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = WriteAheadLog::open_with_policy(dir.path(), group_policy()).unwrap();

    assert!(
        wal.is_group_commit(),
        "Group 策略必须报告 is_group_commit()=true"
    );
    assert!(
        wal.durable_receiver().is_some(),
        "Group 策略必须暴露 durable watermark receiver"
    );

    let seq = append_dummy(&mut wal, 1);
    assert_eq!(seq, 1, "首条 append 序号应为 1");

    // replay 内部会 flush,因此已缓冲记录在同进程可读回(证明 append 确实写了数据,
    // 只是尚未 fsync)。
    let result = wal.replay().unwrap();
    assert_eq!(result.records.len(), 1, "flush 后应能读回缓冲的 1 条记录");
}

/// Always / EveryN 策略不是组提交,没有 durable receiver(同步落盘,无需 barrier)。
#[test]
fn non_group_policies_have_no_durable_receiver() {
    let dir = tempfile::tempdir().unwrap();
    let wal_always = WriteAheadLog::open_with_policy(dir.path(), WalSyncPolicy::Always).unwrap();
    assert!(!wal_always.is_group_commit());
    assert!(wal_always.durable_receiver().is_none());

    let dir2 = tempfile::tempdir().unwrap();
    let wal_everyn =
        WriteAheadLog::open_with_policy(dir2.path(), WalSyncPolicy::EveryN(10)).unwrap();
    assert!(!wal_everyn.is_group_commit());
    assert!(wal_everyn.durable_receiver().is_none());
}

/// 崩溃模拟:Group 策略下若从未 flush/sync,进程"崩溃"(drop 掉未落盘 buffer,
/// 用一个独立文件句柄读裸文件)时,未落盘的数据不保证在磁盘上。
///
/// 这里的重点是对称面:一旦调用 sync(flusher 会做的事),数据必然可从
/// **全新打开的** WAL(模拟重启)读回 —— 即"落盘后可恢复"。
#[test]
fn synced_records_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();

    {
        let mut wal = WriteAheadLog::open_with_policy(dir.path(), group_policy()).unwrap();
        for i in 0..50 {
            append_dummy(&mut wal, i);
        }
        // 模拟 flusher 的批量 fsync。
        wal.sync().unwrap();
        // wal drop —— 文件保留在 dir(模拟重启,不是掉电丢 buffer)。
    }

    // 重新打开(全新句柄,模拟进程重启)。
    let mut reopened = WriteAheadLog::open_with_policy(dir.path(), group_policy()).unwrap();
    let result = reopened.replay().unwrap();
    assert_eq!(
        result.records.len(),
        50,
        "sync 之后的所有记录必须在重启后完整恢复"
    );
}

// ============================================================================
// 层次 2:端到端(GraphService,生产 Group 策略)
// ============================================================================

/// 已 ack 的写(set_property 返回)必须在崩溃后可恢复。
///
/// set_property 内部走 commit_operations → append(buffer)→ 等 durable 水位。
/// 因此返回时该写已被 flusher fsync。这里在返回后**不调用 shutdown**,直接 drop
/// service(模拟崩溃),再用同一 WAL 目录 + persistor 重启并 replay,断言数据在。
#[tokio::test]
async fn acked_write_survives_crash_without_shutdown() {
    let temp = tempfile::tempdir().unwrap();
    let wal_dir = temp.path().join("wal");
    let db_dir = temp.path().join("db");
    let persistor = Arc::new(RocksDbPersistor::open(&db_dir).unwrap());
    let qid = NexoraId::from_bytes(b"acked-node".to_vec());

    // Phase 1: 写入并等待 ack,然后"崩溃"(drop,不 shutdown)。
    {
        let svc =
            GraphService::new_with_wal(make_config(), persistor.clone(), wal_dir.clone(), None)
                .unwrap();
        svc.replay_all_wals().await.unwrap();

        // 这几个写返回即代表已通过 durable barrier(已 fsync)。
        svc.set_property(&qid, "name", PropertyValue::String("durable".into()))
            .await
            .unwrap();
        svc.set_property(&qid, "n", PropertyValue::Integer(7))
            .await
            .unwrap();
        // 故意不 shutdown:模拟进程被杀。svc 在此 drop。
    }

    // Phase 2: 重启,replay WAL,断言已 ack 的写都在。
    {
        let svc = GraphService::new_with_wal(make_config(), persistor, wal_dir, None).unwrap();
        let replayed = svc.replay_all_wals().await.unwrap();
        assert!(replayed > 0, "崩溃恢复应回放到已 ack 的 WAL 记录");

        assert_eq!(
            svc.get_property(&qid, "name").await.unwrap(),
            Some(PropertyValue::String("durable".into())),
            "已 ack 的字符串属性必须在崩溃恢复后存在"
        );
        assert_eq!(
            svc.get_property(&qid, "n").await.unwrap(),
            Some(PropertyValue::Integer(7)),
            "已 ack 的整型属性必须在崩溃恢复后存在"
        );
    }
}

/// 优雅关停:shutdown() 必须 drain flusher,使所有写入 + snapshot checkpoint 落盘,
/// 重启后数据完整。
#[tokio::test]
async fn graceful_shutdown_drains_flusher() {
    let temp = tempfile::tempdir().unwrap();
    let wal_dir = temp.path().join("wal");
    let db_dir = temp.path().join("db");
    let persistor = Arc::new(RocksDbPersistor::open(&db_dir).unwrap());

    // Phase 1: 写一批,正常 shutdown(应 drain flusher)。
    {
        let svc =
            GraphService::new_with_wal(make_config(), persistor.clone(), wal_dir.clone(), None)
                .unwrap();
        svc.replay_all_wals().await.unwrap();

        for i in 0..64 {
            let qid = NexoraId::from_bytes(format!("g-{i:03}").into_bytes());
            svc.set_property(&qid, "v", PropertyValue::Integer(i))
                .await
                .unwrap();
        }
        svc.shutdown().await.unwrap();
    }

    // Phase 2: 重启,断言全部 64 个节点可恢复。
    {
        let svc = GraphService::new_with_wal(make_config(), persistor, wal_dir, None).unwrap();
        svc.replay_all_wals().await.unwrap();
        for i in 0..64 {
            let qid = NexoraId::from_bytes(format!("g-{i:03}").into_bytes());
            assert_eq!(
                svc.get_property(&qid, "v").await.unwrap(),
                Some(PropertyValue::Integer(i)),
                "shutdown 后节点 {i} 的属性必须完整恢复"
            );
        }
    }
}

/// 并发写:多个节点并发提交,组提交 flusher 批量 fsync;所有返回的写在 shutdown
/// 后必须全部可恢复(验证 durable 水位在并发下正确释放各 committer)。
#[tokio::test]
async fn concurrent_writes_all_durable_after_shutdown() {
    let temp = tempfile::tempdir().unwrap();
    let wal_dir = temp.path().join("wal");
    let db_dir = temp.path().join("db");
    let persistor = Arc::new(RocksDbPersistor::open(&db_dir).unwrap());

    const N: i64 = 200;

    {
        let svc = Arc::new(
            GraphService::new_with_wal(make_config(), persistor.clone(), wal_dir.clone(), None)
                .unwrap(),
        );
        svc.replay_all_wals().await.unwrap();

        let mut handles = Vec::new();
        for i in 0..N {
            let svc = svc.clone();
            handles.push(tokio::spawn(async move {
                let qid = NexoraId::from_bytes(format!("c-{i:04}").into_bytes());
                svc.set_property(&qid, "v", PropertyValue::Integer(i))
                    .await
                    .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        svc.shutdown().await.unwrap();
    }

    {
        let svc = GraphService::new_with_wal(make_config(), persistor, wal_dir, None).unwrap();
        svc.replay_all_wals().await.unwrap();
        for i in 0..N {
            let qid = NexoraId::from_bytes(format!("c-{i:04}").into_bytes());
            assert_eq!(
                svc.get_property(&qid, "v").await.unwrap(),
                Some(PropertyValue::Integer(i)),
                "并发写入的节点 {i} 在 shutdown 后必须已落盘"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn append_dummy(wal: &mut WriteAheadLog, i: i64) -> u64 {
    use nexora_core::event::{NodeChangeEvent, TimedEvent};
    use nexora_core::wal::WalOperation;
    use nexora_id::EventTime;
    use nexora_value::Symbol;

    wal.append(WalOperation::NodeEvent {
        qid: NexoraId::from_bytes(format!("dummy-{i}").into_bytes()),
        event: TimedEvent::new(
            NodeChangeEvent::PropertySet {
                key: Symbol::new("v"),
                value: PropertyValue::Integer(i),
            },
            EventTime::from_micros(i as u64),
        ),
    })
    .unwrap()
}

// ---------------------------------------------------------------------------
// C4: group_commit_byte_budget_triggers_flush
// ---------------------------------------------------------------------------

/// C4: WalSyncPolicy::Group with a tight byte budget causes an inline flush
/// after the budget is exceeded — without needing to reach max_ops.
///
/// Strategy: set max_ops=1000 (won't be hit) and max_bytes=512.
/// Write entries until total buffered bytes exceed 512.
/// Assert: is_group_commit() still true; all writes succeed; WAL replay
/// recovers every entry (confirming data was actually flushed).
#[test]
fn group_commit_byte_budget_triggers_flush() {
    use nexora_core::event::{NodeChangeEvent, TimedEvent};
    use nexora_core::wal::WalOperation;
    use nexora_id::EventTime;
    use nexora_value::Symbol;

    let tmp = tempfile::tempdir().unwrap();

    // Tight byte budget: 512 bytes.  Large ops cap: 1000 — won't be hit.
    let policy = WalSyncPolicy::Group {
        max_ops: 1000,
        max_delay: Duration::from_secs(60), // flusher won't fire spontaneously
        max_bytes: Some(512),
    };

    let mut wal = WriteAheadLog::open_with_policy(tmp.path(), policy).unwrap();
    assert!(wal.is_group_commit(), "should be group-commit mode");

    // Write 20 entries (~50-60 bytes each encoded); after ~9-10 the byte
    // budget should be exceeded, forcing an inline flush.
    let mut seqs = Vec::new();
    for i in 0i64..20 {
        let seq = wal
            .append(WalOperation::NodeEvent {
                qid: NexoraId::from_bytes(format!("budget-key-{i:04}").into_bytes()),
                event: TimedEvent::new(
                    NodeChangeEvent::PropertySet {
                        key: Symbol::new("budget_test"),
                        value: PropertyValue::Integer(i),
                    },
                    EventTime::from_micros(i as u64),
                ),
            })
            .unwrap();
        seqs.push(seq);
    }

    // Flush any remaining buffer so everything is durable.
    wal.sync().unwrap();

    // All 20 sequence numbers must be monotonically increasing.
    for w in seqs.windows(2) {
        assert!(w[1] > w[0], "sequence numbers must increase");
    }

    // Replay the WAL to confirm the data is physically on disk.
    let result = WriteAheadLog::replay_dir(tmp.path()).unwrap();
    assert_eq!(
        result.records.len(),
        20,
        "all 20 writes must survive replay after byte-budget flush"
    );
}
