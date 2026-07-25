# Nexora 性能基准测试方案

## 测试目标

验证 Nexora 分布式图数据库在生产负载下的性能表现，包括高并发写入能力和故障切换恢复时间。

## 测试环境

### 硬件配置
- **集群规模**: 3 节点
- **单节点**: 8 核 CPU, 32GB 内存, 500GB NVMe SSD
- **网络**: 10 Gbps, RTT < 1ms

### 软件版本
- Nexora: v0.2.0
- OS: Ubuntu 22.04 LTS
- Rust: 1.75+

## 测试场景

### 场景 1: 稳态高并发写入

**目标**: 验证系统在 1000+ QPS 写入下的稳定性和延迟表现

**测试参数**:
- 并发客户端: 50
- 持续时间: 30 分钟
- 写入 QPS: 1000 (每客户端 20 QPS)
- 操作类型: 70% SetProperty, 30% AddEdge
- 数据规模: 100万节点起始

**成功标准**:
- 吞吐量: >= 1000 QPS
- P50 延迟: < 20ms
- P99 延迟: < 100ms
- P999 延迟: < 500ms
- 错误率: < 0.01%

### 场景 2: 混合读写负载

**目标**: 验证读写混合场景下的性能

**测试参数**:
- 读写比: 80:20
- 总 QPS: 2000 (1600 读 + 400 写)
- 持续时间: 30 分钟
- 查询类型: 50% 点查, 30% 1-hop 遍历, 20% Cypher 聚合

**成功标准**:
- 读取 P99 延迟: < 50ms
- 写入 P99 延迟: < 100ms
- 吞吐量: >= 2000 QPS

### 场景 3: Leader 故障切换

**目标**: 测量系统在 Leader 节点宕机时的恢复时间

**测试步骤**:
1. 稳态写入 500 QPS
2. T+30s: kill Leader 节点进程
3. 观察 Follower 选举和路由恢复
4. 测量写入中断时长

**成功标准**:
- 选举时间: < 10s (Raft 心跳 5s * 1.5 + 选举 < 10s)
- 写入中断: < 15s (选举 + 客户端重试)
- 数据零丢失: 所有 quorum 提交的写入可恢复
- 恢复后吞吐量: >= 500 QPS

### 场景 4: 网络分区恢复

**目标**: 验证网络分区修复后的追赶性能

**测试步骤**:
1. 3 节点集群稳态运行
2. 隔离 Node-C (使用 iptables DROP)
3. 持续写入 5 分钟 (约 5000 次操作)
4. 恢复网络连接
5. 测量 Node-C 追赶时间

**成功标准**:
- 追赶速度: >= 1000 ops/s
- 追赶完成时间: < 10s (5000 ops / 1000 ops/s)
- 追赶期间集群可用性: >= 99%

## 测试工具实现

### 压测工具: `nexora-bench`

```rust
// tools/nexora-bench/src/main.rs
use clap::Parser;
use tokio_postgres::{Config, NoTls};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "localhost")]
    host: String,
    
    #[arg(long, default_value = "5432")]
    port: u16,
    
    #[arg(long, default_value = "50")]
    clients: usize,
    
    #[arg(long, default_value = "1000")]
    target_qps: usize,
    
    #[arg(long, default_value = "60")]
    duration_secs: u64,
    
    #[arg(long, default_value = "write")]
    workload: String, // write, read, mixed
}

struct BenchmarkStats {
    total_ops: AtomicU64,
    success: AtomicU64,
    errors: AtomicU64,
    latencies: Arc<hdrhistogram::Histogram<u64>>,
}

async fn run_benchmark(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let stats = Arc::new(BenchmarkStats {
        total_ops: AtomicU64::new(0),
        success: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        latencies: Arc::new(hdrhistogram::Histogram::new(3)?),
    });
    
    let ops_per_client = args.target_qps / args.clients;
    let interval = Duration::from_secs(1) / ops_per_client as u32;
    
    let mut handles = vec![];
    
    for client_id in 0..args.clients {
        let stats = stats.clone();
        let host = args.host.clone();
        let port = args.port;
        let duration = Duration::from_secs(args.duration_secs);
        let workload = args.workload.clone();
        
        handles.push(tokio::spawn(async move {
            let config = format!("host={} port={} user=nexora dbname=graph", host, port);
            let (client, connection) = tokio_postgres::connect(&config, NoTls).await?;
            
            tokio::spawn(connection);
            
            let start = Instant::now();
            while start.elapsed() < duration {
                let op_start = Instant::now();
                
                let result = match workload.as_str() {
                    "write" => execute_write(&client, client_id).await,
                    "read" => execute_read(&client, client_id).await,
                    _ => Ok(()),
                };
                
                let latency = op_start.elapsed().as_micros() as u64;
                stats.latencies.record(latency)?;
                stats.total_ops.fetch_add(1, Ordering::Relaxed);
                
                if result.is_ok() {
                    stats.success.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                }
                
                tokio::time::sleep(interval).await;
            }
            
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }));
    }
    
    // Wait for all clients
    for handle in handles {
        handle.await??;
    }
    
    // Print results
    print_stats(&stats, args.duration_secs);
    
    Ok(())
}

async fn execute_write(client: &tokio_postgres::Client, id: usize) -> Result<(), Box<dyn std::error::Error>> {
    let qid = format!("client-{}-", id, uuid::Uuid::new_v4());
    client.execute(
        "CREATE (n:User {id: $1, name: $2})",
        &[&qid, &format!("User-{}", id)],
    ).await?;
    Ok(())
}

async fn execute_read(client: &tokio_postgres::Client, id: usize) -> Result<(), Box<dyn std::error::Error>> {
    client.query(
        "MATCH (n:User) WHERE n.id = $1 RETURN n",
        &[&format!("client-{}", id)],
    ).await?;
    Ok(())
}

fn print_stats(stats: &BenchmarkStats, duration_secs: u64) {
    let total = stats.total_ops.load(Ordering::Relaxed);
    let success = stats.success.load(Ordering::Relaxed);
    let errors = stats.errors.load(Ordering::Relaxed);
    
    println!("\n=== Benchmark Results ===");
    println!("Duration: {}s", duration_secs);
    println!("Total ops: {}", total);
    println!("Success: {}", success);
    println!("Errors: {}", errors);
    println!("Error rate: {:.4}%", (errors as f64 / total as f64) * 100.0);
    println!("Throughput: {:.2} ops/s", total as f64 / duration_secs as f64);
    println!("\nLatency (microseconds):");
    println!("  P50: {}", stats.latencies.value_at_quantile(0.50));
    println!("  P95: {}", stats.latencies.value_at_quantile(0.95));
    println!("  P99: {}", stats.latencies.value_at_quantile(0.99));
    println!("  P999: {}", stats.latencies.value_at_quantile(0.999));
    println!("  Max: {}", stats.latencies.max());
}
```

### 故障注入工具: `chaos.sh`

```bash
#!/bin/bash
# chaos.sh - 故障注入脚本

set -euo pipefail

case "$1" in
    kill-leader)
        # 找到当前 Raft leader 并 kill
        LEADER=$(curl -s http://node-a:9091/metrics | grep 'raft_role{role="leader"}' | awk '{print $1}' | cut -d'{' -f1)
        echo "Killing leader: $LEADER"
        ssh "$LEADER" "sudo killall -9 nexora"
        ;;
    
    partition-node)
        # 隔离指定节点的网络
        NODE=$2
        echo "Partitioning $NODE from cluster"
        ssh "$NODE" "sudo iptables -A INPUT -p tcp --dport 7447 -j DROP"
        ssh "$NODE" "sudo iptables -A OUTPUT -p tcp --dport 7447 -j DROP"
        ;;
    
    heal-partition)
        # 恢复网络连接
        NODE=$2
        echo "Healing partition for $NODE"
        ssh "$NODE" "sudo iptables -F"
        ;;
    
    cpu-stress)
        # CPU 压力测试
        NODE=$2
        CORES=${3:-2}
        echo "Starting CPU stress on $NODE ($CORES cores)"
        ssh "$NODE" "stress-ng --cpu $CORES --timeout 60s &"
        ;;
    
    *)
        echo "Usage: $0 {kill-leader|partition-node <node>|heal-partition <node>|cpu-stress <node> <cores>}"
        exit 1
        ;;
esac
```

## 执行步骤

### 1. 准备环境

```bash
# 部署 3 节点集群
./deploy-cluster.sh node-a node-b node-c

# 预热数据 (100万节点)
./nexora-bench --host node-a --workload write --target-qps 5000 --duration-secs 200

# 验证集群健康
curl http://node-a:9091/health
curl http://node-a:9091/metrics | grep nexora_replication_health_ratio
```

### 2. 运行场景 1: 高并发写入

```bash
./nexora-bench \
    --host node-a \
    --workload write \
    --clients 50 \
    --target-qps 1000 \
    --duration-secs 1800

# 同时监控指标
watch -n 1 'curl -s http://node-a:9091/metrics | grep -E "(qps|latency|health)"'
```

### 3. 运行场景 3: 故障切换

```bash
# 启动后台写入
./nexora-bench --host node-a --workload write --target-qps 500 --duration-secs 300 &
BENCH_PID=$!

# 等待 30 秒
sleep 30

# 注入故障
./chaos.sh kill-leader

# 观察恢复 (等待 20 秒)
sleep 20

# 检查写入是否恢复
curl http://node-b:9091/metrics | grep nexora_replication_quorum_ok_total

# 等待压测完成
wait $BENCH_PID
```

## 结果报告模板

```markdown
# Nexora 性能基准测试报告

**测试日期**: 2026-07-15
**集群版本**: v0.2.0
**测试环境**: 3节点 AWS c5.2xlarge

## 场景 1: 高并发写入

| 指标 | 目标 | 实际 | 达标 |
|------|------|------|------|
| 吞吐量 | >= 1000 QPS | 1050 QPS | ✅ |
| P50 延迟 | < 20ms | 15ms | ✅ |
| P99 延迟 | < 100ms | 85ms | ✅ |
| P999 延迟 | < 500ms | 320ms | ✅ |
| 错误率 | < 0.01% | 0.005% | ✅ |

## 场景 3: Leader 故障切换

| 指标 | 目标 | 实际 | 达标 |
|------|------|------|------|
| 选举时间 | < 10s | 8.2s | ✅ |
| 写入中断 | < 15s | 12.5s | ✅ |
| 数据丢失 | 0 | 0 | ✅ |
| 恢复后 QPS | >= 500 | 520 | ✅ |

## 结论

Nexora 在所有测试场景下均达到或超过预期性能目标,具备生产部署条件。

**推荐下一步**: 进入灰度发布阶段
```

---

**文档版本**: v1.0  
**最后更新**: 2026-07-15
