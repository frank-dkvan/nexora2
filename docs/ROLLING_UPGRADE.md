# Nexora 分布式图数据库滚动升级 Runbook

## 概述

本文档描述 Nexora 三节点集群的滚动升级流程，确保零停机升级和快速回滚能力。

## 前置条件

- [ ] 集群健康：所有节点 UP，无网络分区
- [ ] 复制健康：`nexora_replication_health_ratio >= 0.95` (查看 /metrics)
- [ ] 备份完成：最近 24h 内的 RocksDB 快照
- [ ] 版本兼容性：确认新版本与当前版本的 wire protocol 兼容
- [ ] 测试环境验证：新版本已在测试集群运行 72h 无故障

## 版本兼容性矩阵

| 当前版本 | 目标版本 | 兼容性 | 注意事项 |
|---------|---------|-------|---------|
| 0.1.x   | 0.1.y   | ✅ 完全兼容 | 补丁版本，可直接升级 |
| 0.1.x   | 0.2.0   | ⚠️ 需检查 | 小版本升级，检查 CHANGELOG |
| 0.x.y   | 1.0.0   | ❌ 不兼容 | 大版本升级，需全集群停机 |

## 升级流程

### 阶段 0：准备 (T-1h)

1. **通知用户**：提前 1 小时通知即将升级
2. **备份数据**：
   ```bash
   # 在每个节点执行
   cd /var/nexora/data
   tar czf backup-$(date +%Y%m%d-%H%M%S).tar.gz rocksdb/
   ```

3. **检查集群状态**：
   ```bash
   curl http://node-a:9091/metrics | grep nexora_replication_health_ratio
   # 期望: nexora_replication_health_ratio >= 0.95
   ```

4. **下载新版本二进制**：
   ```bash
   wget https://releases.nexora.io/nexora-v0.2.0-linux-x64.tar.gz
   tar xzf nexora-v0.2.0-linux-x64.tar.gz
   ./nexora --version  # 验证版本号
   ```

### 阶段 1：升级 Follower-1 (Node-B)

**节点角色**: Follower (非 Owner 分片的副本)

1. **停止节点**：
   ```bash
   systemctl stop nexora
   # 或使用优雅关闭 (如果支持)
   kill -TERM $(cat /var/run/nexora.pid)
   ```

2. **替换二进制**：
   ```bash
   cp /path/to/new/nexora /usr/local/bin/nexora
   chmod +x /usr/local/bin/nexora
   ```

3. **启动新版本**：
   ```bash
   systemctl start nexora
   # 查看启动日志
   journalctl -u nexora -f
   ```

4. **健康检查** (等待 3-5 分钟)：
   ```bash
   # 检查节点存活
   curl http://node-b:9091/health
   # 期望: 200 OK

   # 检查复制延迟
   curl http://node-a:9091/metrics | grep nexora_replication
   # 期望: node-b 开始接收 ack
   ```

5. **功能验证**：
   ```bash
   # 通过 PG-wire 执行简单查询
   psql -h node-b -U nexora -d graph -c "MATCH (n) RETURN count(n)"
   ```

6. **观察 15 分钟**：监控错误日志、复制指标、CPU/内存

### 阶段 2：升级 Follower-2 (Node-C)

**重复阶段 1 的所有步骤，目标节点改为 Node-C**

### 阶段 3：升级 Leader (Node-A)

**节点角色**: Owner (大部分分片的 Leader)

**⚠️ 关键步骤**：此步骤会触发 Owner 切换 (failover)

1. **确认 Follower 健康**：
   ```bash
   # Node-B 和 Node-C 必须都在线且复制正常
   curl http://node-b:9091/health && curl http://node-c:9091/health
   ```

2. **停止 Node-A**：
   ```bash
   systemctl stop nexora
   ```

3. **预期行为**：
   - Raft 心跳超时 (默认 5s * 1.5 = 7.5s)
   - Node-B 或 Node-C 自动选举为新 Leader
   - Owner epoch 递增，Fencing token 保护防止脑裂
   - 写入自动路由到新 Owner

4. **替换二进制并启动**：
   ```bash
   cp /path/to/new/nexora /usr/local/bin/nexora
   systemctl start nexora
   ```

5. **验证恢复**：
   ```bash
   # Node-A 作为 Follower 重新加入
   curl http://node-a:9091/health

   # 检查集群拓扑
   curl http://node-b:9091/metrics | grep nexora_cluster
   # 期望: 3 个节点都 alive
   ```

6. **观察 30 分钟**：
   - 查询延迟 P99 < 100ms
   - 写入成功率 > 99.9%
   - 无 fencing rejection 错误

### 阶段 4：验证与清理

1. **端到端测试**：
   ```bash
   # 执行生产流量的 10% 镜像流量
   ./smoke-test.sh --target node-a,node-b,node-c
   ```

2. **清理旧备份** (保留 7 天)：
   ```bash
   find /var/nexora/data -name "backup-*.tar.gz" -mtime +7 -delete
   ```

3. **更新监控面板**：标记升级事件，便于后续分析

## 回滚流程

**触发条件**：
- 新版本启动失败
- 复制健康率 < 0.8
- P99 延迟 > 500ms 持续 5 分钟
- 数据不一致错误

**回滚步骤**：

1. **立即停止所有新版本节点**：
   ```bash
   # 在已升级的节点执行
   systemctl stop nexora
   ```

2. **恢复旧版本二进制**：
   ```bash
   cp /usr/local/bin/nexora.backup /usr/local/bin/nexora
   systemctl start nexora
   ```

3. **数据恢复** (如果新版本写入了不兼容数据)：
   ```bash
   # 停止所有节点
   systemctl stop nexora
   
   # 恢复备份
   cd /var/nexora/data
   rm -rf rocksdb/
   tar xzf backup-<timestamp>.tar.gz
   
   # 重启旧版本
   systemctl start nexora
   ```

4. **验证集群恢复**：
   ```bash
   curl http://node-a:9091/health
   # 所有节点返回 200
   ```

## 常见问题排查

### 问题 1: 节点启动后立即崩溃

**症状**: `systemctl status nexora` 显示 failed

**排查**:
```bash
journalctl -u nexora -n 100 --no-pager
# 查找 panic 或 "fatal" 关键词
```

**可能原因**:
- 配置文件格式变更 (检查 CHANGELOG)
- RocksDB 格式不兼容 (大版本升级)
- 端口冲突

### 问题 2: 复制延迟飙升

**症状**: `nexora_replication_quorum_failed_total` 持续增长

**排查**:
```bash
curl http://node-a:9091/metrics | grep follower_nacks
# 找到哪个 follower 无响应
```

**可能原因**:
- 网络分区
- Follower 节点 CPU 100%
- 新版本性能回退

### 问题 3: Fencing rejection 错误

**症状**: 日志中大量 "stale epoch rejected"

**原因**: 旧版本节点使用过期的 epoch 尝试写入

**解决**:
- 确认所有节点已升级完成
- 重启出现 rejection 的节点
- 检查是否有僵尸进程 (`ps aux | grep nexora`)

## 监控检查清单

升级过程中持续监控以下指标 (建议设置 Grafana 告警):

- [ ] `nexora_replication_health_ratio >= 0.95`
- [ ] `nexora_replication_quorum_failed_total` 增长率 < 1/min
- [ ] Raft 选举次数 <= 预期 (follower 升级时 0 次, leader 升级时 1 次)
- [ ] 查询 P99 延迟 < 100ms
- [ ] CPU 使用率 < 70%
- [ ] 内存使用率 < 80%
- [ ] 无 OOM killer 事件
- [ ] 无 "split brain" 或 "data corruption" 错误

## 紧急联系

- **运维负责人**: on-call@company.com
- **开发负责人**: dev-lead@company.com
- **Nexora 社区**: https://github.com/nexora/nexora/issues

## 附录：自动化脚本

```bash
#!/bin/bash
# rolling-upgrade.sh - 自动化滚动升级脚本

set -euo pipefail

NODES=("node-a" "node-b" "node-c")
NEW_BINARY="/path/to/new/nexora"
HEALTH_CHECK_RETRIES=30

upgrade_node() {
    local node=$1
    echo "[$(date)] Upgrading $node..."
    
    # 停止服务
    ssh "$node" "sudo systemctl stop nexora"
    
    # 替换二进制
    scp "$NEW_BINARY" "$node:/tmp/nexora"
    ssh "$node" "sudo mv /tmp/nexora /usr/local/bin/nexora && sudo chmod +x /usr/local/bin/nexora"
    
    # 启动服务
    ssh "$node" "sudo systemctl start nexora"
    
    # 等待健康
    for i in $(seq 1 $HEALTH_CHECK_RETRIES); do
        if curl -sf "http://$node:9091/health" > /dev/null; then
            echo "[$(date)] $node is healthy"
            return 0
        fi
        echo "Waiting for $node to be healthy... ($i/$HEALTH_CHECK_RETRIES)"
        sleep 10
    done
    
    echo "[$(date)] ERROR: $node failed to become healthy"
    return 1
}

# Upgrade followers first, then leader
upgrade_node "node-b"
sleep 60
upgrade_node "node-c"
sleep 60
upgrade_node "node-a"

echo "[$(date)] Rolling upgrade completed successfully"
```

---

**文档版本**: v1.0  
**最后更新**: 2026-07-15  
**适用版本**: Nexora 0.1.x → 0.2.x
