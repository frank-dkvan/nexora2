# Nexora 安全加固指南

## 当前安全能力

| 能力 | 状态 | 说明 |
|------|------|------|
| HMAC-SHA256 API key 验证 | 已实现 | `crates/nexora-app/src/auth.rs` |
| WAL AES-256-GCM 加密 | 已实现（可选）| `--encrypt` 启动参数 |
| TLS | 通过反向代理 | Nginx / Caddy 终止 |
| RBAC | 已实现 | `readonly` / `readwrite` / `admin` 角色 |
| 请求速率限制 | 已实现 | Token bucket，`crates/nexora-app/src/security.rs` |
| 安全响应头 | 已实现 | `security_headers` 中间件 |

---

## 审计日志

Nexora 支持两级审计追踪：

### 1. HTTP 中间件级审计（`audit_log` middleware）

在 `security.rs` 中，所有 `POST / PUT / DELETE / PATCH` 请求都会被自动记录为结构化 JSON 日志。
启用持久化审计文件：

```bash
# 启动时加参数
nexora-app --audit-log /var/log/nexora/audit.jsonl
```

每条记录格式：

```json
{
  "timestamp": "2026-07-18T03:59:00Z",
  "client_ip": "10.0.0.1",
  "method": "POST",
  "path": "/api/v2/graph/node/abc/property/name",
  "status": 200,
  "user": "alice",
  "user_agent": "nexora-cli/1.0"
}
```

### 2. 业务操作级审计（`audit!` 宏）

关键写操作（`set_property`、`add_edge`、`admin/restore`）会额外发出带 `audit=true` 字段的
tracing 日志，可通过日志聚合工具（Loki / ELK）过滤：

```
{audit="true"} |= "set_property"
```

---

## 密钥轮转

### WAL 加密密钥轮转

在线轮转尚未实现（需要 WAL 重写，只能离线操作）。请参考：

```
POST /api/v2/admin/rotate-key
```

该接口返回分步操作指引，不执行实际轮转。具体步骤：

1. 排空节点：`POST /api/v2/admin/drain`
2. 停止进程
3. 备份 WAL 目录：`cp -r <wal_dir> <wal_dir>.bak`
4. 用新密钥重加密：`nexora-core wal reencrypt --old-key <old> --new-key <new> <wal_dir>`
5. 更新配置 / Secret Store 中的密钥
6. 用新密钥重启

**建议轮转周期：每 90 天**

---

## 多租户隔离

**当前限制：** Namespace 隔离是逻辑的，不是物理的。所有租户共享同一进程内存和 WAL。

**推荐方案：**

- 每租户独立进程 + 独立端口
- 用 Nginx upstream 做统一入口
- 每租户独立 WAL 目录 + 独立加密密钥

物理多租户隔离（内核级别）列入后续路线图。

---

## 安全加固清单

- [ ] 启用 WAL 加密：`--encrypt` + 强随机密钥（至少 256-bit）
- [ ] 为每个客户端创建独立 API key，分配最小权限角色
- [ ] 配置反向代理 TLS 终止（Nginx / Caddy）
- [ ] 启用审计日志持久化：`--audit-log /var/log/nexora/audit.jsonl`
- [ ] 将审计日志接入中央化日志系统（Loki / ELK）
- [ ] 制定密钥轮转计划（推荐每 90 天）
- [ ] 在生产环境中禁用 sample-data 端点
- [ ] 配置速率限制参数（默认 100 req/s，按需调整）
- [ ] 定期执行备份：`POST /api/v2/admin/backup`

---

## 漏洞报告

请通过 GitHub Issues（标记 `security`）或私信维护者报告安全问题。
