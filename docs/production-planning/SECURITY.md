# Nexora 安全架构

**版本:** 1.0  
**日期:** 2026/07/05  
**状态:** 基础设计，待实现（P4）

---

## 1. 认证（Authentication）

### 1.1 OIDC / Keycloak 集成（P4.1）

```yaml
auth:
  enabled: true
  provider: oidc
  
  oidc:
    issuer_url: https://keycloak.example.com/realms/nexora
    client_id: nexora-api
    client_secret: ${OIDC_CLIENT_SECRET}
    
    # JWKS 端点（自动发现）
    jwks_url: https://keycloak.example.com/realms/nexora/protocol/openid-connect/certs
    
    # 用户信息映射
    user_id_claim: sub
    email_claim: email
    roles_claim: realm_access.roles
```

### 1.2 JWT Token 验证

```rust
pub struct JwtValidator {
    issuer: String,
    audience: String,
    jwks: JwkSet,
}

impl JwtValidator {
    pub async fn validate(&self, token: &str) -> Result<Claims> {
        // 1. 解码 JWT header
        let header = decode_header(token)?;
        
        // 2. 获取公钥
        let key = self.jwks.find(&header.kid)?;
        
        // 3. 验证签名
        let claims = decode::<Claims>(
            token,
            &DecodingKey::from_jwk(key)?,
            &Validation::new(header.alg)
        )?;
        
        // 4. 验证 issuer 和 audience
        if claims.iss != self.issuer {
            return Err("Invalid issuer");
        }
        
        Ok(claims.claims)
    }
}
```

### 1.3 HTTP API 认证

```rust
#[middleware]
async fn auth_middleware(req: Request) -> Result<Response> {
    // 1. 提取 Bearer Token
    let token = req.headers()
        .get("Authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    
    let token = token.ok_or("Missing authorization header")?;
    
    // 2. 验证 JWT
    let claims = jwt_validator.validate(token).await?;
    
    // 3. 注入用户上下文
    req.extensions_mut().insert(UserContext {
        user_id: claims.sub,
        email: claims.email,
        roles: claims.roles,
    });
    
    Ok(next(req).await)
}
```

---

## 2. 授权（Authorization）

### 2.1 RBAC 模型（P4.2）

```rust
pub struct RbacPolicy {
    pub roles: HashMap<String, Role>,
    pub permissions: HashMap<String, Permission>,
}

pub struct Role {
    pub name: String,
    pub permissions: Vec<String>,
    pub inherits: Vec<String>,  // 角色继承
}

pub enum Permission {
    // Namespace 权限
    AccessNamespace { namespace: String },
    
    // Label 权限
    ReadLabel { label: String },
    WriteLabel { label: String },
    
    // Property 权限
    ReadProperty { property: String },
    WriteProperty { property: String },
    MaskProperty { property: String },  // 脱敏
    
    // Query 权限
    ExecuteQuery,
    ExecuteMutation,
    
    // Standing Query 权限
    RegisterStandingQuery,
    RemoveStandingQuery,
    
    // Admin 权限
    ManageDomains,
    ManageUsers,
    ViewAuditLog,
}
```

### 2.2 权限配置示例

```yaml
rbac:
  roles:
    - name: viewer
      permissions:
        - read_any_label
        - execute_query
    
    - name: operator
      inherits: [viewer]
      permissions:
        - create_node
        - update_property
        - create_edge
    
    - name: admin
      inherits: [operator]
      permissions:
        - register_standing_query
        - manage_domains
        - view_audit_log
  
  users:
    - email: alice@example.com
      roles: [operator]
      namespaces: [airport_cargo, manufacturing]
    
    - email: bob@example.com
      roles: [viewer]
      namespaces: [airport_cargo]
```

### 2.3 细粒度权限检查

```rust
pub struct PermissionChecker {
    policy: RbacPolicy,
}

impl PermissionChecker {
    pub fn can_access_namespace(&self, user: &UserContext, namespace: &str) -> bool {
        for role in &user.roles {
            if self.policy.roles[role].has_permission(&format!("namespace:{}", namespace)) {
                return true;
            }
        }
        false
    }
    
    pub fn can_read_label(&self, user: &UserContext, label: &str) -> bool {
        // 检查用户角色是否有读取该 label 的权限
        for role in &user.roles {
            if self.policy.roles[role].has_permission(&format!("read_label:{}", label)) {
                return true;
            }
        }
        false
    }
    
    pub fn mask_properties(&self, user: &UserContext, properties: &mut BTreeMap<Symbol, PropertyValue>) {
        // 脱敏敏感属性
        let masked_props = self.get_masked_properties(user);
        for prop in masked_props {
            if let Some(value) = properties.get_mut(&prop) {
                *value = PropertyValue::String("***MASKED***".into());
            }
        }
    }
}
```

### 2.4 查询重写（Row-Level Security）

```rust
// 自动注入 namespace 过滤
let original_query = "MATCH (n:Device) RETURN n";
let rewritten_query = format!(
    "MATCH (n:Device) WHERE n.namespace IN {:?} RETURN n",
    user.allowed_namespaces
);
```

---

## 3. 审计日志（P4.3）

### 3.1 审计事件类型

```rust
pub enum AuditEvent {
    // 认证事件
    LoginSuccess { user: String, ip: String },
    LoginFailed { user: String, ip: String, reason: String },
    TokenRefreshed { user: String },
    
    // 查询事件
    QueryExecuted { user: String, query: String, namespace: Option<String>, latency_ms: u64 },
    QueryFailed { user: String, query: String, error: String },
    
    // Mutation 事件
    NodeCreated { user: String, node_id: NexoraId, labels: Vec<Symbol> },
    PropertySet { user: String, node_id: NexoraId, key: Symbol },
    EdgeAdded { user: String, src: NexoraId, edge_type: Symbol, dst: NexoraId },
    NodeDeleted { user: String, node_id: NexoraId },
    
    // Standing Query 事件
    StandingQueryRegistered { user: String, query_id: String, pattern: String },
    StandingQueryRemoved { user: String, query_id: String },
    
    // 管理事件
    DomainPackageLoaded { user: String, domain: String },
    RoleChanged { user: String, target_user: String, role: String },
    PermissionGranted { user: String, target_user: String, permission: String },
    
    // 安全事件
    UnauthorizedAccess { user: String, resource: String, action: String },
    SuspiciousActivity { user: String, description: String },
}
```

### 3.2 审计日志格式

```json
{
  "timestamp": "2026-07-05T10:30:15.123Z",
  "event_type": "QueryExecuted",
  "user": "alice@example.com",
  "user_id": "550e8400-e29b-41d4-a716-446655440000",
  "ip_address": "192.168.1.100",
  "namespace": "airport_cargo",
  "query": "MATCH (n:Device) WHERE n.status='RUNNING' RETURN n LIMIT 10",
  "result": "SUCCESS",
  "rows_returned": 10,
  "latency_ms": 45,
  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"
}
```

### 3.3 审计日志输出

```yaml
audit:
  enabled: true
  outputs:
    - type: file
      path: /var/log/nexora/audit.log
      rotation: daily
    
    - type: kafka
      topic: nexora-audit
      bootstrap_servers: kafka:9092
    
    - type: opensearch
      endpoint: https://opensearch.example.com:9200
      index: nexora-audit-logs
```

---

## 4. 数据加密

### 4.1 传输加密（TLS）

```yaml
tls:
  enabled: true
  
  # 服务端证书
  cert_file: /etc/nexora/tls/server.crt
  key_file: /etc/nexora/tls/server.key
  
  # CA 证书（mTLS）
  ca_file: /etc/nexora/tls/ca.crt
  
  # 客户端证书验证
  verify_client: true
```

### 4.2 WAL 加密（已实现）

```rust
pub struct EncryptedWal {
    cipher: Aes256Gcm,
    nonce_generator: NonceGenerator,
}

impl EncryptedWal {
    pub fn append(&mut self, mutation: GraphMutation) -> Result<()> {
        // 1. 序列化
        let plaintext = bincode::serialize(&mutation)?;
        
        // 2. 生成 nonce
        let nonce = self.nonce_generator.next();
        
        // 3. AES-256-GCM 加密
        let ciphertext = self.cipher.encrypt(&nonce, plaintext.as_ref())?;
        
        // 4. 写入磁盘
        self.write_encrypted_entry(nonce, ciphertext)?;
        
        Ok(())
    }
}
```

### 4.3 Property 加密（计划）

```cypher
-- 敏感属性自动加密
CREATE (u:User {
  id: 'user001',
  email: 'alice@example.com',
  password: ENCRYPT('secret123'),  -- 自动加密
  api_key: ENCRYPT('sk-12345...')
})

-- 查询时自动解密（需权限）
MATCH (u:User {id: 'user001'})
RETURN u.email, DECRYPT(u.password)
```

---

## 5. 网络安全

### 5.1 防火墙规则

```bash
# 只允许特定 IP 访问
iptables -A INPUT -p tcp --dport 8080 -s 192.168.1.0/24 -j ACCEPT
iptables -A INPUT -p tcp --dport 8080 -j DROP

# 限制连接速率（防 DDoS）
iptables -A INPUT -p tcp --dport 8080 -m limit --limit 100/s --limit-burst 200 -j ACCEPT
```

### 5.2 Rate Limiting

```rust
#[middleware]
async fn rate_limit_middleware(req: Request) -> Result<Response> {
    let user_id = req.extensions().get::<UserContext>()?.user_id;
    
    // 每用户限流：100 req/min
    if !rate_limiter.check_rate(user_id, 100, Duration::from_secs(60)) {
        return Err(Error::TooManyRequests);
    }
    
    Ok(next(req).await)
}
```

---

## 6. 安全最佳实践

### 6.1 密钥管理

```yaml
secrets:
  provider: vault  # HashiCorp Vault
  
  vault:
    address: https://vault.example.com:8200
    token: ${VAULT_TOKEN}
    
    # 密钥路径
    paths:
      oidc_client_secret: secret/nexora/oidc/client_secret
      wal_encryption_key: secret/nexora/wal/encryption_key
      jwt_signing_key: secret/nexora/jwt/signing_key
```

### 6.2 最小权限原则

```yaml
# 生产环境推荐配置
rbac:
  default_role: viewer  # 新用户默认只读
  
  roles:
    - name: viewer
      permissions:
        - read_own_namespace  # 只能读取自己的 namespace
        - execute_read_query  # 只能执行读查询
```

### 6.3 安全配置检查

```bash
#!/bin/bash
# scripts/security_check.sh

echo "检查 TLS 配置..."
if ! grep -q "tls.enabled: true" /etc/nexora/config.yaml; then
  echo "❌ TLS 未启用"
fi

echo "检查认证配置..."
if ! grep -q "auth.enabled: true" /etc/nexora/config.yaml; then
  echo "❌ 认证未启用"
fi

echo "检查审计日志..."
if ! grep -q "audit.enabled: true" /etc/nexora/config.yaml; then
  echo "⚠️  审计日志未启用"
fi
```

---

## 7. 合规性

### 7.1 GDPR 数据保护

- ✅ 个人数据加密存储
- ✅ 访问日志审计
- ✅ 数据删除能力（DELETE + 物理清理）
- ✅ 数据导出能力（EXPORT 查询）

### 7.2 SOC 2 合规

- ✅ 访问控制（RBAC）
- ✅ 审计日志（所有操作可追溯）
- ✅ 数据加密（传输 + 静态）
- ✅ 变更管理（版本控制）

---

## 8. 安全事件响应

### 8.1 异常检测

```prometheus
# 登录失败率过高
rate(nexora_login_failed_total[5m]) > 10

# 未授权访问尝试
nexora_unauthorized_access_total > 0
```

### 8.2 自动阻断

```rust
pub struct SecurityMonitor {
    blocked_ips: Arc<RwLock<HashSet<IpAddr>>>,
}

impl SecurityMonitor {
    pub async fn check_suspicious_activity(&self, user: &str, ip: IpAddr) {
        let failed_logins = self.get_failed_login_count(user, Duration::from_mins(5));
        
        if failed_logins > 5 {
            // 自动阻断 IP
            self.blocked_ips.write().await.insert(ip);
            
            // 发送告警
            self.alert_security_team(format!("IP {} blocked due to {} failed logins", ip, failed_logins));
        }
    }
}
```

---

## 9. 实现计划

### P4.1 — OIDC / Keycloak 集成（3-4天）
- [ ] JWT 验证
- [ ] JWKS 自动发现
- [ ] Token 刷新
- [ ] 用户信息映射

### P4.2 — RBAC 细粒度权限（5-6天）
- [ ] Role/Permission 模型
- [ ] Namespace 隔离
- [ ] Label 权限
- [ ] Property 脱敏
- [ ] 查询重写

### P4.3 — 审计日志（2-3天）
- [ ] 审计事件定义
- [ ] 多输出支持（File/Kafka/OpenSearch）
- [ ] 查询 API

---

**维护者:** Nexora Team  
**最后更新:** 2026/07/05
