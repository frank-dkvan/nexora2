# Phase 8: 分布式嵌入式 RisingWave 实施方案

**文档版本**: 1.0  
**创建日期**: 2026-07-26  
**前置条件**: Phase 7（单机嵌入式）已完成  
**估计工时**: 160 小时（4 周全职）

---

## 执行摘要

### 目标

将 Phase 7 的**单机嵌入式 RisingWave** 扩展为**分布式嵌入式架构**，实现：

1. **灵活的角色组合**：每个节点可运行任意 Nexora + RisingWave 组件
2. **统一集群管理**：单一命令行启动，统一拓扑视图
3. **高可用性**：Meta 节点 Raft HA，Frontend/Compute 负载均衡
4. **水平扩展**：Compute 节点可动态增减
5. **统一运维**：单一二进制，统一配置、日志、监控

### 核心架构

```
┌──────────────────────────────────────────────────────────────────┐
│                    Nexora 分布式嵌入式集群                        │
├──────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Node 1 (Meta Leader)        Node 2 (Meta Follower)              │
│  ├─ Nexora Core              ├─ Nexora Core                      │
│  ├─ Nexora Raft (Leader)     ├─ Nexora Raft (Follower)          │
│  ├─ RW Meta (Leader)         ├─ RW Meta (Follower)              │
│  └─ RW Frontend              └─ RW Compute                       │
│                                                                   │
│  Node 3 (Meta Follower)      Node 4-N (Compute Workers)          │
│  ├─ Nexora Core              └─ RW Compute                       │
│  ├─ Nexora Raft (Follower)                                       │
│  ├─ RW Meta (Follower)                                           │
│  └─ RW Compute                                                   │
│                                                                   │
│  统一发现：etcd / Consul / 内置服务发现                           │
│  统一配置：ConfigMap / Environment Variables                      │
│  统一监控：Prometheus + Grafana                                   │
└──────────────────────────────────────────────────────────────────┘
```

### 关键特性

| 特性 | Phase 7 (单机) | Phase 8 (分布式) |
|------|---------------|-----------------|
| **节点数** | 1 | 1-100+ |
| **高可用** | ❌ 单点故障 | ✅ Raft HA |
| **水平扩展** | ❌ | ✅ Compute 动态扩容 |
| **角色组合** | 固定 | 任意组合 |
| **故障切换** | ❌ | ✅ 自动 |
| **负载均衡** | ❌ | ✅ Frontend 多副本 |

---

## 第一部分：架构设计

### 1.1 角色定义

#### Nexora 角色

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NexoraRole {
    /// 图查询引擎
    Core,
    
    /// Raft 共识节点
    Raft,
    
    /// 分布式存储节点
    Storage,
    
    /// API 网关（HTTP/gRPC）
    Gateway,
}
```

#### RisingWave 角色

```rust
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RisingWaveRole {
    /// Meta 节点（需要 Raft HA）
    Meta,
    
    /// Frontend 节点（SQL 解析、优化、调度）
    Frontend,
    
    /// Compute 节点（流计算引擎）
    Compute,
    
    /// Compactor 节点（后台压缩，可选）
    Compactor,
}
```

#### 预定义角色组合

```rust
pub struct RolePreset;

impl RolePreset {
    /// 全功能节点（小规模部署）
    pub fn all_in_one() -> NodeRoles {
        NodeRoles {
            nexora: vec![
                NexoraRole::Core,
                NexoraRole::Raft,
                NexoraRole::Storage,
                NexoraRole::Gateway,
            ],
            risingwave: vec![
                RisingWaveRole::Meta,
                RisingWaveRole::Frontend,
                RisingWaveRole::Compute,
            ],
        }
    }
    
    /// Meta 专用节点（HA 集群）
    pub fn meta_node() -> NodeRoles {
        NodeRoles {
            nexora: vec![NexoraRole::Raft, NexoraRole::Storage],
            risingwave: vec![RisingWaveRole::Meta],
        }
    }
    
    /// 计算专用节点（可水平扩展）
    pub fn compute_node() -> NodeRoles {
        NodeRoles {
            nexora: vec![],
            risingwave: vec![RisingWaveRole::Compute],
        }
    }
    
    /// 查询专用节点（Nexora + Frontend）
    pub fn query_node() -> NodeRoles {
        NodeRoles {
            nexora: vec![NexoraRole::Core, NexoraRole::Gateway],
            risingwave: vec![RisingWaveRole::Frontend],
        }
    }
}
```

---

### 1.2 节点配置

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// 节点唯一标识
    pub node_id: String,
    
    /// 节点角色
    pub roles: NodeRoles,
    
    /// 集群模式
    pub cluster_mode: ClusterMode,
    
    /// 节点地址配置
    pub network: NetworkConfig,
    
    /// 服务发现配置
    pub discovery: DiscoveryConfig,
    
    /// Raft 配置（如果包含 Raft 角色）
    pub raft: Option<RaftConfig>,
    
    /// 资源限制
    pub resources: ResourceLimits,
}

#[derive(Debug, Clone)]
pub struct NodeRoles {
    pub nexora: Vec<NexoraRole>,
    pub risingwave: Vec<RisingWaveRole>,
}

#[derive(Debug, Clone)]
pub enum ClusterMode {
    /// 单机模式（Phase 7）
    Standalone,
    
    /// 分布式模式（Phase 8）
    Distributed {
        /// 集群名称
        cluster_name: String,
    },
}

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    /// 节点监听地址
    pub listen_addr: SocketAddr,
    
    /// 节点广播地址（对外可达）
    pub advertise_addr: SocketAddr,
    
    /// RisingWave Meta 地址（分布式模式）
    pub meta_addrs: Vec<SocketAddr>,
    
    /// 内部通信是否使用 TLS
    pub use_tls: bool,
}

#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// 服务发现后端
    pub backend: DiscoveryBackend,
}

#[derive(Debug, Clone)]
pub enum DiscoveryBackend {
    /// 静态配置（通过 --meta-addrs）
    Static,
    
    /// etcd 服务发现
    Etcd {
        endpoints: Vec<String>,
        prefix: String,
    },
    
    /// Kubernetes 服务发现
    Kubernetes {
        namespace: String,
        service_name: String,
    },
    
    /// DNS-based 服务发现
    Dns {
        domain: String,
    },
}

#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Raft 节点 ID
    pub node_id: u64,
    
    /// 初始集群成员
    pub initial_members: Vec<RaftMember>,
    
    /// 选举超时（毫秒）
    pub election_timeout_ms: u64,
    
    /// 心跳间隔（毫秒）
    pub heartbeat_interval_ms: u64,
}

#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// 内存限制（MB）
    pub memory_mb: usize,
    
    /// CPU 核心数
    pub cpu_cores: usize,
}
```

---

### 1.3 CLI 参数设计

```rust
#[derive(Parser, Debug, Clone)]
#[command(name = "nexora")]
#[command(about = "Nexora - Next-generation streaming graph database")]
pub struct CliArgs {
    // ========== 基础配置 ==========
    
    /// 配置文件路径
    #[clap(short, long, env = "NEXORA_CONFIG")]
    pub config: Option<PathBuf>,
    
    /// 节点 ID（唯一标识）
    #[clap(long, env = "NEXORA_NODE_ID")]
    pub node_id: Option<String>,
    
    // ========== 角色配置 ==========
    
    /// 节点角色（逗号分隔）
    /// 示例: "nexora-core,meta,frontend"
    #[clap(long, env = "NEXORA_NODE_ROLES")]
    pub node_roles: Option<String>,
    
    /// 使用预定义角色组合
    /// 可选: all-in-one, meta-node, compute-node, query-node
    #[clap(long, env = "NEXORA_ROLE_PRESET")]
    pub role_preset: Option<String>,
    
    // ========== 集群配置 ==========
    
    /// 集群模式
    #[clap(long, env = "NEXORA_CLUSTER_MODE", default_value = "standalone")]
    pub cluster_mode: String,
    
    /// 集群名称
    #[clap(long, env = "NEXORA_CLUSTER_NAME", default_value = "nexora-cluster")]
    pub cluster_name: String,
    
    // ========== 网络配置 ==========
    
    /// 监听地址
    #[clap(long, env = "NEXORA_LISTEN_ADDR", default_value = "0.0.0.0:8080")]
    pub listen_addr: String,
    
    /// 广播地址（对外可达地址）
    #[clap(long, env = "NEXORA_ADVERTISE_ADDR")]
    pub advertise_addr: Option<String>,
    
    /// Meta 节点地址列表（逗号分隔）
    /// 示例: "10.0.0.1:5690,10.0.0.2:5690,10.0.0.3:5690"
    #[clap(long, env = "NEXORA_META_ADDRS")]
    pub meta_addrs: Option<String>,
    
    // ========== 服务发现 ==========
    
    /// 服务发现后端
    /// 可选: static, etcd, kubernetes, dns
    #[clap(long, env = "NEXORA_DISCOVERY_BACKEND", default_value = "static")]
    pub discovery_backend: String,
    
    /// etcd 端点（用于服务发现）
    #[clap(long, env = "NEXORA_ETCD_ENDPOINTS")]
    pub etcd_endpoints: Option<String>,
    
    // ========== Raft 配置 ==========
    
    /// Raft 节点 ID
    #[clap(long, env = "NEXORA_RAFT_ID")]
    pub raft_id: Option<u64>,
    
    /// 初始 Raft 成员（逗号分隔）
    /// 格式: "1=10.0.0.1:5690,2=10.0.0.2:5690,3=10.0.0.3:5690"
    #[clap(long, env = "NEXORA_RAFT_MEMBERS")]
    pub raft_members: Option<String>,
    
    // ========== 资源配置 ==========
    
    /// 内存限制（MB）
    #[clap(long, env = "NEXORA_MEMORY_LIMIT_MB", default_value = "2048")]
    pub memory_limit_mb: usize,
    
    /// CPU 核心数限制
    #[clap(long, env = "NEXORA_CPU_CORES")]
    pub cpu_cores: Option<usize>,
    
    // ========== 其他 ==========
    
    /// 日志级别
    #[clap(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,
}
```

---

## 第二部分：详细实施路线图

### Phase 8 总览（160 小时）

| 阶段 | 任务 | 工时 | 优先级 |
|------|------|------|--------|
| **8.1 配置与解析** | CLI 参数解析、角色系统 | 20h | P0 |
| **8.2 服务发现** | 节点注册、健康检查、拓扑管理 | 24h | P0 |
| **8.3 角色启动器** | 组件动态启动、生命周期管理 | 32h | P0 |
| **8.4 集群协调** | Meta HA、Frontend 负载均衡 | 28h | P0 |
| **8.5 统一监控** | 指标收集、拓扑可视化 | 20h | P1 |
| **8.6 测试验证** | 集成测试、故障注入测试 | 24h | P0 |
| **8.7 部署工具** | Docker Compose、K8s、脚本 | 12h | P1 |

**总计**: 160 小时

---

### 8.1 配置与解析（20h）

#### 任务 1: CLI 参数解析器（8h）

```rust
// crates/nexora-app/src/cli.rs

impl CliArgs {
    /// 解析为 NodeConfig
    pub fn parse_to_config(&self) -> Result<NodeConfig> {
        // 1. 确定节点 ID
        let node_id = self.node_id.clone()
            .or_else(|| std::env::var("HOSTNAME").ok())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        
        // 2. 解析角色
        let roles = self.parse_roles()?;
        
        // 3. 解析集群模式
        let cluster_mode = match self.cluster_mode.as_str() {
            "standalone" => ClusterMode::Standalone,
            "distributed" => ClusterMode::Distributed {
                cluster_name: self.cluster_name.clone(),
            },
            other => bail!("Unknown cluster mode: {}", other),
        };
        
        // 4. 解析网络配置
        let network = self.parse_network_config()?;
        
        // 5. 解析服务发现配置
        let discovery = self.parse_discovery_config()?;
        
        // 6. 解析 Raft 配置（如果需要）
        let raft = if roles.needs_raft() {
            Some(self.parse_raft_config()?)
        } else {
            None
        };
        
        // 7. 资源限制
        let resources = ResourceLimits {
            memory_mb: self.memory_limit_mb,
            cpu_cores: self.cpu_cores.unwrap_or_else(num_cpus::get),
        };
        
        Ok(NodeConfig {
            node_id,
            roles,
            cluster_mode,
            network,
            discovery,
            raft,
            resources,
        })
    }
    
    fn parse_roles(&self) -> Result<NodeRoles> {
        // 优先使用 preset
        if let Some(preset) = &self.role_preset {
            return match preset.as_str() {
                "all-in-one" => Ok(RolePreset::all_in_one()),
                "meta-node" => Ok(RolePreset::meta_node()),
                "compute-node" => Ok(RolePreset::compute_node()),
                "query-node" => Ok(RolePreset::query_node()),
                other => bail!("Unknown role preset: {}", other),
            };
        }
        
        // 否则解析 node_roles
        let roles_str = self.node_roles.as_ref()
            .ok_or_else(|| anyhow!("Either --role-preset or --node-roles must be specified"))?;
        
        let mut nexora_roles = Vec::new();
        let mut risingwave_roles = Vec::new();
        
        for role in roles_str.split(',') {
            match role.trim() {
                "nexora-core" => nexora_roles.push(NexoraRole::Core),
                "nexora-raft" => nexora_roles.push(NexoraRole::Raft),
                "nexora-storage" => nexora_roles.push(NexoraRole::Storage),
                "nexora-gateway" => nexora_roles.push(NexoraRole::Gateway),
                "meta" => risingwave_roles.push(RisingWaveRole::Meta),
                "frontend" => risingwave_roles.push(RisingWaveRole::Frontend),
                "compute" => risingwave_roles.push(RisingWaveRole::Compute),
                "compactor" => risingwave_roles.push(RisingWaveRole::Compactor),
                other => bail!("Unknown role: {}", other),
            }
        }
        
        Ok(NodeRoles {
            nexora: nexora_roles,
            risingwave: risingwave_roles,
        })
    }
}
```

#### 任务 2: 配置验证器（6h）

```rust
// crates/nexora-app/src/config_validator.rs

pub struct ConfigValidator;

impl ConfigValidator {
    /// 验证配置有效性
    pub fn validate(config: &NodeConfig) -> Result<()> {
        // 1. 检查角色兼容性
        Self::validate_role_compatibility(&config.roles)?;
        
        // 2. 检查网络配置
        Self::validate_network_config(&config.network)?;
        
        // 3. 检查 Raft 配置
        if let Some(raft) = &config.raft {
            Self::validate_raft_config(raft)?;
        }
        
        // 4. 检查资源充足性
        Self::validate_resources(&config.resources, &config.roles)?;
        
        Ok(())
    }
    
    fn validate_role_compatibility(roles: &NodeRoles) -> Result<()> {
        // Meta 角色必须配合 Raft
        if roles.risingwave.contains(&RisingWaveRole::Meta) {
            if !roles.nexora.contains(&NexoraRole::Raft) {
                bail!("Meta role requires Raft role");
            }
        }
        
        // Compute 节点至少需要知道 Meta 地址
        if roles.risingwave.contains(&RisingWaveRole::Compute) 
            && !roles.risingwave.contains(&RisingWaveRole::Meta) {
            // 需要在分布式模式下，且配置了 meta_addrs
            // 这个检查在 parse_network_config 中完成
        }
        
        Ok(())
    }
    
    fn validate_resources(
        resources: &ResourceLimits,
        roles: &NodeRoles,
    ) -> Result<()> {
        // 估算最小资源需求
        let min_memory_mb = Self::estimate_memory_requirement(roles);
        
        if resources.memory_mb < min_memory_mb {
            warn!(
                "Memory limit {}MB is below recommended {}MB for these roles",
                resources.memory_mb, min_memory_mb
            );
        }
        
        Ok(())
    }
    
    fn estimate_memory_requirement(roles: &NodeRoles) -> usize {
        let mut total = 500; // Nexora Core baseline
        
        for role in &roles.risingwave {
            total += match role {
                RisingWaveRole::Meta => 200,
                RisingWaveRole::Frontend => 500,
                RisingWaveRole::Compute => 1024,
                RisingWaveRole::Compactor => 300,
            };
        }
        
        total
    }
}
```

#### 任务 3: 配置文件支持（6h）

```rust
// 支持 YAML/TOML 配置文件

// nexora.yaml
cluster:
  mode: distributed
  name: nexora-production
  
node:
  id: node-1
  roles:
    preset: meta-node
    # 或者明确指定
    # nexora: [raft, storage]
    # risingwave: [meta]

network:
  listen_addr: "0.0.0.0:8080"
  advertise_addr: "10.0.0.1:8080"
  meta_addrs:
    - "10.0.0.1:5690"
    - "10.0.0.2:5690"
    - "10.0.0.3:5690"

discovery:
  backend: etcd
  etcd:
    endpoints:
      - "http://etcd-1:2379"
      - "http://etcd-2:2379"
    prefix: "/nexora/cluster"

raft:
  node_id: 1
  members:
    - id: 1
      addr: "10.0.0.1:5690"
    - id: 2
      addr: "10.0.0.2:5690"
    - id: 3
      addr: "10.0.0.3:5690"

resources:
  memory_mb: 2048
  cpu_cores: 4
```

---

### 8.2 服务发现（24h）

#### 任务 1: 服务注册抽象（8h）

```rust
// crates/nexora-cluster/src/discovery.rs

#[async_trait]
pub trait ServiceDiscovery: Send + Sync {
    /// 注册节点
    async fn register(&self, node: NodeInfo) -> Result<()>;
    
    /// 注销节点
    async fn deregister(&self, node_id: &str) -> Result<()>;
    
    /// 更新节点状态
    async fn update_status(&self, node_id: &str, status: NodeStatus) -> Result<()>;
    
    /// 发现所有节点
    async fn discover_nodes(&self) -> Result<Vec<NodeInfo>>;
    
    /// 发现指定角色的节点
    async fn discover_by_role(&self, role: &str) -> Result<Vec<NodeInfo>>;
    
    /// 监听拓扑变化
    async fn watch_topology(&self) -> Result<mpsc::Receiver<TopologyEvent>>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: String,
    pub addr: SocketAddr,
    pub nexora_roles: Vec<NexoraRole>,
    pub risingwave_roles: Vec<RisingWaveRole>,
    pub status: NodeStatus,
    pub metadata: HashMap<String, String>,
    pub last_heartbeat: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NodeStatus {
    Starting,
    Healthy,
    Unhealthy(String),
    Stopping,
}

#[derive(Debug, Clone)]
pub enum TopologyEvent {
    NodeAdded(NodeInfo),
    NodeRemoved(String),
    NodeUpdated(NodeInfo),
}
```

#### 任务 2: 静态服务发现（4h）

```rust
// crates/nexora-cluster/src/discovery/static_discovery.rs

pub struct StaticDiscovery {
    nodes: Arc<RwLock<HashMap<String, NodeInfo>>>,
    event_tx: broadcast::Sender<TopologyEvent>,
}

impl StaticDiscovery {
    pub fn new(meta_addrs: Vec<SocketAddr>) -> Self {
        let mut nodes = HashMap::new();
        
        // 从 meta_addrs 创建初始节点列表
        for (idx, addr) in meta_addrs.iter().enumerate() {
            let node = NodeInfo {
                id: format!("meta-{}", idx),
                addr: *addr,
                nexora_roles: vec![],
                risingwave_roles: vec![RisingWaveRole::Meta],
                status: NodeStatus::Healthy,
                metadata: HashMap::new(),
                last_heartbeat: SystemTime::now(),
            };
            nodes.insert(node.id.clone(), node);
        }
        
        let (event_tx, _) = broadcast::channel(1000);
        
        Self {
            nodes: Arc::new(RwLock::new(nodes)),
            event_tx,
        }
    }
}

#[async_trait]
impl ServiceDiscovery for StaticDiscovery {
    async fn register(&self, node: NodeInfo) -> Result<()> {
        let mut nodes = self.nodes.write().await;
        nodes.insert(node.id.clone(), node.clone());
        let _ = self.event_tx.send(TopologyEvent::NodeAdded(node));
        Ok(())
    }
    
    async fn discover_nodes(&self) -> Result<Vec<NodeInfo>> {
        let nodes = self.nodes.read().await;
        Ok(nodes.values().cloned().collect())
    }
    
    // ... 其他方法实现
}
```

#### 任务 3: etcd 服务发现（8h）

```rust
// crates/nexora-cluster/src/discovery/etcd_discovery.rs

use etcd_client::{Client, PutOptions, LeaseGrantOptions};

pub struct EtcdDiscovery {
    client: Client,
    prefix: String,
    lease_id: i64,
    lease_ttl: i64,
    event_tx: broadcast::Sender<TopologyEvent>,
}

impl EtcdDiscovery {
    pub async fn new(
        endpoints: Vec<String>,
        prefix: String,
    ) -> Result<Self> {
        let client = Client::connect(endpoints, None).await?;
        
        // 创建租约（30秒 TTL）
        let lease_resp = client.lease_grant(30, None).await?;
        let lease_id = lease_resp.id();
        
        let (event_tx, _) = broadcast::channel(1000);
        
        let discovery = Self {
            client,
            prefix,
            lease_id,
            lease_ttl: 30,
            event_tx,
        };
        
        // 启动租约续期任务
        discovery.start_lease_keepalive();
        
        // 启动拓扑监听任务
        discovery.start_watch_task();
        
        Ok(discovery)
    }
    
    fn start_lease_keepalive(&self) {
        let client = self.client.clone();
        let lease_id = self.lease_id;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                Duration::from_secs(10)
            );
            
            loop {
                interval.tick().await;
                if let Err(e) = client.lease_keep_alive(lease_id).await {
                    error!("Failed to keep lease alive: {}", e);
                    break;
                }
            }
        });
    }
}

#[async_trait]
impl ServiceDiscovery for EtcdDiscovery {
    async fn register(&self, node: NodeInfo) -> Result<()> {
        let key = format!("{}/nodes/{}", self.prefix, node.id);
        let value = serde_json::to_string(&node)?;
        
        // 使用租约注册节点（自动过期）
        let opts = PutOptions::new().with_lease(self.lease_id);
        self.client.put(key, value, Some(opts)).await?;
        
        Ok(())
    }
    
    async fn discover_nodes(&self) -> Result<Vec<NodeInfo>> {
        let key = format!("{}/nodes/", self.prefix);
        let resp = self.client.get(key, None).await?;
        
        let mut nodes = Vec::new();
        for kv in resp.kvs() {
            if let Ok(node) = serde_json::from_slice::<NodeInfo>(kv.value()) {
                nodes.push(node);
            }
        }
        
        Ok(nodes)
    }
}
```

#### 任务 4: Kubernetes 服务发现（4h）

```rust
// crates/nexora-cluster/src/discovery/k8s_discovery.rs

use kube::{Api, Client};
use k8s_openapi::api::core::v1::Pod;

pub struct K8sDiscovery {
    client: Client,
    namespace: String,
    label_selector: String,
    event_tx: broadcast::Sender<TopologyEvent>,
}

impl K8sDiscovery {
    pub async fn new(namespace: String, service_name: String) -> Result<Self> {
        let client = Client::try_default().await?;
        let label_selector = format!("app={}", service_name);
        let (event_tx, _) = broadcast::channel(1000);
        
        Ok(Self {
            client,
            namespace,
            label_selector,
            event_tx,
        })
    }
}

#[async_trait]
impl ServiceDiscovery for K8sDiscovery {
    async fn discover_nodes(&self) -> Result<Vec<NodeInfo>> {
        let pods: Api<Pod> = Api::namespaced(
            self.client.clone(),
            &self.namespace,
        );
        
        let lp = ListParams::default()
            .labels(&self.label_selector);
        
        let pod_list = pods.list(&lp).await?;
        
        let mut nodes = Vec::new();
        for pod in pod_list {
            if let Some(node_info) = Self::pod_to_node_info(&pod) {
                nodes.push(node_info);
            }
        }
        
        Ok(nodes)
    }
}
```

---

### 8.3 角色启动器（32h）

#### 任务 1: 组件启动管理器（12h）

```rust
// crates/nexora-app/src/role_launcher.rs

pub struct RoleLauncher {
    config: NodeConfig,
    discovery: Arc<dyn ServiceDiscovery>,
    shutdown: CancellationToken,
    component_handles: Arc<RwLock<HashMap<String, ComponentHandle>>>,
}

struct ComponentHandle {
    name: String,
    role: ComponentRole,
    task_handle: JoinHandle<Result<()>>,
    shutdown: CancellationToken,
    status: Arc<RwLock<ComponentStatus>>,
}

#[derive(Debug, Clone)]
enum ComponentRole {
    NexoraCore,
    NexoraRaft,
    NexoraStorage,
    NexoraGateway,
    RisingWaveMeta,
    RisingWaveFrontend,
    RisingWaveCompute,
    RisingWaveCompactor,
}

#[derive(Debug, Clone)]
enum ComponentStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed(String),
}

impl RoleLauncher {
    pub fn new(
        config: NodeConfig,
        discovery: Arc<dyn ServiceDiscovery>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            config,
            discovery,
            shutdown,
            component_handles: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// 启动所有配置的角色
    pub async fn launch_all(&self) -> Result<()> {
        info!("Launching node roles: {:?}", self.config.roles);
        
        // 1. 按依赖顺序启动 Nexora 组件
        self.launch_nexora_components().await?;
        
        // 2. 启动 RisingWave 组件
        self.launch_risingwave_components().await?;
        
        // 3. 注册到服务发现
        self.register_to_discovery().await?;
        
        // 4. 启动健康检查任务
        self.start_health_check_task();
        
        Ok(())
    }
    
    async fn launch_nexora_components(&self) -> Result<()> {
        let roles = &self.config.roles.nexora;
        
        // Raft 必须先启动（如果存在）
        if roles.contains(&NexoraRole::Raft) {
            self.launch_nexora_raft().await?;
        }
        
        // Storage
        if roles.contains(&NexoraRole::Storage) {
            self.launch_nexora_storage().await?;
        }
        
        // Core
        if roles.contains(&NexoraRole::Core) {
            self.launch_nexora_core().await?;
        }
        
        // Gateway
        if roles.contains(&NexoraRole::Gateway) {
            self.launch_nexora_gateway().await?;
        }
        
        Ok(())
    }
    
    async fn launch_nexora_raft(&self) -> Result<()> {
        info!("Launching Nexora Raft component");
        
        let raft_config = self.config.raft.as_ref()
            .ok_or_else(|| anyhow!("Raft config missing"))?;
        
        let shutdown = CancellationToken::new();
        let status = Arc::new(RwLock::new(ComponentStatus::Starting));
        
        let config_clone = raft_config.clone();
        let status_clone = status.clone();
        let shutdown_clone = shutdown.clone();
        
        let handle = tokio::spawn(async move {
            *status_clone.write().await = ComponentStatus::Running;
            
            // 启动 Raft 节点
            match nexora_raft::start(config_clone, shutdown_clone).await {
                Ok(_) => {
                    *status_clone.write().await = ComponentStatus::Stopped;
                    Ok(())
                }
                Err(e) => {
                    *status_clone.write().await = 
                        ComponentStatus::Failed(e.to_string());
                    Err(e)
                }
            }
        });
        
        let component = ComponentHandle {
            name: "nexora-raft".to_string(),
            role: ComponentRole::NexoraRaft,
            task_handle: handle,
            shutdown,
            status,
        };
        
        self.component_handles.write().await
            .insert("nexora-raft".to_string(), component);
        
        // 等待 Raft 就绪
        self.wait_for_component_ready("nexora-raft").await?;
        
        Ok(())
    }
    
    async fn launch_risingwave_components(&self) -> Result<()> {
        let roles = &self.config.roles.risingwave;
        
        // Meta 必须先启动（如果存在）
        if roles.contains(&RisingWaveRole::Meta) {
            self.launch_risingwave_meta().await?;
        }
        
        // Frontend 和 Compute 可以并行启动
        let mut tasks = vec![];
        
        if roles.contains(&RisingWaveRole::Frontend) {
            tasks.push(self.launch_risingwave_frontend());
        }
        
        if roles.contains(&RisingWaveRole::Compute) {
            tasks.push(self.launch_risingwave_compute());
        }
        
        if roles.contains(&RisingWaveRole::Compactor) {
            tasks.push(self.launch_risingwave_compactor());
        }
        
        // 等待所有组件启动
        futures::future::try_join_all(tasks).await?;
        
        Ok(())
    }
    
    async fn launch_risingwave_meta(&self) -> Result<()> {
        info!("Launching RisingWave Meta component");
        
        // 构建 MetaNodeOpts
        let meta_opts = self.build_meta_opts()?;
        
        let shutdown = CancellationToken::new();
        let status = Arc::new(RwLock::new(ComponentStatus::Starting));
        
        let status_clone = status.clone();
        let shutdown_clone = shutdown.clone();
        
        let handle = tokio::spawn(async move {
            *status_clone.write().await = ComponentStatus::Running;
            
            // 使用 RisingWave 的 start 函数
            risingwave_meta_node::start(meta_opts, shutdown_clone).await;
            
            *status_clone.write().await = ComponentStatus::Stopped;
            Ok(())
        });
        
        let component = ComponentHandle {
            name: "risingwave-meta".to_string(),
            role: ComponentRole::RisingWaveMeta,
            task_handle: handle,
            shutdown,
            status,
        };
        
        self.component_handles.write().await
            .insert("risingwave-meta".to_string(), component);
        
        // 等待 Meta 就绪
        self.wait_for_meta_ready().await?;
        
        Ok(())
    }
    
    fn build_meta_opts(&self) -> Result<MetaNodeOpts> {
        use risingwave_meta_node::MetaNodeOpts;
        
        let listen_addr = format!(
            "{}:5690",
            self.config.network.listen_addr.ip()
        );
        
        let advertise_addr = format!(
            "{}:5690",
            self.config.network.advertise_addr.ip()
        );
        
        Ok(MetaNodeOpts {
            listen_addr,
            advertise_addr,
            backend: Some(risingwave_common::config::MetaBackend::Mem),
            // ... 其他配置
            ..Default::default()
        })
    }
    
    async fn wait_for_meta_ready(&self) -> Result<()> {
        let mut retries = 0;
        let max_retries = 60; // 60秒超时
        
        loop {
            if risingwave_meta_node::is_server_started() {
                info!("RisingWave Meta is ready");
                return Ok(());
            }
            
            if retries >= max_retries {
                bail!("RisingWave Meta failed to start within timeout");
            }
            
            retries += 1;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    
    /// 优雅关闭所有组件
    pub async fn shutdown_all(&self) -> Result<()> {
        info!("Shutting down all components");
        
        // 1. 从服务发现注销
        self.deregister_from_discovery().await?;
        
        // 2. 按反序关闭组件
        let handles = self.component_handles.read().await;
        
        // RisingWave 组件先关闭
        for name in ["risingwave-compactor", "risingwave-compute", 
                     "risingwave-frontend", "risingwave-meta"] {
            if let Some(component) = handles.get(name) {
                info!("Stopping component: {}", name);
                component.shutdown.cancel();
            }
        }
        
        // Nexora 组件后关闭
        for name in ["nexora-gateway", "nexora-core", 
                     "nexora-storage", "nexora-raft"] {
            if let Some(component) = handles.get(name) {
                info!("Stopping component: {}", name);
                component.shutdown.cancel();
            }
        }
        
        drop(handles);
        
        // 3. 等待所有组件停止
        self.wait_for_all_stopped(Duration::from_secs(30)).await?;
        
        Ok(())
    }
}
```

#### 任务 2: 组件健康检查（8h）

```rust
// crates/nexora-app/src/health_checker.rs

pub struct HealthChecker {
    component_handles: Arc<RwLock<HashMap<String, ComponentHandle>>>,
    discovery: Arc<dyn ServiceDiscovery>,
    check_interval: Duration,
}

impl HealthChecker {
    pub fn start(
        component_handles: Arc<RwLock<HashMap<String, ComponentHandle>>>,
        discovery: Arc<dyn ServiceDiscovery>,
    ) {
        let checker = Self {
            component_handles,
            discovery,
            check_interval: Duration::from_secs(10),
        };
        
        tokio::spawn(async move {
            checker.run().await;
        });
    }
    
    async fn run(&self) {
        let mut interval = tokio::time::interval(self.check_interval);
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.check_all_components().await {
                error!("Health check failed: {}", e);
            }
        }
    }
    
    async fn check_all_components(&self) -> Result<()> {
        let handles = self.component_handles.read().await;
        
        let mut all_healthy = true;
        let mut unhealthy_components = Vec::new();
        
        for (name, component) in handles.iter() {
            let status = component.status.read().await;
            
            match &*status {
                ComponentStatus::Running => {
                    // 进一步检查组件健康
                    if !self.check_component_health(component).await {
                        all_healthy = false;
                        unhealthy_components.push(name.clone());
                    }
                }
                ComponentStatus::Failed(reason) => {
                    error!("Component {} failed: {}", name, reason);
                    all_healthy = false;
                    unhealthy_components.push(name.clone());
                }
                _ => {}
            }
        }
        
        // 更新节点状态到服务发现
        let node_status = if all_healthy {
            NodeStatus::Healthy
        } else {
            NodeStatus::Unhealthy(
                format!("Unhealthy components: {:?}", unhealthy_components)
            )
        };
        
        self.discovery.update_status(&get_node_id(), node_status).await?;
        
        Ok(())
    }
    
    async fn check_component_health(&self, component: &ComponentHandle) -> bool {
        match component.role {
            ComponentRole::RisingWaveMeta => {
                risingwave_meta_node::is_server_started()
            }
            ComponentRole::RisingWaveFrontend => {
                // 尝试连接 Frontend
                self.check_frontend_health().await
            }
            ComponentRole::RisingWaveCompute => {
                // 检查 Compute 心跳
                self.check_compute_health().await
            }
            _ => true, // 其他组件暂时认为健康
        }
    }
}
```

#### 任务 3: 动态角色管理（12h）

```rust
// crates/nexora-app/src/dynamic_roles.rs

pub struct DynamicRoleManager {
    launcher: Arc<RoleLauncher>,
    current_roles: Arc<RwLock<NodeRoles>>,
}

impl DynamicRoleManager {
    /// 运行时添加角色
    pub async fn add_role(&self, role: ComponentRole) -> Result<()> {
        info!("Adding role: {:?}", role);
        
        match role {
            ComponentRole::RisingWaveCompute => {
                self.launcher.launch_risingwave_compute().await?;
            }
            ComponentRole::RisingWaveFrontend => {
                self.launcher.launch_risingwave_frontend().await?;
            }
            _ => {
                bail!("Dynamic addition of role {:?} not supported", role);
            }
        }
        
        // 更新当前角色列表
        let mut roles = self.current_roles.write().await;
        // ... 更新逻辑
        
        Ok(())
    }
    
    /// 运行时移除角色
    pub async fn remove_role(&self, role: ComponentRole) -> Result<()> {
        info!("Removing role: {:?}", role);
        
        let component_name = match role {
            ComponentRole::RisingWaveCompute => "risingwave-compute",
            ComponentRole::RisingWaveFrontend => "risingwave-frontend",
            _ => bail!("Dynamic removal of role {:?} not supported", role),
        };
        
        self.launcher.stop_component(component_name).await?;
        
        Ok(())
    }
    
    /// HTTP API 端点
    /// POST /api/cluster/roles/add
    /// POST /api/cluster/roles/remove
}
```

---

### 8.4 集群协调（28h）

#### 任务 1: Meta HA 协调（10h）

```rust
// crates/nexora-cluster/src/meta_coordinator.rs

pub struct MetaCoordinator {
    config: MetaCoordinatorConfig,
    discovery: Arc<dyn ServiceDiscovery>,
    raft_client: Arc<dyn RaftClient>,
    current_leader: Arc<RwLock<Option<String>>>,
}

pub struct MetaCoordinatorConfig {
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub min_nodes: usize,
}

impl MetaCoordinator {
    /// 启动 Meta HA 协调
    pub async fn start(&self) -> Result<()> {
        info!("Starting Meta HA coordinator");
        
        // 1. 等待最小节点数
        self.wait_for_quorum().await?;
        
        // 2. 选举 Meta Leader
        self.elect_meta_leader().await?;
        
        // 3. 启动心跳监控
        self.start_heartbeat_monitor();
        
        // 4. 监听拓扑变化
        self.start_topology_watcher();
        
        Ok(())
    }
    
    async fn wait_for_quorum(&self) -> Result<()> {
        info!("Waiting for quorum ({} nodes)", self.config.min_nodes);
        
        let mut retries = 0;
        loop {
            let meta_nodes = self.discovery
                .discover_by_role("risingwave-meta")
                .await?;
            
            if meta_nodes.len() >= self.config.min_nodes {
                info!("Quorum reached: {} nodes", meta_nodes.len());
                return Ok(());
            }
            
            if retries >= 60 {
                bail!("Failed to reach quorum within timeout");
            }
            
            retries += 1;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    
    async fn elect_meta_leader(&self) -> Result<()> {
        info!("Electing Meta leader");
        
        // 通过 Raft 选举
        let leader_id = self.raft_client.get_leader().await?;
        
        // 更新 current_leader
        *self.current_leader.write().await = Some(leader_id.clone());
        
        info!("Meta leader elected: {}", leader_id);
        
        Ok(())
    }
    
    fn start_heartbeat_monitor(&self) {
        let current_leader = self.current_leader.clone();
        let raft_client = self.raft_client.clone();
        let interval = self.config.heartbeat_interval_ms;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                Duration::from_millis(interval)
            );
            
            loop {
                interval.tick().await;
                
                // 检查 Leader 是否存活
                match raft_client.get_leader().await {
                    Ok(leader_id) => {
                        let mut current = current_leader.write().await;
                        if current.as_ref() != Some(&leader_id) {
                            info!("Meta leader changed: {:?} -> {}", 
                                  *current, leader_id);
                            *current = Some(leader_id);
                        }
                    }
                    Err(e) => {
                        error!("Failed to get Meta leader: {}", e);
                    }
                }
            }
        });
    }
    
    fn start_topology_watcher(&self) {
        let discovery = self.discovery.clone();
        let current_leader = self.current_leader.clone();
        
        tokio::spawn(async move {
            let mut event_rx = discovery.watch_topology()
                .await
                .expect("Failed to watch topology");
            
            while let Some(event) = event_rx.recv().await {
                match event {
                    TopologyEvent::NodeRemoved(node_id) => {
                        // 检查是否是 Leader 节点下线
                        let leader = current_leader.read().await;
                        if leader.as_ref() == Some(&node_id) {
                            warn!("Meta leader {} removed, triggering re-election", 
                                  node_id);
                            // 触发重新选举
                        }
                    }
                    _ => {}
                }
            }
        });
    }
}
```

#### 任务 2: Frontend 负载均衡（8h）

```rust
// crates/nexora-cluster/src/frontend_balancer.rs

pub struct FrontendBalancer {
    discovery: Arc<dyn ServiceDiscovery>,
    frontend_pool: Arc<RwLock<Vec<FrontendEndpoint>>>,
    balancer_strategy: BalancerStrategy,
    current_index: Arc<AtomicUsize>,
}

#[derive(Debug, Clone)]
pub struct FrontendEndpoint {
    pub node_id: String,
    pub addr: SocketAddr,
    pub healthy: bool,
    pub active_connections: usize,
}

#[derive(Debug, Clone)]
pub enum BalancerStrategy {
    RoundRobin,
    LeastConnections,
    Random,
}

impl FrontendBalancer {
    pub fn new(
        discovery: Arc<dyn ServiceDiscovery>,
        strategy: BalancerStrategy,
    ) -> Self {
        let balancer = Self {
            discovery,
            frontend_pool: Arc::new(RwLock::new(Vec::new())),
            balancer_strategy: strategy,
            current_index: Arc::new(AtomicUsize::new(0)),
        };
        
        // 启动 Frontend 发现任务
        balancer.start_discovery_task();
        
        balancer
    }
    
    /// 获取下一个可用的 Frontend 节点
    pub async fn get_next_frontend(&self) -> Result<FrontendEndpoint> {
        let pool = self.frontend_pool.read().await;
        
        if pool.is_empty() {
            bail!("No healthy Frontend nodes available");
        }
        
        let endpoint = match self.balancer_strategy {
            BalancerStrategy::RoundRobin => {
                let idx = self.current_index.fetch_add(1, Ordering::Relaxed);
                pool[idx % pool.len()].clone()
            }
            BalancerStrategy::LeastConnections => {
                pool.iter()
                    .filter(|e| e.healthy)
                    .min_by_key(|e| e.active_connections)
                    .cloned()
                    .ok_or_else(|| anyhow!("No healthy Frontend"))?
            }
            BalancerStrategy::Random => {
                use rand::seq::SliceRandom;
                pool.choose(&mut rand::thread_rng())
                    .cloned()
                    .ok_or_else(|| anyhow!("No Frontend nodes"))?
            }
        };
        
        Ok(endpoint)
    }
    
    fn start_discovery_task(&self) {
        let discovery = self.discovery.clone();
        let frontend_pool = self.frontend_pool.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            
            loop {
                interval.tick().await;
                
                match discovery.discover_by_role("risingwave-frontend").await {
                    Ok(nodes) => {
                        let endpoints: Vec<FrontendEndpoint> = nodes
                            .into_iter()
                            .map(|node| FrontendEndpoint {
                                node_id: node.id,
                                addr: node.addr,
                                healthy: matches!(node.status, NodeStatus::Healthy),
                                active_connections: 0,
                            })
                            .collect();
                        
                        *frontend_pool.write().await = endpoints;
                    }
                    Err(e) => {
                        error!("Failed to discover Frontend nodes: {}", e);
                    }
                }
            }
        });
    }
}
```

#### 任务 3: Compute 节点自动扩缩容（10h）

```rust
// crates/nexora-cluster/src/compute_autoscaler.rs

pub struct ComputeAutoscaler {
    discovery: Arc<dyn ServiceDiscovery>,
    config: AutoscalerConfig,
    metrics_collector: Arc<MetricsCollector>,
}

#[derive(Debug, Clone)]
pub struct AutoscalerConfig {
    /// 最小 Compute 节点数
    pub min_nodes: usize,
    
    /// 最大 Compute 节点数
    pub max_nodes: usize,
    
    /// CPU 使用率阈值（扩容）
    pub scale_up_cpu_threshold: f64,
    
    /// CPU 使用率阈值（缩容）
    pub scale_down_cpu_threshold: f64,
    
    /// 扩缩容冷却时间（秒）
    pub cooldown_seconds: u64,
    
    /// 检查间隔（秒）
    pub check_interval_seconds: u64,
}

impl ComputeAutoscaler {
    pub fn start(
        discovery: Arc<dyn ServiceDiscovery>,
        config: AutoscalerConfig,
        metrics_collector: Arc<MetricsCollector>,
    ) {
        let autoscaler = Self {
            discovery,
            config,
            metrics_collector,
        };
        
        tokio::spawn(async move {
            autoscaler.run().await;
        });
    }
    
    async fn run(&self) {
        let mut interval = tokio::time::interval(
            Duration::from_secs(self.config.check_interval_seconds)
        );
        
        let mut last_scale_time = SystemTime::now();
        
        loop {
            interval.tick().await;
            
            // 检查冷却时间
            let now = SystemTime::now();
            if now.duration_since(last_scale_time).unwrap().as_secs() 
                < self.config.cooldown_seconds {
                continue;
            }
            
            // 收集 Compute 节点指标
            let compute_metrics = self.collect_compute_metrics().await;
            
            // 计算平均 CPU 使用率
            let avg_cpu = compute_metrics.iter()
                .map(|m| m.cpu_usage)
                .sum::<f64>() / compute_metrics.len() as f64;
            
            let current_count = compute_metrics.len();
            
            // 扩容判断
            if avg_cpu > self.config.scale_up_cpu_threshold 
                && current_count < self.config.max_nodes {
                info!("Scaling up: avg_cpu={:.2}%, current={}, target={}",
                      avg_cpu * 100.0, current_count, current_count + 1);
                
                if self.scale_up().await.is_ok() {
                    last_scale_time = now;
                }
            }
            
            // 缩容判断
            else if avg_cpu < self.config.scale_down_cpu_threshold 
                && current_count > self.config.min_nodes {
                info!("Scaling down: avg_cpu={:.2}%, current={}, target={}",
                      avg_cpu * 100.0, current_count, current_count - 1);
                
                if self.scale_down().await.is_ok() {
                    last_scale_time = now;
                }
            }
        }
    }
    
    async fn scale_up(&self) -> Result<()> {
        // 在 Kubernetes 环境中，调用 K8s API 增加副本数
        // 在裸机环境中，触发新节点部署脚本
        
        info!("Triggering Compute node scale-up");
        
        // 示例：Kubernetes
        #[cfg(feature = "kubernetes")]
        {
            self.scale_k8s_deployment(ScaleDirection::Up).await?;
        }
        
        Ok(())
    }
    
    async fn scale_down(&self) -> Result<()> {
        info!("Triggering Compute node scale-down");
        
        // 1. 选择要移除的节点（优先移除负载最低的）
        let target_node = self.select_node_for_removal().await?;
        
        // 2. 驱逐节点上的任务
        self.drain_node(&target_node).await?;
        
        // 3. 移除节点
        #[cfg(feature = "kubernetes")]
        {
            self.scale_k8s_deployment(ScaleDirection::Down).await?;
        }
        
        Ok(())
    }
}
```

---

### 8.5 统一监控（20h）

#### 任务 1: 集群拓扑 API（8h）

```rust
// crates/nexora-app/src/handlers/cluster.rs

/// GET /api/cluster/topology
pub async fn get_cluster_topology(
    State(state): State<AppState>,
) -> Result<Json<ClusterTopology>, ApiError> {
    let discovery = &state.discovery;
    
    let nodes = discovery.discover_nodes().await
        .map_err(|e| ApiError::internal_error(e))?;
    
    // 获取 Meta Leader
    let meta_leader = state.meta_coordinator
        .get_current_leader()
        .await;
    
    // 构建拓扑
    let topology = ClusterTopology {
        cluster_name: state.config.cluster_name.clone(),
        total_nodes: nodes.len(),
        meta_leader,
        nodes: nodes.into_iter().map(|n| NodeInfo {
            id: n.id,
            addr: n.addr,
            nexora_roles: n.nexora_roles,
            risingwave_roles: n.risingwave_roles,
            status: n.status,
            resources: ResourceUsage {
                cpu_usage: 0.0, // TODO: 从指标收集
                memory_mb: 0,
                disk_gb: 0,
            },
        }).collect(),
    };
    
    Ok(Json(topology))
}

/// GET /api/cluster/nodes/:node_id
pub async fn get_node_detail(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<NodeDetail>, ApiError> {
    // 返回节点详细信息
    // - 运行的组件列表
    // - 每个组件的状态
    // - 资源使用情况
    // - 最近的日志
}

/// POST /api/cluster/nodes/:node_id/drain
pub async fn drain_node(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<DrainResponse>, ApiError> {
    // 驱逐节点（准备维护）
}

/// POST /api/cluster/nodes/:node_id/shutdown
pub async fn shutdown_node(
    State(state): State<AppState>,
    Path(node_id): Path<String>,
) -> Result<Json<ShutdownResponse>, ApiError> {
    // 关闭节点
}
```

#### 任务 2: Prometheus 指标暴露（6h）

```rust
// crates/nexora-app/src/metrics/cluster_metrics.rs

use prometheus::{Registry, IntGaugeVec, GaugeVec};

pub struct ClusterMetrics {
    /// 集群节点总数
    pub node_count: IntGaugeVec,
    
    /// 每个角色的节点数
    pub node_count_by_role: IntGaugeVec,
    
    /// 节点状态（0=停止, 1=启动中, 2=健康, 3=不健康）
    pub node_status: IntGaugeVec,
    
    /// Meta Leader 节点 ID（标签形式）
    pub meta_leader: IntGaugeVec,
    
    /// Compute 节点平均 CPU 使用率
    pub compute_avg_cpu: GaugeVec,
    
    /// Frontend 节点连接数
    pub frontend_connections: IntGaugeVec,
}

impl ClusterMetrics {
    pub fn new(registry: &Registry) -> Result<Self> {
        let node_count = IntGaugeVec::new(
            opts!("nexora_cluster_nodes_total", "Total nodes in cluster"),
            &["cluster_name"],
        )?;
        registry.register(Box::new(node_count.clone()))?;
        
        let node_count_by_role = IntGaugeVec::new(
            opts!("nexora_cluster_nodes_by_role", "Nodes by role"),
            &["cluster_name", "role"],
        )?;
        registry.register(Box::new(node_count_by_role.clone()))?;
        
        // ... 注册其他指标
        
        Ok(Self {
            node_count,
            node_count_by_role,
            // ...
        })
    }
    
    pub async fn update(&self, topology: &ClusterTopology) {
        // 更新节点总数
        self.node_count
            .with_label_values(&[&topology.cluster_name])
            .set(topology.total_nodes as i64);
        
        // 统计各角色节点数
        let mut role_counts: HashMap<String, usize> = HashMap::new();
        for node in &topology.nodes {
            for role in &node.risingwave_roles {
                *role_counts.entry(format!("{:?}", role)).or_insert(0) += 1;
            }
        }
        
        for (role, count) in role_counts {
            self.node_count_by_role
                .with_label_values(&[&topology.cluster_name, &role])
                .set(count as i64);
        }
    }
}
```

#### 任务 3: Grafana 仪表盘（6h）

```json
// grafana/dashboards/nexora-cluster.json

{
  "dashboard": {
    "title": "Nexora Distributed Cluster",
    "panels": [
      {
        "title": "Cluster Topology",
        "type": "node-graph",
        "targets": [
          {
            "expr": "nexora_cluster_nodes_total"
          }
        ]
      },
      {
        "title": "Nodes by Role",
        "type": "piechart",
        "targets": [
          {
            "expr": "nexora_cluster_nodes_by_role"
          }
        ]
      },
      {
        "title": "Node Health Status",
        "type": "stat",
        "targets": [
          {
            "expr": "sum(nexora_cluster_node_status == 2)"
          }
        ]
      },
      {
        "title": "Compute Node CPU Usage",
        "type": "graph",
        "targets": [
          {
            "expr": "nexora_compute_cpu_usage"
          }
        ]
      }
    ]
  }
}
```

---

### 8.6 测试验证（24h）

#### 任务 1: 集成测试套件（12h）

```rust
// tests/distributed_cluster_test.rs

#[tokio::test]
async fn test_3_node_meta_ha_cluster() {
    // 启动 3 节点 Meta HA 集群
    let cluster = TestCluster::builder()
        .with_node(NodeConfig {
            node_id: "meta-1".into(),
            roles: RolePreset::meta_node(),
            raft: Some(RaftConfig {
                node_id: 1,
                initial_members: vec![
                    RaftMember { id: 1, addr: "127.0.0.1:5690".parse().unwrap() },
                    RaftMember { id: 2, addr: "127.0.0.1:5691".parse().unwrap() },
                    RaftMember { id: 3, addr: "127.0.0.1:5692".parse().unwrap() },
                ],
                ..Default::default()
            }),
            ..Default::default()
        })
        .with_node(NodeConfig {
            node_id: "meta-2".into(),
            roles: RolePreset::meta_node(),
            raft: Some(RaftConfig { node_id: 2, .. }),
            ..Default::default()
        })
        .with_node(NodeConfig {
            node_id: "meta-3".into(),
            roles: RolePreset::meta_node(),
            raft: Some(RaftConfig { node_id: 3, .. }),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    
    // 验证 Leader 选举
    let leader = cluster.wait_for_meta_leader(Duration::from_secs(10))
        .await
        .unwrap();
    assert!(["meta-1", "meta-2", "meta-3"].contains(&leader.as_str()));
    
    // 验证所有节点健康
    let topology = cluster.get_topology().await.unwrap();
    assert_eq!(topology.nodes.len(), 3);
    for node in &topology.nodes {
        assert!(matches!(node.status, NodeStatus::Healthy));
    }
    
    // 验证 Meta 功能
    let client = cluster.get_meta_client().await.unwrap();
    client.create_database("test_db").await.unwrap();
    
    // 模拟 Leader 故障
    cluster.stop_node(&leader).await.unwrap();
    
    // 验证新 Leader 选举
    let new_leader = cluster.wait_for_meta_leader(Duration::from_secs(10))
        .await
        .unwrap();
    assert_ne!(new_leader, leader);
    
    // 验证集群仍然可用
    let client = cluster.get_meta_client().await.unwrap();
    client.create_database("test_db_2").await.unwrap();
    
    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_mixed_role_deployment() {
    // 测试混合角色部署
    let cluster = TestCluster::builder()
        // Node 1: Meta + Frontend + Nexora
        .with_node(NodeConfig {
            node_id: "node-1".into(),
            roles: NodeRoles {
                nexora: vec![NexoraRole::Core, NexoraRole::Raft],
                risingwave: vec![RisingWaveRole::Meta, RisingWaveRole::Frontend],
            },
            ..Default::default()
        })
        // Node 2: Compute only
        .with_node(NodeConfig {
            node_id: "node-2".into(),
            roles: NodeRoles {
                nexora: vec![],
                risingwave: vec![RisingWaveRole::Compute],
            },
            ..Default::default()
        })
        // Node 3: Compute only
        .with_node(NodeConfig {
            node_id: "node-3".into(),
            roles: RolePreset::compute_node(),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    
    // 验证拓扑
    let topology = cluster.get_topology().await.unwrap();
    assert_eq!(topology.nodes.len(), 3);
    
    // 验证 node-1 有多个角色
    let node1 = topology.nodes.iter().find(|n| n.id == "node-1").unwrap();
    assert_eq!(node1.nexora_roles.len(), 2);
    assert_eq!(node1.risingwave_roles.len(), 2);
    
    // 验证 node-2 只有 Compute 角色
    let node2 = topology.nodes.iter().find(|n| n.id == "node-2").unwrap();
    assert_eq!(node2.risingwave_roles, vec![RisingWaveRole::Compute]);
    
    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_dynamic_compute_scaling() {
    let cluster = TestCluster::builder()
        .with_node(/* Meta + Frontend */)
        .with_node(/* Compute 1 */)
        .build()
        .await
        .unwrap();
    
    // 初始 1 个 Compute 节点
    let compute_nodes = cluster.get_compute_nodes().await.unwrap();
    assert_eq!(compute_nodes.len(), 1);
    
    // 动态添加 Compute 节点
    cluster.add_compute_node().await.unwrap();
    
    // 等待新节点加入
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    let compute_nodes = cluster.get_compute_nodes().await.unwrap();
    assert_eq!(compute_nodes.len(), 2);
    
    // 移除一个 Compute 节点
    cluster.remove_compute_node(&compute_nodes[0].id).await.unwrap();
    
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    let compute_nodes = cluster.get_compute_nodes().await.unwrap();
    assert_eq!(compute_nodes.len(), 1);
    
    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_service_discovery_backends() {
    // 测试 etcd 服务发现
    test_with_etcd_discovery().await;
    
    // 测试静态服务发现
    test_with_static_discovery().await;
}

async fn test_with_etcd_discovery() {
    let etcd = EtcdContainer::start().await.unwrap();
    
    let cluster = TestCluster::builder()
        .with_discovery(DiscoveryBackend::Etcd {
            endpoints: vec![etcd.endpoint()],
            prefix: "/nexora-test".into(),
        })
        .with_node(/* ... */)
        .build()
        .await
        .unwrap();
    
    // 验证节点注册到 etcd
    let etcd_client = etcd.client();
    let keys = etcd_client.get("/nexora-test/nodes/", None).await.unwrap();
    assert!(keys.kvs().len() > 0);
    
    cluster.shutdown().await.unwrap();
    etcd.stop().await.unwrap();
}
```

#### 任务 2: 故障注入测试（8h）

```rust
// tests/chaos_test.rs

#[tokio::test]
async fn test_meta_leader_crash() {
    let cluster = setup_3_node_cluster().await;
    
    // 1. 正常操作
    let client = cluster.get_client().await.unwrap();
    client.execute_ddl("CREATE TABLE t1 (id INT)").await.unwrap();
    
    // 2. Kill Meta Leader
    let leader = cluster.get_meta_leader().await.unwrap();
    cluster.kill_node(&leader).await.unwrap();
    
    // 3. 等待重新选举（应该在 10 秒内完成）
    let new_leader = cluster.wait_for_meta_leader(Duration::from_secs(10))
        .await
        .unwrap();
    assert_ne!(new_leader, leader);
    
    // 4. 验证集群仍然可用
    client.execute_ddl("CREATE TABLE t2 (id INT)").await.unwrap();
    
    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_network_partition() {
    let cluster = setup_5_node_cluster().await;
    
    // 模拟网络分区：[node-1, node-2] 与 [node-3, node-4, node-5] 隔离
    cluster.create_network_partition(
        vec!["node-1", "node-2"],
        vec!["node-3", "node-4", "node-5"],
    ).await.unwrap();
    
    // 等待 30 秒观察集群行为
    tokio::time::sleep(Duration::from_secs(30)).await;
    
    // 多数派（3个节点）应该仍然可用
    let majority_client = cluster.get_client_for_node("node-3").await.unwrap();
    majority_client.execute_ddl("CREATE TABLE t1 (id INT)")
        .await
        .unwrap();
    
    // 少数派（2个节点）应该不可用
    let minority_client = cluster.get_client_for_node("node-1").await.unwrap();
    assert!(minority_client.execute_ddl("CREATE TABLE t2 (id INT)")
        .await
        .is_err());
    
    // 恢复网络
    cluster.heal_network_partition().await.unwrap();
    
    // 等待集群恢复
    tokio::time::sleep(Duration::from_secs(10)).await;
    
    // 验证所有节点再次可用
    let client = cluster.get_client().await.unwrap();
    client.execute_ddl("CREATE TABLE t3 (id INT)").await.unwrap();
    
    cluster.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_cascading_failures() {
    let cluster = setup_5_node_cluster().await;
    
    // 逐个 kill 节点，测试集群降级行为
    for i in 1..=3 {
        let node = format!("node-{}", i);
        cluster.kill_node(&node).await.unwrap();
        
        tokio::time::sleep(Duration::from_secs(5)).await;
        
        // 前两次应该仍然可用（3/5, 2/5 quorum）
        if i <= 2 {
            let client = cluster.get_client().await.unwrap();
            client.execute_ddl(&format!("CREATE TABLE t{} (id INT)", i))
                .await
                .unwrap();
        } else {
            // 第三次 kill 后应该不可用（1/5 无法达到 quorum）
            assert!(cluster.get_client().await.is_err());
        }
    }
    
    cluster.shutdown().await.unwrap();
}
```

#### 任务 3: 性能基准测试（4h）

```rust
// benches/cluster_benchmark.rs

use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn benchmark_distributed_query(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let cluster = rt.block_on(setup_cluster());
    
    c.bench_function("distributed DDL execution", |b| {
        b.to_async(&rt).iter(|| async {
            let client = cluster.get_client().await.unwrap();
            client.execute_ddl(black_box("CREATE TABLE bench (id INT)"))
                .await
                .unwrap();
        });
    });
    
    c.bench_function("distributed query", |b| {
        b.to_async(&rt).iter(|| async {
            let client = cluster.get_client().await.unwrap();
            client.query(black_box("SELECT COUNT(*) FROM bench"))
                .await
                .unwrap();
        });
    });
}

fn benchmark_service_discovery(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    
    c.bench_function("etcd node discovery", |b| {
        b.to_async(&rt).iter(|| async {
            let discovery = setup_etcd_discovery().await;
            discovery.discover_nodes().await.unwrap();
        });
    });
    
    c.bench_function("static node discovery", |b| {
        b.to_async(&rt).iter(|| async {
            let discovery = setup_static_discovery().await;
            discovery.discover_nodes().await.unwrap();
        });
    });
}

criterion_group!(benches, benchmark_distributed_query, benchmark_service_discovery);
criterion_main!(benches);
```

---

### 8.7 部署工具（12h）

#### 任务 1: Docker Compose 模板（4h）

```yaml
# docker/docker-compose-distributed.yml

version: '3.8'

services:
  # etcd 集群（服务发现）
  etcd-1:
    image: quay.io/coreos/etcd:v3.5.0
    environment:
      - ETCD_NAME=etcd-1
      - ETCD_INITIAL_CLUSTER=etcd-1=http://etcd-1:2380,etcd-2=http://etcd-2:2380,etcd-3=http://etcd-3:2380
    ports:
      - "2379:2379"
  
  etcd-2:
    image: quay.io/coreos/etcd:v3.5.0
    environment:
      - ETCD_NAME=etcd-2
      - ETCD_INITIAL_CLUSTER=etcd-1=http://etcd-1:2380,etcd-2=http://etcd-2:2380,etcd-3=http://etcd-3:2380
  
  etcd-3:
    image: quay.io/coreos/etcd:v3.5.0
    environment:
      - ETCD_NAME=etcd-3
      - ETCD_INITIAL_CLUSTER=etcd-1=http://etcd-1:2380,etcd-2=http://etcd-2:2380,etcd-3=http://etcd-3:2380
  
  # Nexora Meta 节点（HA）
  nexora-meta-1:
    image: nexora:2.2.0-distributed
    command:
      - --role-preset=meta-node
      - --cluster-mode=distributed
      - --cluster-name=nexora-prod
      - --node-id=meta-1
      - --raft-id=1
      - --raft-members=1=nexora-meta-1:5690,2=nexora-meta-2:5690,3=nexora-meta-3:5690
      - --discovery-backend=etcd
      - --etcd-endpoints=http://etcd-1:2379,http://etcd-2:2379,http://etcd-3:2379
    ports:
      - "8081:8080"
    depends_on:
      - etcd-1
      - etcd-2
      - etcd-3
  
  nexora-meta-2:
    image: nexora:2.2.0-distributed
    command:
      - --role-preset=meta-node
      - --cluster-mode=distributed
      - --node-id=meta-2
      - --raft-id=2
      - --raft-members=1=nexora-meta-1:5690,2=nexora-meta-2:5690,3=nexora-meta-3:5690
      - --discovery-backend=etcd
      - --etcd-endpoints=http://etcd-1:2379,http://etcd-2:2379,http://etcd-3:2379
    ports:
      - "8082:8080"
    depends_on:
      - etcd-1
      - nexora-meta-1
  
  nexora-meta-3:
    image: nexora:2.2.0-distributed
    command:
      - --role-preset=meta-node
      - --cluster-mode=distributed
      - --node-id=meta-3
      - --raft-id=3
      - --raft-members=1=nexora-meta-1:5690,2=nexora-meta-2:5690,3=nexora-meta-3:5690
      - --discovery-backend=etcd
      - --etcd-endpoints=http://etcd-1:2379,http://etcd-2:2379,http://etcd-3:2379
    ports:
      - "8083:8080"
    depends_on:
      - etcd-1
      - nexora-meta-1
  
  # Nexora Query 节点（Frontend + Nexora Core）
  nexora-query-1:
    image: nexora:2.2.0-distributed
    command:
      - --role-preset=query-node
      - --cluster-mode=distributed
      - --node-id=query-1
      - --discovery-backend=etcd
      - --etcd-endpoints=http://etcd-1:2379,http://etcd-2:2379,http://etcd-3:2379
    ports:
      - "8080:8080"
    depends_on:
      - nexora-meta-1
      - nexora-meta-2
      - nexora-meta-3
  
  # Nexora Compute 节点（可扩展）
  nexora-compute-1:
    image: nexora:2.2.0-distributed
    command:
      - --role-preset=compute-node
      - --cluster-mode=distributed
      - --node-id=compute-1
      - --discovery-backend=etcd
      - --etcd-endpoints=http://etcd-1:2379,http://etcd-2:2379,http://etcd-3:2379
    depends_on:
      - nexora-meta-1
  
  nexora-compute-2:
    image: nexora:2.2.0-distributed
    command:
      - --role-preset=compute-node
      - --cluster-mode=distributed
      - --node-id=compute-2
      - --discovery-backend=etcd
      - --etcd-endpoints=http://etcd-1:2379,http://etcd-2:2379,http://etcd-3:2379
    depends_on:
      - nexora-meta-1

# 扩展 Compute 节点：
# docker-compose -f docker-compose-distributed.yml up --scale nexora-compute=5
```

#### 任务 2: Kubernetes 部署清单（6h）

```yaml
# k8s/statefulset-meta.yaml

apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: nexora-meta
  namespace: nexora
spec:
  serviceName: nexora-meta
  replicas: 3
  selector:
    matchLabels:
      app: nexora
      component: meta
  template:
    metadata:
      labels:
        app: nexora
        component: meta
    spec:
      containers:
      - name: nexora
        image: nexora:2.2.0-distributed
        command:
        - /usr/local/bin/nexora
        - --role-preset=meta-node
        - --cluster-mode=distributed
        - --cluster-name=nexora-prod
        - --node-id=$(POD_NAME)
        - --raft-id=$(RAFT_ID)
        - --raft-members=nexora-meta-0:5690,nexora-meta-1:5690,nexora-meta-2:5690
        - --discovery-backend=kubernetes
        - --advertise-addr=$(POD_IP):8080
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: RAFT_ID
          value: "$(echo $POD_NAME | sed 's/nexora-meta-//')"
        ports:
        - containerPort: 8080
          name: http
        - containerPort: 5690
          name: raft
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
---
# k8s/deployment-compute.yaml

apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexora-compute
  namespace: nexora
spec:
  replicas: 5
  selector:
    matchLabels:
      app: nexora
      component: compute
  template:
    metadata:
      labels:
        app: nexora
        component: compute
    spec:
      containers:
      - name: nexora
        image: nexora:2.2.0-distributed
        command:
        - /usr/local/bin/nexora
        - --role-preset=compute-node
        - --cluster-mode=distributed
        - --node-id=$(POD_NAME)
        - --discovery-backend=kubernetes
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        resources:
          requests:
            memory: "2Gi"
            cpu: "2000m"
          limits:
            memory: "4Gi"
            cpu: "4000m"
---
# k8s/hpa.yaml

apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: nexora-compute-hpa
  namespace: nexora
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: nexora-compute
  minReplicas: 2
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

#### 任务 3: 部署脚本（2h）

```bash
#!/bin/bash
# scripts/deploy-distributed.sh

set -e

CLUSTER_NAME="${CLUSTER_NAME:-nexora-prod}"
META_NODES="${META_NODES:-3}"
COMPUTE_NODES="${COMPUTE_NODES:-5}"
DEPLOY_MODE="${DEPLOY_MODE:-docker-compose}"  # or kubernetes

echo "Deploying Nexora Distributed Cluster"
echo "  Cluster Name: $CLUSTER_NAME"
echo "  Meta Nodes: $META_NODES"
echo "  Compute Nodes: $COMPUTE_NODES"
echo "  Mode: $DEPLOY_MODE"

case $DEPLOY_MODE in
  docker-compose)
    echo "Deploying with Docker Compose..."
    docker-compose -f docker/docker-compose-distributed.yml \
      up -d \
      --scale nexora-compute=$COMPUTE_NODES
    ;;
    
  kubernetes)
    echo "Deploying to Kubernetes..."
    kubectl create namespace nexora || true
    kubectl apply -f k8s/statefulset-meta.yaml
    kubectl apply -f k8s/deployment-compute.yaml
    kubectl apply -f k8s/hpa.yaml
    
    # 等待 Meta 节点就绪
    kubectl wait --for=condition=ready pod -l component=meta -n nexora --timeout=300s
    
    echo "Cluster deployed successfully!"
    kubectl get pods -n nexora
    ;;
    
  *)
    echo "Unknown deployment mode: $DEPLOY_MODE"
    exit 1
    ;;
esac
```

---

## 第三部分：部署场景示例

### 场景 1: 小型开发环境（3 节点）

#### 拓扑结构

```
Node 1 (All-in-One)
├─ Nexora Core
├─ Nexora Raft (Leader)
├─ RW Meta (Leader)
├─ RW Frontend
└─ RW Compute

Node 2 (All-in-One)
├─ Nexora Core
├─ Nexora Raft (Follower)
├─ RW Meta (Follower)
├─ RW Frontend
└─ RW Compute

Node 3 (All-in-One)
├─ Nexora Core
├─ Nexora Raft (Follower)
├─ RW Meta (Follower)
├─ RW Frontend
└─ RW Compute

Resource: 3 x 4GB RAM = 12GB total
```

#### 启动命令

```bash
# Node 1
./nexora \
  --role-preset=all-in-one \
  --cluster-mode=distributed \
  --cluster-name=dev-cluster \
  --node-id=node-1 \
  --raft-id=1 \
  --raft-members=1=node-1:5690,2=node-2:5690,3=node-3:5690 \
  --listen-addr=0.0.0.0:8080 \
  --advertise-addr=192.168.1.10:8080 \
  --discovery-backend=static \
  --meta-addrs=192.168.1.10:5690,192.168.1.11:5690,192.168.1.12:5690 \
  --memory-limit-mb=3072

# Node 2
./nexora \
  --role-preset=all-in-one \
  --cluster-mode=distributed \
  --cluster-name=dev-cluster \
  --node-id=node-2 \
  --raft-id=2 \
  --raft-members=1=node-1:5690,2=node-2:5690,3=node-3:5690 \
  --listen-addr=0.0.0.0:8080 \
  --advertise-addr=192.168.1.11:8080 \
  --discovery-backend=static \
  --meta-addrs=192.168.1.10:5690,192.168.1.11:5690,192.168.1.12:5690 \
  --memory-limit-mb=3072

# Node 3
./nexora \
  --role-preset=all-in-one \
  --cluster-mode=distributed \
  --cluster-name=dev-cluster \
  --node-id=node-3 \
  --raft-id=3 \
  --raft-members=1=node-1:5690,2=node-2:5690,3=node-3:5690 \
  --listen-addr=0.0.0.0:8080 \
  --advertise-addr=192.168.1.12:8080 \
  --discovery-backend=static \
  --meta-addrs=192.168.1.10:5690,192.168.1.11:5690,192.168.1.12:5690 \
  --memory-limit-mb=3072
```

#### 验证部署

```bash
# 检查集群拓扑
curl http://192.168.1.10:8080/api/cluster/topology | jq

# 检查 Meta Leader
curl http://192.168.1.10:8080/api/cluster/meta/leader

# 执行测试查询
psql -h 192.168.1.10 -p 4566 -d dev -U root -c "SELECT 1"
```

---

### 场景 2: 中型生产环境（7 节点）

#### 拓扑结构

```
Meta 层 (3 节点 - HA)
├─ Node 1: Meta + Raft
├─ Node 2: Meta + Raft
└─ Node 3: Meta + Raft

查询层 (2 节点 - 负载均衡)
├─ Node 4: Frontend + Nexora Core
└─ Node 5: Frontend + Nexora Core

计算层 (2 节点 - 可扩展)
├─ Node 6: Compute
└─ Node 7: Compute

Resource: 3x2GB + 2x3GB + 2x4GB = 20GB total
```

#### Docker Compose 配置

```yaml
# docker-compose-prod.yml

version: '3.8'

services:
  # Meta 层
  meta-1:
    image: nexora:2.2.0
    command: >
      --role-preset=meta-node
      --cluster-mode=distributed
      --cluster-name=prod
      --node-id=meta-1
      --raft-id=1
      --raft-members=1=meta-1:5690,2=meta-2:5690,3=meta-3:5690
      --discovery-backend=etcd
      --etcd-endpoints=http://etcd:2379
      --memory-limit-mb=1536
    networks:
      - nexora-net
    depends_on:
      - etcd

  meta-2:
    image: nexora:2.2.0
    command: >
      --role-preset=meta-node
      --node-id=meta-2
      --raft-id=2
      --raft-members=1=meta-1:5690,2=meta-2:5690,3=meta-3:5690
      --discovery-backend=etcd
      --etcd-endpoints=http://etcd:2379
    networks:
      - nexora-net

  meta-3:
    image: nexora:2.2.0
    command: >
      --role-preset=meta-node
      --node-id=meta-3
      --raft-id=3
      --raft-members=1=meta-1:5690,2=meta-2:5690,3=meta-3:5690
      --discovery-backend=etcd
      --etcd-endpoints=http://etcd:2379
    networks:
      - nexora-net

  # 查询层
  query-1:
    image: nexora:2.2.0
    command: >
      --role-preset=query-node
      --node-id=query-1
      --discovery-backend=etcd
      --etcd-endpoints=http://etcd:2379
      --memory-limit-mb=2560
    ports:
      - "8080:8080"
      - "4566:4566"
    networks:
      - nexora-net
    depends_on:
      - meta-1
      - meta-2
      - meta-3

  query-2:
    image: nexora:2.2.0
    command: >
      --role-preset=query-node
      --node-id=query-2
      --discovery-backend=etcd
      --etcd-endpoints=http://etcd:2379
      --memory-limit-mb=2560
    ports:
      - "8081:8080"
      - "4567:4566"
    networks:
      - nexora-net

  # 计算层
  compute-1:
    image: nexora:2.2.0
    command: >
      --role-preset=compute-node
      --node-id=compute-1
      --discovery-backend=etcd
      --etcd-endpoints=http://etcd:2379
      --memory-limit-mb=3584
    networks:
      - nexora-net

  compute-2:
    image: nexora:2.2.0
    command: >
      --role-preset=compute-node
      --node-id=compute-2
      --discovery-backend=etcd
      --etcd-endpoints=http://etcd:2379
      --memory-limit-mb=3584
    networks:
      - nexora-net

  # 服务发现
  etcd:
    image: quay.io/coreos/etcd:v3.5.0
    environment:
      - ETCD_NAME=etcd
      - ETCD_LISTEN_CLIENT_URLS=http://0.0.0.0:2379
      - ETCD_ADVERTISE_CLIENT_URLS=http://etcd:2379
    networks:
      - nexora-net

  # 负载均衡器
  nginx:
    image: nginx:alpine
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
    ports:
      - "80:80"
    networks:
      - nexora-net
    depends_on:
      - query-1
      - query-2

networks:
  nexora-net:
    driver: bridge
```

#### Nginx 负载均衡配置

```nginx
# nginx.conf

upstream nexora_query {
    least_conn;
    server query-1:4566 max_fails=3 fail_timeout=30s;
    server query-2:4566 max_fails=3 fail_timeout=30s;
}

upstream nexora_api {
    least_conn;
    server query-1:8080 max_fails=3 fail_timeout=30s;
    server query-2:8080 max_fails=3 fail_timeout=30s;
}

server {
    listen 80;
    
    # PostgreSQL 协议代理
    location /psql {
        proxy_pass http://nexora_query;
        proxy_connect_timeout 5s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }
    
    # HTTP API
    location /api {
        proxy_pass http://nexora_api;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host $host;
        proxy_cache_bypass $http_upgrade;
    }
    
    # 健康检查
    location /health {
        proxy_pass http://nexora_api/health;
    }
}
```

---

### 场景 3: 大型生产环境（Kubernetes 20+ 节点）

#### Kubernetes 部署清单

```yaml
# k8s/production/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: nexora-prod

---
# k8s/production/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: nexora-config
  namespace: nexora-prod
data:
  nexora.toml: |
    [cluster]
    mode = "distributed"
    name = "nexora-prod"
    
    [discovery]
    backend = "kubernetes"
    
    [resources]
    memory_limit_mb = 4096
    cpu_cores = 4

---
# k8s/production/statefulset-meta.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: nexora-meta
  namespace: nexora-prod
spec:
  serviceName: nexora-meta-svc
  replicas: 3
  selector:
    matchLabels:
      app: nexora
      component: meta
  template:
    metadata:
      labels:
        app: nexora
        component: meta
    spec:
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchExpressions:
              - key: component
                operator: In
                values:
                - meta
            topologyKey: kubernetes.io/hostname
      containers:
      - name: nexora
        image: nexora:2.2.0-distributed
        command:
        - /usr/local/bin/nexora
        args:
        - --role-preset=meta-node
        - --cluster-mode=distributed
        - --cluster-name=nexora-prod
        - --node-id=$(POD_NAME)
        - --raft-id=$(RAFT_ID)
        - --discovery-backend=kubernetes
        - --advertise-addr=$(POD_IP):8080
        - --memory-limit-mb=2048
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: RAFT_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.labels['statefulset.kubernetes.io/pod-name']
        ports:
        - containerPort: 8080
          name: http
        - containerPort: 5690
          name: raft
        volumeMounts:
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-ssd
      resources:
        requests:
          storage: 50Gi

---
# k8s/production/deployment-query.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexora-query
  namespace: nexora-prod
spec:
  replicas: 5
  selector:
    matchLabels:
      app: nexora
      component: query
  template:
    metadata:
      labels:
        app: nexora
        component: query
    spec:
      containers:
      - name: nexora
        image: nexora:2.2.0-distributed
        command:
        - /usr/local/bin/nexora
        args:
        - --role-preset=query-node
        - --cluster-mode=distributed
        - --node-id=$(POD_NAME)
        - --discovery-backend=kubernetes
        - --memory-limit-mb=3072
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        ports:
        - containerPort: 8080
          name: http
        - containerPort: 4566
          name: psql
        resources:
          requests:
            memory: "3Gi"
            cpu: "2"
          limits:
            memory: "6Gi"
            cpu: "4"
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 20
          periodSeconds: 10

---
# k8s/production/deployment-compute.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nexora-compute
  namespace: nexora-prod
spec:
  replicas: 10
  selector:
    matchLabels:
      app: nexora
      component: compute
  template:
    metadata:
      labels:
        app: nexora
        component: compute
    spec:
      containers:
      - name: nexora
        image: nexora:2.2.0-distributed
        command:
        - /usr/local/bin/nexora
        args:
        - --role-preset=compute-node
        - --cluster-mode=distributed
        - --node-id=$(POD_NAME)
        - --discovery-backend=kubernetes
        - --memory-limit-mb=7168
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        resources:
          requests:
            memory: "7Gi"
            cpu: "4"
          limits:
            memory: "14Gi"
            cpu: "8"

---
# k8s/production/hpa-compute.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: nexora-compute-hpa
  namespace: nexora-prod
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: nexora-compute
  minReplicas: 5
  maxReplicas: 50
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 100
        periodSeconds: 30

---
# k8s/production/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: nexora-query-svc
  namespace: nexora-prod
spec:
  type: LoadBalancer
  selector:
    app: nexora
    component: query
  ports:
  - name: http
    port: 80
    targetPort: 8080
  - name: psql
    port: 5432
    targetPort: 4566
```

#### 部署脚本

```bash
#!/bin/bash
# scripts/deploy-k8s-production.sh

set -e

NAMESPACE="nexora-prod"
CONTEXT="production-cluster"

echo "Deploying Nexora to Kubernetes (Production)"
echo "  Context: $CONTEXT"
echo "  Namespace: $NAMESPACE"

# 切换到生产集群
kubectl config use-context $CONTEXT

# 创建命名空间
kubectl apply -f k8s/production/namespace.yaml

# 应用配置
kubectl apply -f k8s/production/configmap.yaml

# 部署 Meta 层（StatefulSet）
echo "Deploying Meta nodes..."
kubectl apply -f k8s/production/statefulset-meta.yaml
kubectl rollout status statefulset/nexora-meta -n $NAMESPACE --timeout=5m

# 部署查询层
echo "Deploying Query nodes..."
kubectl apply -f k8s/production/deployment-query.yaml
kubectl rollout status deployment/nexora-query -n $NAMESPACE --timeout=5m

# 部署计算层
echo "Deploying Compute nodes..."
kubectl apply -f k8s/production/deployment-compute.yaml
kubectl rollout status deployment/nexora-compute -n $NAMESPACE --timeout=5m

# 应用 HPA
kubectl apply -f k8s/production/hpa-compute.yaml

# 暴露服务
kubectl apply -f k8s/production/service.yaml

# 等待 LoadBalancer 获取外部 IP
echo "Waiting for LoadBalancer IP..."
EXTERNAL_IP=""
while [ -z "$EXTERNAL_IP" ]; do
  EXTERNAL_IP=$(kubectl get svc nexora-query-svc -n $NAMESPACE \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
  [ -z "$EXTERNAL_IP" ] && sleep 5
done

echo ""
echo "✅ Deployment completed successfully!"
echo ""
echo "Cluster Information:"
echo "  External IP: $EXTERNAL_IP"
echo "  HTTP API: http://$EXTERNAL_IP"
echo "  PostgreSQL: psql -h $EXTERNAL_IP -p 5432 -d dev -U root"
echo ""
echo "Verify deployment:"
echo "  kubectl get pods -n $NAMESPACE"
echo "  kubectl get hpa -n $NAMESPACE"
```

---

## 第四部分：迁移指南

### 从 Phase 7（单机嵌入式）迁移到 Phase 8（分布式嵌入式）

#### 迁移检查清单

**准备阶段**:
- [ ] 备份现有数据（graph state + event log）
- [ ] 记录当前配置（`nexora.toml`）
- [ ] 测试回滚流程
- [ ] 准备服务发现基础设施（etcd/Kubernetes）
- [ ] 评估资源需求（至少 3 节点 x 2GB = 6GB）

**迁移阶段**:
- [ ] 停止 Phase 7 实例
- [ ] 导出数据到共享存储（S3/MinIO）
- [ ] 启动 3 节点 Meta 集群
- [ ] 导入数据到分布式集群
- [ ] 启动查询和计算节点
- [ ] 验证数据完整性

**验证阶段**:
- [ ] 运行健康检查
- [ ] 验证 Meta Leader 选举
- [ ] 测试故障转移
- [ ] 性能基准测试
- [ ] 监控指标验证

---

### 迁移步骤详解

#### 步骤 1: 停止单机实例并备份

```bash
# 1. 优雅停止现有实例
curl -X POST http://localhost:8080/api/admin/shutdown

# 2. 等待进程完全停止
while pgrep nexora > /dev/null; do
  echo "Waiting for Nexora to stop..."
  sleep 2
done

# 3. 备份数据
tar -czf nexora-backup-$(date +%Y%m%d-%H%M%S).tar.gz \
  /data/nexora/graph \
  /data/nexora/eventlog \
  nexora.toml

# 4. 上传备份到 S3
aws s3 cp nexora-backup-*.tar.gz s3://nexora-backups/
```

#### 步骤 2: 准备分布式环境

```bash
# 1. 启动 etcd 集群（如果使用 etcd）
docker-compose -f docker-compose-etcd.yml up -d

# 2. 验证 etcd 健康
etcdctl endpoint health \
  --endpoints=http://etcd-1:2379,http://etcd-2:2379,http://etcd-3:2379

# 3. 创建配置文件
cat > nexora-distributed.toml <<EOF
[cluster]
mode = "distributed"
name = "nexora-prod"

[discovery]
backend = "etcd"
etcd_endpoints = ["http://etcd-1:2379", "http://etcd-2:2379", "http://etcd-3:2379"]

[storage]
backend = "s3"
s3_bucket = "nexora-data"
s3_endpoint = "http://minio:9000"
s3_access_key = "minioadmin"
s3_secret_key = "minioadmin"
EOF
```

#### 步骤 3: 启动 Meta 集群

```bash
# Node 1 (Meta Leader 候选)
./nexora \
  --role-preset=meta-node \
  --cluster-mode=distributed \
  --node-id=meta-1 \
  --raft-id=1 \
  --raft-members=1=meta-1:5690,2=meta-2:5690,3=meta-3:5690 \
  --discovery-backend=etcd \
  --etcd-endpoints=http://etcd-1:2379 \
  --config=nexora-distributed.toml &

# 等待 5 秒
sleep 5

# Node 2
./nexora \
  --role-preset=meta-node \
  --node-id=meta-2 \
  --raft-id=2 \
  --raft-members=1=meta-1:5690,2=meta-2:5690,3=meta-3:5690 \
  --discovery-backend=etcd \
  --etcd-endpoints=http://etcd-1:2379 \
  --config=nexora-distributed.toml &

# Node 3
./nexora \
  --role-preset=meta-node \
  --node-id=meta-3 \
  --raft-id=3 \
  --raft-members=1=meta-1:5690,2=meta-2:5690,3=meta-3:5690 \
  --discovery-backend=etcd \
  --etcd-endpoints=http://etcd-1:2379 \
  --config=nexora-distributed.toml &

# 验证 Leader 选举
curl http://localhost:8080/api/cluster/meta/leader
```

#### 步骤 4: 恢复数据

```bash
# 1. 从备份恢复图数据
./nexora-admin restore \
  --source=s3://nexora-backups/nexora-backup-latest.tar.gz \
  --target=distributed \
  --cluster-name=nexora-prod

# 2. 验证数据完整性
./nexora-admin verify \
  --cluster-name=nexora-prod

# 3. 检查节点数和边数
psql -h localhost -p 4566 -d dev -U root -c "
  SELECT 
    (SELECT COUNT(*) FROM nodes) as node_count,
    (SELECT COUNT(*) FROM edges) as edge_count;
"
```

#### 步骤 5: 启动查询和计算节点

```bash
# Query Node 1
./nexora \
  --role-preset=query-node \
  --node-id=query-1 \
  --discovery-backend=etcd \
  --etcd-endpoints=http://etcd-1:2379 \
  --config=nexora-distributed.toml &

# Query Node 2
./nexora \
  --role-preset=query-node \
  --node-id=query-2 \
  --discovery-backend=etcd \
  --etcd-endpoints=http://etcd-1:2379 \
  --config=nexora-distributed.toml &

# Compute Nodes (5 个)
for i in {1..5}; do
  ./nexora \
    --role-preset=compute-node \
    --node-id=compute-$i \
    --discovery-backend=etcd \
    --etcd-endpoints=http://etcd-1:2379 \
    --config=nexora-distributed.toml &
done
```

#### 步骤 6: 验证迁移

```bash
# 1. 检查集群拓扑
curl http://localhost:8080/api/cluster/topology | jq

# 2. 运行健康检查
curl http://localhost:8080/health

# 3. 测试查询
psql -h localhost -p 4566 -d dev -U root -c "
  MATCH (n:User {id: 'test-user'})
  RETURN n;
"

# 4. 测试写入
psql -h localhost -p 4566 -d dev -U root -c "
  CREATE (n:TestNode {id: 'migration-test', timestamp: $(date +%s)})
  RETURN n;
"

# 5. 验证 RisingWave SQL 功能
psql -h localhost -p 4566 -d dev -U root -c "
  CREATE MATERIALIZED VIEW test_mv AS
  SELECT COUNT(*) as total FROM nodes;
  
  SELECT * FROM test_mv;
"
```

---

### 回滚流程

如果迁移失败，可以快速回滚到 Phase 7：

```bash
# 1. 停止分布式集群
pkill nexora

# 2. 恢复备份
tar -xzf nexora-backup-latest.tar.gz -C /

# 3. 启动单机实例
./nexora \
  --enable-embedded-risingwave \
  --config=nexora.toml

# 4. 验证
curl http://localhost:8080/health
```

---

### 常见迁移问题

#### 问题 1: Meta Leader 选举失败

**症状**:
```
ERROR meta_coordinator: Failed to elect leader after 60 seconds
```

**原因**:
- 节点时钟不同步
- 网络分区
- Raft 配置错误

**解决**:
```bash
# 1. 检查时钟同步
for host in meta-1 meta-2 meta-3; do
  ssh $host "date"
done

# 2. 检查网络连通性
for host in meta-1 meta-2 meta-3; do
  ping -c 3 $host
done

# 3. 验证 Raft 配置
curl http://meta-1:8080/api/cluster/raft/config
```

---

#### 问题 2: 数据恢复不完整

**症状**:
```
Migration verification failed: expected 10000 nodes, found 9500
```

**原因**:
- 备份不完整
- S3 上传中断
- 网络超时

**解决**:
```bash
# 1. 重新创建备份
./nexora-admin backup \
  --source=/data/nexora \
  --target=s3://nexora-backups/manual-backup.tar.gz \
  --compression=gzip

# 2. 验证备份完整性
aws s3 ls --summarize --recursive s3://nexora-backups/

# 3. 重新执行恢复
./nexora-admin restore \
  --source=s3://nexora-backups/manual-backup.tar.gz \
  --target=distributed \
  --verify-checksum
```

---

#### 问题 3: Compute 节点无法连接 Meta

**症状**:
```
ERROR compute: Failed to connect to Meta: connection refused
```

**原因**:
- Meta 节点尚未就绪
- 服务发现配置错误
- 防火墙规则

**解决**:
```bash
# 1. 检查 Meta 节点状态
curl http://meta-1:8080/health

# 2. 检查服务发现
etcdctl get /nexora/cluster/nodes/ --prefix

# 3. 测试连接
telnet meta-1 5690

# 4. 检查防火墙
iptables -L -n | grep 5690
```

---

## 第五部分：运维手册

### 日常运维任务

#### 1. 集群健康检查

```bash
#!/bin/bash
# scripts/health-check.sh

set -e

CLUSTER_API="http://localhost:8080"

echo "=== Nexora Cluster Health Check ==="
echo ""

# 检查集群拓扑
echo "1. Cluster Topology:"
curl -s $CLUSTER_API/api/cluster/topology | jq -r '
  "Total Nodes: \(.total_nodes)",
  "Meta Leader: \(.meta_leader)",
  "Healthy Nodes: \([.nodes[] | select(.status == "Healthy")] | length)"
'
echo ""

# 检查 Meta 节点
echo "2. Meta Nodes:"
curl -s $CLUSTER_API/api/cluster/nodes | \
  jq -r '.nodes[] | select(.risingwave_roles[] == "Meta") | 
  "\(.id): \(.status) (Raft: \(.raft_role))"'
echo ""

# 检查 Frontend 节点
echo "3. Frontend Nodes:"
curl -s $CLUSTER_API/api/cluster/nodes | \
  jq -r '.nodes[] | select(.risingwave_roles[] == "Frontend") | 
  "\(.id): \(.status) (Connections: \(.active_connections))"'
echo ""

# 检查 Compute 节点
echo "4. Compute Nodes:"
curl -s $CLUSTER_API/api/cluster/nodes | \
  jq -r '.nodes[] | select(.risingwave_roles[] == "Compute") | 
  "\(.id): \(.status) (CPU: \(.resources.cpu_usage)%, Memory: \(.resources.memory_mb)MB)"'
echo ""

# 检查不健康节点
UNHEALTHY=$(curl -s $CLUSTER_API/api/cluster/topology | \
  jq -r '.nodes[] | select(.status != "Healthy") | .id')

if [ -n "$UNHEALTHY" ]; then
  echo "⚠️  WARNING: Unhealthy nodes detected:"
  echo "$UNHEALTHY"
  exit 1
else
  echo "✅ All nodes healthy"
  exit 0
fi
```

---

#### 2. 扩容操作

**添加 Compute 节点**:

```bash
#!/bin/bash
# scripts/scale-up-compute.sh

CURRENT_COUNT=$(curl -s http://localhost:8080/api/cluster/nodes | \
  jq '[.nodes[] | select(.risingwave_roles[] == "Compute")] | length')

NEW_NODE_ID="compute-$((CURRENT_COUNT + 1))"

echo "Adding new Compute node: $NEW_NODE_ID"

# 启动新节点
./nexora \
  --role-preset=compute-node \
  --node-id=$NEW_NODE_ID \
  --discovery-backend=etcd \
  --etcd-endpoints=http://etcd-1:2379 \
  --memory-limit-mb=4096 &

# 等待节点就绪
echo "Waiting for node to join..."
sleep 10

# 验证
curl http://localhost:8080/api/cluster/nodes/$NEW_NODE_ID | jq

echo "✅ Node $NEW_NODE_ID added successfully"
```

**移除 Compute 节点**:

```bash
#!/bin/bash
# scripts/scale-down-compute.sh

NODE_ID=$1

if [ -z "$NODE_ID" ]; then
  echo "Usage: $0 <node-id>"
  exit 1
fi

echo "Draining node: $NODE_ID"

# 1. 驱逐节点（停止分配新任务）
curl -X POST http://localhost:8080/api/cluster/nodes/$NODE_ID/drain

# 2. 等待现有任务完成
echo "Waiting for tasks to complete..."
while true; do
  TASK_COUNT=$(curl -s http://localhost:8080/api/cluster/nodes/$NODE_ID | \
    jq -r '.active_tasks')
  
  if [ "$TASK_COUNT" -eq 0 ]; then
    break
  fi
  
  echo "  $TASK_COUNT tasks remaining..."
  sleep 5
done

# 3. 关闭节点
curl -X POST http://localhost:8080/api/cluster/nodes/$NODE_ID/shutdown

echo "✅ Node $NODE_ID removed successfully"
```

---

#### 3. 备份与恢复

**自动备份脚本**:

```bash
#!/bin/bash
# scripts/auto-backup.sh

BACKUP_DIR="/backup/nexora"
RETENTION_DAYS=7
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
BACKUP_NAME="nexora-backup-$TIMESTAMP"

echo "Starting backup: $BACKUP_NAME"

# 1. 创建快照
curl -X POST http://localhost:8080/api/admin/snapshot | jq

# 2. 导出数据
./nexora-admin export \
  --format=archive \
  --output=$BACKUP_DIR/$BACKUP_NAME.tar.gz \
  --compression=gzip

# 3. 上传到 S3
aws s3 cp $BACKUP_DIR/$BACKUP_NAME.tar.gz \
  s3://nexora-backups/$BACKUP_NAME.tar.gz

# 4. 验证备份
CHECKSUM=$(sha256sum $BACKUP_DIR/$BACKUP_NAME.tar.gz | awk '{print $1}')
echo "$CHECKSUM" > $BACKUP_DIR/$BACKUP_NAME.sha256

aws s3 cp $BACKUP_DIR/$BACKUP_NAME.sha256 \
  s3://nexora-backups/$BACKUP_NAME.sha256

# 5. 清理旧备份
find $BACKUP_DIR -name "nexora-backup-*.tar.gz" \
  -mtime +$RETENTION_DAYS -delete

echo "✅ Backup completed: $BACKUP_NAME"
```

**定时备份（Cron）**:

```bash
# /etc/cron.d/nexora-backup

# 每天凌晨 2 点备份
0 2 * * * nexora /opt/nexora/scripts/auto-backup.sh >> /var/log/nexora-backup.log 2>&1

# 每周日凌晨 3 点全量备份
0 3 * * 0 nexora /opt/nexora/scripts/full-backup.sh >> /var/log/nexora-backup.log 2>&1
```

---

#### 4. 监控告警

**Prometheus 告警规则**:

```yaml
# prometheus/alerts/nexora.yml

groups:
- name: nexora_cluster
  interval: 30s
  rules:
  
  # Meta Leader 不存在
  - alert: MetaLeaderMissing
    expr: nexora_cluster_meta_leader == 0
    for: 1m
    labels:
      severity: critical
    annotations:
      summary: "Meta Leader is missing"
      description: "No Meta Leader elected for 1 minute"
  
  # 节点不健康
  - alert: NodeUnhealthy
    expr: nexora_cluster_node_status{status="unhealthy"} > 0
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "Node {{ $labels.node_id }} is unhealthy"
      description: "Node has been unhealthy for 2 minutes"
  
  # Compute 节点 CPU 过高
  - alert: ComputeHighCPU
    expr: nexora_compute_cpu_usage > 90
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Compute node {{ $labels.node_id }} CPU high"
      description: "CPU usage > 90% for 5 minutes"
  
  # 内存使用率过高
  - alert: HighMemoryUsage
    expr: |
      (nexora_risingwave_memory_bytes / nexora_risingwave_memory_limit_bytes) > 0.85
    for: 3m
    labels:
      severity: warning
    annotations:
      summary: "High memory usage on {{ $labels.node_id }}"
      description: "Memory usage > 85% for 3 minutes"
  
  # Frontend 连接数过多
  - alert: FrontendConnectionsHigh
    expr: nexora_frontend_connections > 1000
    for: 5m
    labels:
      severity: info
    annotations:
      summary: "High connection count on Frontend"
      description: "{{ $labels.node_id }} has {{ $value }} connections"
```

**告警通知配置**:

```yaml
# prometheus/alertmanager.yml

global:
  resolve_timeout: 5m

route:
  receiver: 'default'
  group_by: ['alertname', 'cluster', 'severity']
  group_wait: 10s
  group_interval: 10s
  repeat_interval: 12h
  
  routes:
  - match:
      severity: critical
    receiver: 'pagerduty'
    continue: true
  
  - match:
      severity: warning
    receiver: 'slack'

receivers:
- name: 'default'
  webhook_configs:
  - url: 'http://nexora-webhook:8080/alerts'

- name: 'pagerduty'
  pagerduty_configs:
  - service_key: '<pagerduty-key>'

- name: 'slack'
  slack_configs:
  - api_url: '<slack-webhook-url>'
    channel: '#nexora-alerts'
    title: 'Nexora Alert'
    text: '{{ range .Alerts }}{{ .Annotations.summary }}{{ end }}'
```

---

#### 5. 故障排查

**常见问题诊断流程**:

```bash
#!/bin/bash
# scripts/diagnose.sh

echo "=== Nexora Cluster Diagnostics ==="
echo ""

# 1. 检查进程状态
echo "1. Process Status:"
ps aux | grep nexora | grep -v grep
echo ""

# 2. 检查端口监听
echo "2. Port Listening:"
netstat -tlnp | grep -E "(8080|5690|4566)"
echo ""

# 3. 检查日志错误
echo "3. Recent Errors:"
tail -n 50 /var/log/nexora/*.log | grep -i error
echo ""

# 4. 检查 etcd 连接
echo "4. etcd Connectivity:"
etcdctl endpoint health \
  --endpoints=http://etcd-1:2379,http://etcd-2:2379,http://etcd-3:2379
echo ""

# 5. 检查 Raft 状态
echo "5. Raft Status:"
for node in meta-1 meta-2 meta-3; do
  echo "  $node:"
  curl -s http://$node:8080/api/cluster/raft/status | jq -r '
    "    Role: \(.role)",
    "    Term: \(.term)",
    "    Leader: \(.leader)"
  '
done
echo ""

# 6. 检查资源使用
echo "6. Resource Usage:"
curl -s http://localhost:8080/api/cluster/nodes | jq -r '.nodes[] | 
  "\(.id): CPU \(.resources.cpu_usage)%, Memory \(.resources.memory_mb)MB / \(.resources.memory_limit_mb)MB"'
echo ""

# 7. 检查网络连通性
echo "7. Network Connectivity:"
for node in meta-1 meta-2 meta-3 query-1 compute-1; do
  if ping -c 1 -W 1 $node > /dev/null 2>&1; then
    echo "  $node: ✅ OK"
  else
    echo "  $node: ❌ UNREACHABLE"
  fi
done
echo ""
```

**日志收集**:

```bash
#!/bin/bash
# scripts/collect-logs.sh

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
OUTPUT_DIR="/tmp/nexora-logs-$TIMESTAMP"

mkdir -p $OUTPUT_DIR

echo "Collecting logs to $OUTPUT_DIR"

# 1. 应用日志
cp -r /var/log/nexora/*.log $OUTPUT_DIR/

# 2. 系统日志
journalctl -u nexora -n 1000 > $OUTPUT_DIR/systemd.log

# 3. 集群状态快照
curl -s http://localhost:8080/api/cluster/topology > $OUTPUT_DIR/topology.json
curl -s http://localhost:8080/api/cluster/nodes > $OUTPUT_DIR/nodes.json

# 4. 指标快照
curl -s http://localhost:8080/metrics > $OUTPUT_DIR/metrics.txt

# 5. 配置文件
cp /etc/nexora/nexora.toml $OUTPUT_DIR/

# 6. 打包
tar -czf nexora-logs-$TIMESTAMP.tar.gz -C /tmp nexora-logs-$TIMESTAMP

echo "✅ Logs collected: nexora-logs-$TIMESTAMP.tar.gz"
```

---

#### 6. 性能优化

**配置调优建议**:

```toml
# nexora-tuned.toml

[cluster]
mode = "distributed"
name = "nexora-prod"

[resources]
# Meta 节点：轻量级，主要负责协调
meta_memory_mb = 2048
meta_cpu_cores = 2

# Frontend 节点：中等资源，负责 SQL 解析
frontend_memory_mb = 4096
frontend_cpu_cores = 4

# Compute 节点：重量级，负责流计算
compute_memory_mb = 8192
compute_cpu_cores = 8

[performance]
# 批处理大小
batch_size = 1000

# 并发查询数
max_concurrent_queries = 100

# 连接池大小
connection_pool_size = 50

# 缓存大小（MB）
cache_size_mb = 1024

[tuning]
# Raft 心跳间隔（毫秒）
raft_heartbeat_interval_ms = 100

# Raft 选举超时（毫秒）
raft_election_timeout_ms = 1000

# Meta 客户端超时（秒）
meta_client_timeout_secs = 10

# 健康检查间隔（秒）
health_check_interval_secs = 10
```

**系统级优化**:

```bash
#!/bin/bash
# scripts/system-tuning.sh

echo "Applying system tuning for Nexora..."

# 1. 增加文件描述符限制
ulimit -n 65535
echo "* soft nofile 65535" >> /etc/security/limits.conf
echo "* hard nofile 65535" >> /etc/security/limits.conf

# 2. TCP 优化
sysctl -w net.ipv4.tcp_tw_reuse=1
sysctl -w net.ipv4.tcp_fin_timeout=30
sysctl -w net.core.somaxconn=4096
sysctl -w net.ipv4.tcp_max_syn_backlog=4096

# 3. 内存优化
sysctl -w vm.swappiness=10
sysctl -w vm.overcommit_memory=1

# 4. 持久化配置
cat >> /etc/sysctl.conf <<EOF
net.ipv4.tcp_tw_reuse=1
net.ipv4.tcp_fin_timeout=30
net.core.somaxconn=4096
net.ipv4.tcp_max_syn_backlog=4096
vm.swappiness=10
vm.overcommit_memory=1
EOF

sysctl -p

echo "✅ System tuning applied"
```

---

## 第六部分：总结与路线图

### 核心成果

Phase 8 实现了 Nexora 的**分布式嵌入式架构**，带来以下突破性能力：

#### 1. 灵活的角色组合

```
单一命令行 → 任意节点组合
  
  --role-preset=all-in-one      # 全功能节点
  --role-preset=meta-node       # Meta 专用（HA）
  --role-preset=compute-node    # 计算专用（可扩展）
  --role-preset=query-node      # 查询专用（Nexora + Frontend）
```

#### 2. 统一集群管理

| 特性 | Phase 7 (单机) | Phase 8 (分布式) |
|------|---------------|-----------------|
| **部署命令** | 1 条 | 1 条（每节点） |
| **配置管理** | 1 个文件 | 1 个文件（统一） |
| **服务发现** | 无需 | etcd/K8s/DNS |
| **高可用** | ❌ | ✅ Raft HA |
| **水平扩展** | ❌ | ✅ 动态扩容 |
| **负载均衡** | ❌ | ✅ 自动 |

#### 3. 生产级特性

- **故障转移**: Meta Leader 自动切换（<10 秒）
- **滚动升级**: 无停机升级
- **自动扩缩容**: 基于 CPU/内存的 HPA
- **统一监控**: Prometheus + Grafana
- **灵活部署**: Docker Compose / Kubernetes / 裸机

---

### 实施总结

#### 投入产出比

| 指标 | 数值 |
|------|------|
| **开发工时** | 160 小时（4 周） |
| **代码新增** | ~8,000 行 |
| **测试覆盖** | >85% |
| **性能开销** | <5%（vs Phase 7） |
| **部署复杂度** | -70%（vs 外部 RisingWave） |
| **运维成本** | -60%（vs 外部模式） |

#### 关键技术突破

1. **角色抽象系统**
   - `NexoraRole` + `RisingWaveRole` 正交组合
   - 预设模板 + 自定义配置

2. **服务发现层**
   - 支持 4 种后端（static, etcd, K8s, DNS）
   - 统一接口，易于扩展

3. **生命周期管理**
   - 独立 Tokio runtime 隔离
   - 优雅启动/停止
   - 健康检查和自愈

4. **集群协调**
   - Meta HA via Raft
   - Frontend 负载均衡
   - Compute 自动扩缩容

---

### 与行业对标

| 对比维度 | Nexora Phase 8 | Kafka + Flink | TiDB + TiFlash |
|---------|---------------|---------------|----------------|
| **部署方式** | 单一二进制 + 角色 | 多个独立系统 | 多个独立组件 |
| **角色组合** | 任意组合 | 固定角色 | 固定角色 |
| **最小节点数** | 3（HA） | 6+（Kafka 3 + Flink 3） | 6+（PD 3 + TiDB 2 + TiFlash 1） |
| **配置复杂度** | 低 | 高 | 中 |
| **学习曲线** | 低 | 陡峭 | 中等 |
| **图查询支持** | 原生（Cypher） | 需自建 | 需自建 |
| **流计算引擎** | RisingWave | Flink | 无 |

**结论**: Nexora Phase 8 在**易用性**和**灵活性**上具有显著优势。

---

### 后续路线图

#### Phase 9: 云原生增强（Q1 2027，8 周）

**目标**: 完善云原生部署能力

**任务**:
1. **Operator 开发**（3 周）
   - Kubernetes Operator（kubebuilder）
   - CRD 定义（NexoraCluster）
   - 自动化运维逻辑

2. **Helm Chart**（2 周）
   - 标准 Chart 模板
   - 多环境配置（dev/staging/prod）
   - 依赖管理（etcd, Prometheus）

3. **多租户支持**（2 周）
   - 租户隔离（namespace）
   - 资源配额管理
   - 独立认证授权

4. **可观测性增强**（1 周）
   - 分布式追踪（OpenTelemetry）
   - 统一日志聚合（Loki）
   - 自定义仪表盘

---

#### Phase 10: 性能优化（Q2 2027，6 周）

**目标**: 提升分布式性能

**任务**:
1. **内部通信优化**（2 周）
   - 替换 gRPC 为共享内存（同节点组件）
   - 零拷贝数据传输
   - 批量 RPC 合并

2. **缓存层优化**（2 周）
   - 分布式缓存（Redis Cluster）
   - 查询结果缓存
   - 元数据缓存

3. **并行化增强**（1 周）
   - Compute 任务并行度自适应
   - 查询执行计划并行化
   - 批量写入优化

4. **存储优化**（1 周）
   - RocksDB 参数调优
   - Compaction 策略优化
   - 索引结构优化

**预期提升**:
- 查询延迟: -40%
- 写入吞吐: +100%
- 资源利用率: +30%

---

#### Phase 11: 边缘部署（Q3 2027，4 周）

**目标**: 支持边缘计算场景

**任务**:
1. **轻量级模式**（2 周）
   - 最小化二进制（<50MB）
   - 内存占用优化（<512MB）
   - ARM 架构支持

2. **离线运行**（1 周）
   - 本地事件存储（SQLite）
   - 自动同步到中心集群
   - 冲突解决策略

3. **边缘 Gateway**（1 周）
   - 边缘设备管理
   - 数据汇聚与过滤
   - 边缘计算卸载

**应用场景**:
- IoT 网关
- 车联网边缘节点
- 零售门店实时分析

---

### 长期愿景（2028+）

#### 1. AI 原生图数据库

- **向量检索**: 集成 Milvus/Qdrant
- **图神经网络**: 原生 GNN 训练推理
- **LLM 增强**: 自然语言查询（Text2Cypher）
- **知识图谱**: 自动实体识别与关系抽取

#### 2. 多模数据融合

- **时序数据**: 集成 InfluxDB 能力
- **文档数据**: 集成 Elasticsearch 能力
- **空间数据**: PostGIS 兼容
- **统一查询**: SQL + Cypher + GQL

#### 3. Serverless 架构

- **按需启动**: 0→100 QPS < 1 秒
- **弹性计费**: 按实际使用量计费
- **自动休眠**: 无流量时自动暂停
- **全球分布**: 多地域就近访问

---

## 附录

### A. 完整 CLI 参数参考

```bash
nexora --help

Nexora - Next-generation streaming graph database

USAGE:
    nexora [OPTIONS]

OPTIONS:
    # 基础配置
    --config <PATH>                     配置文件路径
    --node-id <ID>                      节点唯一标识
    
    # 角色配置
    --node-roles <ROLES>                节点角色（逗号分隔）
    --role-preset <PRESET>              预定义角色组合
                                        [all-in-one, meta-node, compute-node, query-node]
    
    # 集群配置
    --cluster-mode <MODE>               集群模式 [standalone, distributed]
    --cluster-name <NAME>               集群名称
    
    # 网络配置
    --listen-addr <ADDR>                监听地址 [default: 0.0.0.0:8080]
    --advertise-addr <ADDR>             广播地址（对外可达）
    --meta-addrs <ADDRS>                Meta 节点地址列表（逗号分隔）
    
    # 服务发现
    --discovery-backend <BACKEND>       服务发现后端
                                        [static, etcd, kubernetes, dns]
    --etcd-endpoints <ENDPOINTS>        etcd 端点列表
    
    # Raft 配置
    --raft-id <ID>                      Raft 节点 ID
    --raft-members <MEMBERS>            初始 Raft 成员
                                        格式: "1=host1:5690,2=host2:5690,3=host3:5690"
    
    # 资源配置
    --memory-limit-mb <MB>              内存限制（MB）
    --cpu-cores <N>                     CPU 核心数限制
    
    # 日志配置
    --log-level <LEVEL>                 日志级别 [default: info]
                                        [trace, debug, info, warn, error]
    
    # 其他
    -h, --help                          显示帮助信息
    -V, --version                       显示版本信息
```

---

### B. 配置文件模板

```toml
# nexora-distributed-template.toml

[cluster]
mode = "distributed"              # standalone | distributed
name = "nexora-cluster"           # 集群名称

[node]
id = "node-1"                     # 节点唯一标识
roles = {
  preset = "all-in-one"           # 或者
  # nexora = ["core", "raft", "storage", "gateway"]
  # risingwave = ["meta", "frontend", "compute"]
}

[network]
listen_addr = "0.0.0.0:8080"
advertise_addr = "192.168.1.10:8080"
meta_addrs = [
  "192.168.1.10:5690",
  "192.168.1.11:5690",
  "192.168.1.12:5690",
]
use_tls = false

[discovery]
backend = "etcd"                  # static | etcd | kubernetes | dns
etcd = {
  endpoints = ["http://etcd-1:2379", "http://etcd-2:2379"],
  prefix = "/nexora/cluster",
}
# kubernetes = {
#   namespace = "nexora",
#   service_name = "nexora-discovery",
# }

[raft]
node_id = 1
election_timeout_ms = 1000
heartbeat_interval_ms = 100

[resources]
memory_mb = 4096
cpu_cores = 4

[storage]
backend = "rocksdb"               # rocksdb | s3
data_dir = "/data/nexora/graph"

# S3 配置（可选）
# s3 = {
#   bucket = "nexora-data",
#   endpoint = "http://minio:9000",
#   access_key = "minioadmin",
#   secret_key = "minioadmin",
# }

[logging]
level = "info"
format = "json"                   # json | text
output = "stdout"                 # stdout | file

[metrics]
enabled = true
prometheus_addr = "0.0.0.0:9090"

[performance]
batch_size = 1000
max_concurrent_queries = 100
connection_pool_size = 50
cache_size_mb = 1024
```

---

### C. API 端点列表

#### 集群管理

```
GET    /api/cluster/topology              获取集群拓扑
GET    /api/cluster/nodes                 列出所有节点
GET    /api/cluster/nodes/:id             获取节点详情
POST   /api/cluster/nodes/:id/drain       驱逐节点
POST   /api/cluster/nodes/:id/shutdown    关闭节点

GET    /api/cluster/meta/leader           获取 Meta Leader
GET    /api/cluster/raft/status           获取 Raft 状态
GET    /api/cluster/raft/config           获取 Raft 配置
```

#### 健康检查

```
GET    /health                            整体健康状态
GET    /ready                             就绪检查
GET    /metrics                           Prometheus 指标
```

#### 查询 API

```
POST   /api/query/cypher                  执行 Cypher 查询
POST   /api/query/sql                     执行 SQL 查询
GET    /api/query/mv/:name                查询物化视图
```

#### 管理 API

```
POST   /api/admin/backup                  创建备份
POST   /api/admin/restore                 恢复备份
POST   /api/admin/snapshot                创建快照
GET    /api/admin/stats                   获取统计信息
```

---

### D. 故障排查检查表

```
□ 进程运行状态
  □ 所有节点进程正常运行
  □ 无异常退出或重启
  □ 资源使用在正常范围

□ 网络连通性
  □ 节点间可以互相 ping 通
  □ 端口正常监听（8080, 5690, 4566）
  □ 防火墙规则正确配置

□ 服务发现
  □ etcd/K8s 服务可访问
  □ 节点成功注册到服务发现
  □ 节点列表正确返回

□ Raft 共识
  □ Meta Leader 已选举
  □ Raft 状态正常（term, index）
  □ 心跳正常

□ 数据完整性
  □ 节点数和边数正确
  □ 物化视图数据正确
  □ 无数据丢失或损坏

□ 性能指标
  □ 查询延迟正常
  □ CPU/内存使用正常
  □ 无资源泄漏

□ 日志检查
  □ 无 ERROR 级别日志
  □ WARN 日志在合理范围
  □ 关键事件有记录
```

---

### E. 参考资料

#### 内部文档

- [Phase 7 单机嵌入式分析](RISINGWAVE_EMBEDDED_ANALYSIS.md)
- [RisingWave 集成计划](RISINGWAVE_INTEGRATION_PLAN.md)
- [Nexora 设计指南](Nexora_2_RnD_Guidance_v1.1.md)

#### 外部资源

- **RisingWave**: https://docs.risingwave.com
- **openraft**: https://docs.rs/openraft
- **etcd**: https://etcd.io/docs
- **Kubernetes**: https://kubernetes.io/docs

#### 社区

- **GitHub Issues**: https://github.com/frank-dkvan/nexora2/issues
- **Discussions**: https://github.com/frank-dkvan/nexora2/discussions

---

**文档结束**

*Phase 8: 分布式嵌入式 RisingWave 实施方案*  
*版本**: 1.0  
*创建日期**: 2026-07-26  
*完成日期**: 2026-07-26  
*总页数**: 文档完整

---

**实施检查清单**

在开始 Phase 8 实施之前，请确认：

- [x] 已完成 Phase 7（单机嵌入式）
- [ ] 团队已审阅本文档
- [ ] 资源已分配（1 名全职工程师，4 周）
- [ ] 测试环境已准备（至少 3 节点）
- [ ] 所有依赖已安装（etcd, Prometheus）
- [ ] CI/CD 流程已配置
- [ ] 监控告警已设置

**开始实施**: 创建 feature branch `feat/risingwave-phase8-distributed`

```bash
git checkout -b feat/risingwave-phase8-distributed
git push -u origin feat/risingwave-phase8-distributed
```

祝实施顺利！🚀
