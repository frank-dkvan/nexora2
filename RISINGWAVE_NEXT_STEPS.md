# RisingWave 集成 - 下一步行动方案

## 当前状态

❌ **编译阻塞**: RisingWave v3.0.2 无法在 macOS 上从源代码编译
- 65 个生命周期/HRTB 错误
- 已尝试所有可能的解决方案
- 详见: RISINGWAVE_COMPILATION_BLOCKER.md

## 推荐方案：Docker 集成

### 为什么选择 Docker？

1. **避免编译问题** - 使用官方预构建镜像
2. **跨平台一致性** - macOS 和 Linux 都能运行
3. **开发友好** - 本地开发体验良好
4. **生产就绪** - 官方支持的部署方式

### 实施计划

#### Step 1: 创建 Docker Compose 配置 (30 分钟)

```bash
mkdir -p docker/risingwave
cd docker/risingwave

cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  risingwave-standalone:
    image: risingwavelabs/risingwave:v3.0.2
    container_name: nexora-risingwave
    ports:
      - "4566:4566"   # PostgreSQL-compatible frontend
      - "5690:5690"   # Meta service
    environment:
      - RUST_LOG=info
      - RW_BACKEND=memory  # or 'hummock' for production
    volumes:
      - risingwave-data:/data
    command: standalone
    healthcheck:
      test: ["CMD", "pg_isready", "-h", "localhost", "-p", "4566"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  risingwave-data:
EOF

# 测试启动
docker compose up -d
docker compose ps
docker compose logs risingwave-standalone
```

#### Step 2: 实现 nexora-risingwave crate (2 小时)

```bash
cd /Users/frank/aiCoding/nexora2

# 创建新 crate
cargo new --lib crates/nexora-risingwave

# 添加依赖
cat >> crates/nexora-risingwave/Cargo.toml << 'EOF'

[dependencies]
tokio = { version = "1", features = ["full"] }
tokio-postgres = "0.7"
anyhow = "1"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
tokio-test = "0.4"
EOF
```

实现 Docker 后端管理:

```rust
// crates/nexora-risingwave/src/lib.rs
use anyhow::Result;
use std::process::Command;
use tokio_postgres::{NoTls, Client};

pub struct RisingWaveDocker {
    compose_file: String,
    frontend_url: String,
}

impl RisingWaveDocker {
    pub fn new(compose_file: &str) -> Self {
        Self {
            compose_file: compose_file.to_string(),
            frontend_url: "postgres://root@localhost:4566/dev".to_string(),
        }
    }

    pub async fn start(&self) -> Result<()> {
        let output = Command::new("docker")
            .args(&["compose", "-f", &self.compose_file, "up", "-d"])
            .output()?;
        
        if !output.status.success() {
            anyhow::bail!("Failed to start RisingWave: {:?}", output);
        }
        
        // Wait for health check
        self.wait_ready().await?;
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        Command::new("docker")
            .args(&["compose", "-f", &self.compose_file, "down"])
            .status()?;
        Ok(())
    }

    async fn wait_ready(&self) -> Result<()> {
        use std::time::Duration;
        use tokio::time::sleep;

        for _ in 0..30 {
            if self.test_connection().await.is_ok() {
                return Ok(());
            }
            sleep(Duration::from_secs(2)).await;
        }
        anyhow::bail!("RisingWave did not become ready in time")
    }

    async fn test_connection(&self) -> Result<()> {
        let (client, connection) = tokio_postgres::connect(&self.frontend_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client.execute("SELECT 1", &[]).await?;
        Ok(())
    }

    pub async fn client(&self) -> Result<Client> {
        let (client, connection) = tokio_postgres::connect(&self.frontend_url, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });
        Ok(client)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_docker_backend() {
        let rw = RisingWaveDocker::new("../../docker/risingwave/docker-compose.yml");
        rw.start().await.unwrap();
        
        let client = rw.client().await.unwrap();
        let row = client.query_one("SELECT 1 as num", &[]).await.unwrap();
        let num: i32 = row.get(0);
        assert_eq!(num, 1);
        
        rw.stop().await.unwrap();
    }
}
EOF
```

#### Step 3: 集成到 nexora-app (1 小时)

```bash
# 更新 Cargo.toml
cat >> Cargo.toml << 'EOF'

[workspace]
members = [
    # ... 现有 members ...
    "crates/nexora-risingwave",
]
EOF
```

更新 nexora.toml:

```toml
[risingwave]
enabled = false  # 默认关闭
backend = "docker"
compose_file = "docker/risingwave/docker-compose.yml"
frontend_url = "postgres://root@localhost:4566/dev"
```

更新 nexora-app:

```rust
// crates/nexora-app/src/main.rs
#[cfg(feature = "risingwave")]
use nexora_risingwave::RisingWaveDocker;

async fn start_services(config: &Config) -> Result<()> {
    #[cfg(feature = "risingwave")]
    if config.risingwave.enabled {
        tracing::info!("Starting RisingWave...");
        let rw = RisingWaveDocker::new(&config.risingwave.compose_file);
        rw.start().await?;
        tracing::info!("RisingWave started successfully");
    }
    
    Ok(())
}
```

#### Step 4: 测试端到端流程 (30 分钟)

```bash
# 1. 启动 RisingWave
cd docker/risingwave
docker compose up -d

# 2. 测试连接
psql -h localhost -p 4566 -d dev -U root

# 3. 创建测试表和 MV
CREATE TABLE events (
    id BIGINT,
    name VARCHAR,
    timestamp TIMESTAMPTZ
);

CREATE MATERIALIZED VIEW event_counts AS
SELECT name, COUNT(*) as count
FROM events
GROUP BY name;

# 4. 插入测试数据
INSERT INTO events VALUES 
    (1, 'login', '2026-07-28 12:00:00+00'),
    (2, 'logout', '2026-07-28 12:05:00+00'),
    (3, 'login', '2026-07-28 12:10:00+00');

# 5. 查询 MV
SELECT * FROM event_counts;

# 6. 清理
docker compose down -v
```

### 验收标准

- [ ] Docker Compose 可以启动 RisingWave
- [ ] nexora-risingwave crate 可以连接到 RisingWave
- [ ] 健康检查工作正常
- [ ] 可以创建表和物化视图
- [ ] 可以查询数据
- [ ] nexora-app 可以通过配置启用/禁用 RisingWave

### 时间估算

- Step 1: 30 分钟
- Step 2: 2 小时
- Step 3: 1 小时
- Step 4: 30 分钟
- **总计**: 4 小时

## 替代方案

如果 Docker 方案不可接受，考虑：

1. **方案 B**: 仅在 Linux CI/生产环境中集成
   - 开发环境跳过 RisingWave
   - CI 和生产环境使用预编译二进制或 Docker

2. **方案 C**: 等待上游修复
   - 向 RisingWave 提交 Issue
   - 跟踪 main 分支的修复进度
   - 估计等待时间: 未知

## 决策点

请决定：
1. 是否接受 Docker 方案？
2. 如果不接受，选择哪个替代方案？
3. 何时开始实施？

---

创建日期: 2026-07-28
状态: 等待决策
