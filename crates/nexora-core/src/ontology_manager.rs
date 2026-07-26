//! OntologyManager - 本体管理器 (阶段6)
//!
//! 负责:
//! 1. 管理 DomainPackage 定义 (创建/查询/列表/删除)
//! 2. 持久化到 ControlPlaneStore (Namespace::DomainDef)
//! 3. 启动时从存储恢复
//!
//! 仿照 MaterializedViewManager 模式,提供 store-backed 的持久化。

use crate::control_plane_store::{ControlPlaneStore, Namespace};
use crate::domain_package::{DomainLoader, DomainPackage};
use std::sync::Arc;
use tokio::sync::RwLock;

/// 本体管理器错误
#[derive(Debug, thiserror::Error)]
pub enum OntologyError {
    #[error("validation failed: {0}")]
    Validation(String),
    #[error("domain not found: {0}")]
    NotFound(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

/// 本体管理器
pub struct OntologyManager {
    /// 内存中的 DomainLoader (索引 + 校验)
    loader: RwLock<DomainLoader>,
    /// 可选的持久化存储
    store: Option<Arc<dyn ControlPlaneStore>>,
}

impl OntologyManager {
    /// 创建一个纯内存的 OntologyManager (无持久化)
    pub fn new() -> Self {
        Self {
            loader: RwLock::new(DomainLoader::new()),
            store: None,
        }
    }

    /// 创建一个 store-backed 的 OntologyManager,并从存储恢复已有本体
    pub async fn with_store(store: Arc<dyn ControlPlaneStore>) -> Result<Self, OntologyError> {
        let mut loader = DomainLoader::new();

        // 从存储恢复已有本体定义 (ControlPlaneStore 方法是同步的)
        let entries = store
            .list(Namespace::DomainDef)
            .map_err(|e| OntologyError::Storage(format!("failed to list domains: {}", e)))?;

        for (domain, bytes) in entries {
            match serde_json::from_slice::<DomainPackage>(&bytes) {
                Ok(pkg) => {
                    // 恢复时跳过校验错误的记录(仅告警)
                    if let Err(e) = loader.validate(&pkg) {
                        tracing::warn!("Skipping invalid domain '{}' on restore: {}", domain, e);
                        continue;
                    }
                    // 直接插入 (通过 register 内部方法)
                    let _ = loader.load_from_package(pkg);
                    tracing::info!("Restored domain '{}' from store", domain);
                }
                Err(e) => {
                    tracing::warn!("Failed to deserialize domain '{}': {}", domain, e);
                }
            }
        }

        Ok(Self {
            loader: RwLock::new(loader),
            store: Some(store),
        })
    }

    /// 创建/更新本体 (从 DomainPackage)
    ///
    /// 校验通过后:① 更新内存 loader ② 持久化到 store
    pub async fn create(&self, pkg: DomainPackage) -> Result<String, OntologyError> {
        let domain = pkg.schema.domain.clone();

        // 1. 校验
        {
            let loader = self.loader.read().await;
            loader.validate(&pkg).map_err(OntologyError::Validation)?;
        }

        // 2. 持久化 (先持久化再更新内存,保证一致性)
        if let Some(store) = &self.store {
            let bytes = serde_json::to_vec(&pkg)
                .map_err(|e| OntologyError::Serialization(e.to_string()))?;
            store
                .put(Namespace::DomainDef, &domain, &bytes)
                .map_err(|e| OntologyError::Storage(e.to_string()))?;
        }

        // 3. 更新内存 loader
        {
            let mut loader = self.loader.write().await;
            loader
                .load_from_package(pkg)
                .map_err(OntologyError::Validation)?;
        }

        tracing::info!("Created/updated ontology '{}'", domain);
        Ok(domain)
    }

    /// 从 YAML 字符串创建本体
    pub async fn create_from_yaml(&self, yaml: &str) -> Result<String, OntologyError> {
        let pkg: DomainPackage = serde_yaml::from_str(yaml)
            .map_err(|e| OntologyError::Serialization(format!("YAML parse error: {}", e)))?;
        self.create(pkg).await
    }

    /// 获取本体定义
    pub async fn get(&self, domain: &str) -> Option<DomainPackage> {
        self.loader.read().await.get(domain).cloned()
    }

    /// 列出所有本体域名
    pub async fn list(&self) -> Vec<String> {
        self.loader.read().await.list()
    }

    /// 删除本体
    pub async fn remove(&self, domain: &str) -> Result<(), OntologyError> {
        // 1. 从存储删除
        if let Some(store) = &self.store {
            store
                .delete(Namespace::DomainDef, domain)
                .map_err(|e| OntologyError::Storage(e.to_string()))?;
        }

        // 2. 从内存删除
        let removed = self.loader.write().await.remove(domain);
        if !removed {
            return Err(OntologyError::NotFound(domain.to_string()));
        }

        tracing::info!("Removed ontology '{}'", domain);
        Ok(())
    }

    /// 校验本体 (不注册,仅干跑)
    pub async fn validate(&self, pkg: &DomainPackage) -> Result<(), OntologyError> {
        self.loader
            .read()
            .await
            .validate(pkg)
            .map_err(OntologyError::Validation)
    }

    /// 统计信息 (域数量, 标签总数, 边类型总数)
    pub async fn stats(&self) -> (usize, usize, usize) {
        let loader = self.loader.read().await;
        (
            loader.list().len(),
            loader.total_labels(),
            loader.total_edge_types(),
        )
    }
}

impl Default for OntologyManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control_plane_store::InMemoryControlPlaneStore;
    use crate::domain_package::DomainSchema;

    fn sample_package(domain: &str) -> DomainPackage {
        DomainPackage {
            schema: DomainSchema {
                domain: domain.to_string(),
                version: "1.0".to_string(),
                description: None,
                extends: None,
                labels: vec![],
                edge_types: vec![],
                constraints: vec![],
                indexes: vec![],
            },
            mappings: vec![],
            standing_queries: vec![],
            materialized_views: vec![],
        }
    }

    #[tokio::test]
    async fn test_create_and_get() {
        let mgr = OntologyManager::new();
        let domain = mgr.create(sample_package("iot")).await.unwrap();
        assert_eq!(domain, "iot");

        let pkg = mgr.get("iot").await.unwrap();
        assert_eq!(pkg.schema.domain, "iot");
        assert_eq!(pkg.schema.version, "1.0");
    }

    #[tokio::test]
    async fn test_list_and_remove() {
        let mgr = OntologyManager::new();
        mgr.create(sample_package("a")).await.unwrap();
        mgr.create(sample_package("b")).await.unwrap();

        let mut domains = mgr.list().await;
        domains.sort();
        assert_eq!(domains, vec!["a", "b"]);

        mgr.remove("a").await.unwrap();
        assert_eq!(mgr.list().await, vec!["b"]);

        // 删除不存在的域返回错误
        assert!(mgr.remove("nonexistent").await.is_err());
    }

    #[tokio::test]
    async fn test_validation_empty_domain() {
        let mgr = OntologyManager::new();
        let mut pkg = sample_package("");
        pkg.schema.domain = "".to_string();

        let result = mgr.create(pkg).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_persistence_and_restore() {
        let store = Arc::new(InMemoryControlPlaneStore::new());

        // 1. 创建 manager 并添加本体
        {
            let mgr = OntologyManager::with_store(store.clone()).await.unwrap();
            mgr.create(sample_package("persistent")).await.unwrap();
        }

        // 2. 新 manager 从同一 store 恢复
        {
            let mgr = OntologyManager::with_store(store.clone()).await.unwrap();
            let domains = mgr.list().await;
            assert_eq!(domains, vec!["persistent"]);

            let pkg = mgr.get("persistent").await.unwrap();
            assert_eq!(pkg.schema.domain, "persistent");
        }
    }

    #[tokio::test]
    async fn test_create_from_yaml() {
        let mgr = OntologyManager::new();
        let yaml = r#"
schema:
  domain: yaml_domain
  version: "2.0"
  labels: []
  edge_types: []
  constraints: []
  indexes: []
mappings: []
standing_queries: []
materialized_views: []
"#;
        let domain = mgr.create_from_yaml(yaml).await.unwrap();
        assert_eq!(domain, "yaml_domain");
        assert_eq!(mgr.get("yaml_domain").await.unwrap().schema.version, "2.0");
    }
}
