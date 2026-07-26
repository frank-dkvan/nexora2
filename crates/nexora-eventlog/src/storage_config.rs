//! 存储配置 - 支持本地文件系统、S3、REST catalog 三种后端
//!
//! EventLogStore 的 Iceberg 表可以存储在:
//! - 本地文件系统 (默认,开发/单机, SQLite catalog)
//! - S3 兼容对象存储 (SQLite catalog + S3 数据文件, 单机 catalog)
//! - REST catalog (Lakekeeper 等, 多节点共享一套元数据 + S3 数据文件)
//!
//! ## 三种后端的多节点语义
//!
//! | 后端 | catalog | 数据文件 | 多节点共享 |
//! |------|---------|---------|-----------|
//! | `LocalFs` | 本地 SQLite | 本地 FS | ❌ 单机 |
//! | `S3` | 本地 SQLite | 共享 S3 | ⚠️ 各节点 catalog 独立(需 NFS 共享 catalog 文件) |
//! | `Rest` | 远程 REST 服务 | 共享 S3 | ✅ 天然共享,无需 NFS |
//!
//! 生产多节点推荐 `Rest` (见 docs/architecture/EVENT_STORE_S3_CONFIGURATION.md)。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 存储后端配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageConfig {
    /// 本地文件系统 (默认)
    LocalFs {
        /// 数据目录
        data_dir: String,
    },

    /// S3 兼容对象存储 (SQLite catalog 本地)
    S3 {
        /// S3 endpoint (e.g. "http://localhost:9000" for MinIO, or AWS URL)
        endpoint: String,
        /// Bucket 名称
        bucket: String,
        /// Region
        region: String,
        /// Access key
        access_key: String,
        /// Secret key
        secret_key: String,
        /// 路径前缀 (虚拟目录)
        prefix: Option<String>,
        /// 路径风格访问 (MinIO=true, AWS S3=false)
        path_style: bool,
        /// SQLite catalog 数据库路径 (本地,存储元数据)
        catalog_db_path: String,
    },

    /// REST catalog (Lakekeeper / Polaris / Nessie 等)
    ///
    /// catalog 元数据由远程 REST 服务统一管理,多节点天然共享同一套数据。
    /// 客户端仍直接读写 S3 数据文件(catalog 只管元数据 + 凭证下发),所以
    /// 依然需要 S3 连接信息供本地 FileIO 使用。
    Rest {
        /// REST catalog endpoint (e.g. "http://localhost:8181/catalog")
        uri: String,
        /// warehouse 名称/标识 (e.g. "nexora")
        warehouse: String,
        /// S3 endpoint (客户端读写数据文件用; 从本节点网络可达的地址)
        s3_endpoint: String,
        /// Region
        s3_region: String,
        /// Access key
        s3_access_key: String,
        /// Secret key
        s3_secret_key: String,
        /// 路径风格访问 (MinIO=true, AWS S3=false)
        s3_path_style: bool,
    },
}

impl StorageConfig {
    /// 创建本地文件系统配置
    pub fn local_fs(data_dir: impl Into<String>) -> Self {
        Self::LocalFs {
            data_dir: data_dir.into(),
        }
    }

    /// 创建 S3 配置
    #[allow(clippy::too_many_arguments)]
    pub fn s3(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        prefix: Option<String>,
        path_style: bool,
        catalog_db_path: impl Into<String>,
    ) -> Self {
        Self::S3 {
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            region: region.into(),
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            prefix,
            path_style,
            catalog_db_path: catalog_db_path.into(),
        }
    }

    /// 创建 REST catalog 配置 (Lakekeeper 等; 多节点共享)
    #[allow(clippy::too_many_arguments)]
    pub fn rest(
        uri: impl Into<String>,
        warehouse: impl Into<String>,
        s3_endpoint: impl Into<String>,
        s3_region: impl Into<String>,
        s3_access_key: impl Into<String>,
        s3_secret_key: impl Into<String>,
        s3_path_style: bool,
    ) -> Self {
        Self::Rest {
            uri: uri.into(),
            warehouse: warehouse.into(),
            s3_endpoint: s3_endpoint.into(),
            s3_region: s3_region.into(),
            s3_access_key: s3_access_key.into(),
            s3_secret_key: s3_secret_key.into(),
            s3_path_style,
        }
    }

    /// 生成 Iceberg catalog 的 warehouse 位置。
    ///
    /// REST 后端返回 warehouse 逻辑标识(非 URL); catalog 服务负责解析到实际存储。
    pub fn warehouse_location(&self) -> String {
        match self {
            Self::LocalFs { data_dir } => {
                format!("{}/eventlog_warehouse", data_dir)
            }
            Self::S3 { bucket, prefix, .. } => {
                let prefix = prefix.as_deref().unwrap_or("eventlog");
                format!("s3://{}/{}/warehouse", bucket, prefix)
            }
            Self::Rest { warehouse, .. } => warehouse.clone(),
        }
    }

    /// 生成 catalog URI。
    /// - LocalFs/S3: SQLite 文件 URI
    /// - Rest: REST endpoint URL
    pub fn catalog_uri(&self) -> String {
        match self {
            Self::LocalFs { data_dir } => {
                format!("sqlite://{}/eventlog_catalog.db?mode=rwc", data_dir)
            }
            Self::S3 {
                catalog_db_path, ..
            } => {
                format!("sqlite://{}?mode=rwc", catalog_db_path)
            }
            Self::Rest { uri, .. } => uri.clone(),
        }
    }

    /// 生成 Iceberg catalog properties (传给 CatalogBuilder.load)
    pub fn catalog_props(&self) -> HashMap<String, String> {
        let mut props = HashMap::new();
        props.insert("warehouse".to_string(), self.warehouse_location());

        // S3 配置键 (iceberg 0.9.1 标准常量)
        match self {
            Self::S3 {
                endpoint,
                region,
                access_key,
                secret_key,
                path_style,
                ..
            } => {
                props.insert("s3.endpoint".to_string(), endpoint.clone());
                props.insert("s3.region".to_string(), region.clone());
                props.insert("s3.access-key-id".to_string(), access_key.clone());
                props.insert("s3.secret-access-key".to_string(), secret_key.clone());
                props.insert("s3.path-style-access".to_string(), path_style.to_string());
            }
            Self::Rest {
                uri,
                s3_endpoint,
                s3_region,
                s3_access_key,
                s3_secret_key,
                s3_path_style,
                ..
            } => {
                // REST catalog 客户端: uri 是必需的连接属性;
                // S3 属性供客户端本地 FileIO 直接读写数据文件用。
                props.insert("uri".to_string(), uri.clone());
                props.insert("s3.endpoint".to_string(), s3_endpoint.clone());
                props.insert("s3.region".to_string(), s3_region.clone());
                props.insert("s3.access-key-id".to_string(), s3_access_key.clone());
                props.insert("s3.secret-access-key".to_string(), s3_secret_key.clone());
                props.insert(
                    "s3.path-style-access".to_string(),
                    s3_path_style.to_string(),
                );
            }
            Self::LocalFs { .. } => {}
        }

        props
    }

    /// 是否为 S3 后端
    pub fn is_s3(&self) -> bool {
        matches!(self, Self::S3 { .. })
    }

    /// 是否为 REST catalog 后端
    pub fn is_rest(&self) -> bool {
        matches!(self, Self::Rest { .. })
    }

    /// 客户端 FileIO 是否需要 S3 storage factory (S3 或 Rest 后端)
    pub fn needs_s3_storage(&self) -> bool {
        matches!(self, Self::S3 { .. } | Self::Rest { .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_fs_config() {
        let config = StorageConfig::local_fs("/data/nexora");

        assert!(!config.is_s3());
        assert_eq!(
            config.warehouse_location(),
            "/data/nexora/eventlog_warehouse"
        );
        assert!(config.catalog_uri().contains("sqlite://"));
        assert!(config.catalog_uri().contains("eventlog_catalog.db"));

        let props = config.catalog_props();
        assert!(props.contains_key("warehouse"));
        assert!(!props.contains_key("s3.endpoint"));
    }

    #[test]
    fn test_s3_config() {
        let config = StorageConfig::s3(
            "http://localhost:9000",
            "my-bucket",
            "us-east-1",
            "minioadmin",
            "minioadmin",
            Some("events".into()),
            true,
            "/data/catalog.db",
        );

        assert!(config.is_s3());
        assert_eq!(
            config.warehouse_location(),
            "s3://my-bucket/events/warehouse"
        );

        let props = config.catalog_props();
        assert_eq!(props.get("s3.endpoint").unwrap(), "http://localhost:9000");
        assert_eq!(props.get("s3.region").unwrap(), "us-east-1");
        assert_eq!(props.get("s3.access-key-id").unwrap(), "minioadmin");
        assert_eq!(props.get("s3.path-style-access").unwrap(), "true");
    }

    #[test]
    fn test_s3_default_prefix() {
        let config = StorageConfig::s3(
            "http://localhost:9000",
            "bucket",
            "us-east-1",
            "key",
            "secret",
            None, // 无前缀
            true,
            "/tmp/catalog.db",
        );

        // 默认前缀为 "eventlog"
        assert_eq!(
            config.warehouse_location(),
            "s3://bucket/eventlog/warehouse"
        );
    }

    #[test]
    fn test_rest_config() {
        let config = StorageConfig::rest(
            "http://localhost:8181/catalog",
            "nexora",
            "http://localhost:9000",
            "us-east-1",
            "minioadmin",
            "minioadmin",
            true,
        );

        assert!(config.is_rest());
        assert!(!config.is_s3());
        assert!(config.needs_s3_storage());
        // warehouse 是逻辑标识,不是 URL
        assert_eq!(config.warehouse_location(), "nexora");
        // catalog_uri 是 REST endpoint
        assert_eq!(config.catalog_uri(), "http://localhost:8181/catalog");

        let props = config.catalog_props();
        // REST 客户端需要 uri
        assert_eq!(props.get("uri").unwrap(), "http://localhost:8181/catalog");
        assert_eq!(props.get("warehouse").unwrap(), "nexora");
        // S3 属性供客户端本地 FileIO 读写数据文件
        assert_eq!(props.get("s3.endpoint").unwrap(), "http://localhost:9000");
        assert_eq!(props.get("s3.access-key-id").unwrap(), "minioadmin");
        assert_eq!(props.get("s3.path-style-access").unwrap(), "true");
    }

    #[test]
    fn test_backend_predicates() {
        assert!(!StorageConfig::local_fs("/x").needs_s3_storage());
        let s3 = StorageConfig::s3("e", "b", "r", "k", "s", None, true, "/c.db");
        assert!(s3.needs_s3_storage() && s3.is_s3() && !s3.is_rest());
        let rest = StorageConfig::rest("u", "w", "e", "r", "k", "s", true);
        assert!(rest.needs_s3_storage() && rest.is_rest() && !rest.is_s3());
    }
}
