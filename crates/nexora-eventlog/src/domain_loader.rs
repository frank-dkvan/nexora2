//! DomainLoader: 从配置文件/目录加载 DomainPackage 定义
//!
//! 支持两种加载方式:
//! 1. 从目录扫描 TOML 文件
//! 2. 从单个 TOML 文件加载多个 domain

use crate::DomainPackage;
use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

/// DomainLoader: 加载 DomainPackage 定义
pub struct DomainLoader;

impl DomainLoader {
    /// 从目录加载所有 .toml 文件中的 DomainPackage
    ///
    /// 扫描指定目录,解析每个 .toml 文件为 DomainPackage
    pub fn load_from_dir(dir: &Path) -> Result<Vec<DomainPackage>> {
        if !dir.exists() {
            tracing::warn!("Domain directory does not exist: {}", dir.display());
            return Ok(vec![]);
        }

        let mut packages = Vec::new();

        for entry in fs::read_dir(dir).context("Failed to read domain directory")? {
            let entry = entry.context("Failed to read directory entry")?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("toml") {
                match Self::load_from_file(&path) {
                    Ok(pkg) => {
                        tracing::info!(
                            "Loaded domain package from {} ({} mappings)",
                            path.display(),
                            pkg.mappings.len()
                        );
                        packages.push(pkg);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to load {}: {}", path.display(), e);
                    }
                }
            }
        }

        Ok(packages)
    }

    /// 从单个 TOML 文件加载 DomainPackage
    ///
    /// TOML 格式:
    /// ```toml
    /// [schema]
    /// name = "iot_domain"
    /// # ... 其他 schema 字段
    ///
    /// [[mappings]]
    /// # EventMapping 定义
    /// # ...
    /// ```
    pub fn load_from_file(path: &Path) -> Result<DomainPackage> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read file: {}", path.display()))?;

        let pkg: DomainPackage = toml::from_str(&content)
            .with_context(|| format!("Failed to parse TOML: {}", path.display()))?;

        Ok(pkg)
    }

    /// 从 TOML 字符串加载 DomainPackage (用于测试)
    pub fn load_from_str(toml_str: &str) -> Result<DomainPackage> {
        let pkg: DomainPackage = toml::from_str(toml_str).context("Failed to parse TOML string")?;
        Ok(pkg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_from_str() {
        let toml = r#"
            [schema]
            domain = "test_domain"
            version = "1.0"
        "#;

        let pkg = DomainLoader::load_from_str(toml).unwrap();
        assert_eq!(pkg.schema.domain, "test_domain");
        assert_eq!(pkg.schema.version, "1.0");
    }
}
