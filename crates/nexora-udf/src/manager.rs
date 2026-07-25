//! Unified UDF manager — manages both Wasm and Python UDFs.

use crate::python_runtime::PythonUdfRuntime;
use crate::wasm_runtime::WasmUdfRuntime;
use crate::UdfRuntimeError;

/// The type of a registered UDF.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UdfType {
    Wasm,
    Python,
}

/// A unified manager that can handle both Wasm and Python UDFs.
pub struct UdfManager {
    wasm_runtime: WasmUdfRuntime,
    python_runtime: PythonUdfRuntime,
}

impl UdfManager {
    /// Create a new UdfManager with default settings.
    ///
    /// - Wasm runtime: created with default config (64MB memory, 5s timeout)
    /// - Python runtime: uses "python3" from PATH
    pub fn new() -> Self {
        let wasm_runtime = WasmUdfRuntime::new().unwrap_or_else(|e| {
            tracing::warn!("Failed to create Wasm UDF runtime: {e}");
            WasmUdfRuntime::default()
        });

        let python_runtime = PythonUdfRuntime::new("python3").unwrap_or_else(|e| {
            tracing::warn!("Failed to create Python UDF runtime: {e}");
            PythonUdfRuntime::stub()
        });

        Self {
            wasm_runtime,
            python_runtime,
        }
    }

    /// Register a Wasm UDF.
    pub fn register_wasm(&mut self, name: &str, wasm_bytes: &[u8]) -> Result<(), UdfRuntimeError> {
        self.wasm_runtime.register(name, wasm_bytes)
    }

    /// Register a Python UDF.
    pub fn register_python(&mut self, name: &str, code: &str) -> Result<(), UdfRuntimeError> {
        self.python_runtime.register(name, code)
    }

    /// Execute a registered UDF by name.
    ///
    /// Tries Wasm first, then Python.
    pub fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, UdfRuntimeError> {
        // Try Wasm first
        if self.wasm_runtime.list().contains(&name.to_string()) {
            return self.wasm_runtime.execute(name, input);
        }

        // Try Python
        if self.python_runtime.list().contains(&name.to_string()) {
            return self.python_runtime.execute(name, input);
        }

        Err(UdfRuntimeError::NotFound(name.to_string()))
    }

    /// List all registered UDFs with their types.
    pub fn list(&self) -> Vec<(String, UdfType)> {
        let mut result: Vec<(String, UdfType)> = self
            .wasm_runtime
            .list()
            .into_iter()
            .map(|name| (name, UdfType::Wasm))
            .collect();

        result.extend(
            self.python_runtime
                .list()
                .into_iter()
                .map(|name| (name, UdfType::Python)),
        );

        result
    }

    /// Remove a registered UDF.
    ///
    /// Tries Wasm first, then Python.
    pub fn unregister(&mut self, name: &str) -> Result<(), UdfRuntimeError> {
        // Try Wasm first
        if self.wasm_runtime.list().contains(&name.to_string()) {
            return self.wasm_runtime.unregister(name);
        }

        // Try Python
        if self.python_runtime.list().contains(&name.to_string()) {
            return self.python_runtime.unregister(name);
        }

        Err(UdfRuntimeError::NotFound(name.to_string()))
    }

    /// Get the type of a registered UDF.
    pub fn get_type(&self, name: &str) -> Option<UdfType> {
        if self.wasm_runtime.list().contains(&name.to_string()) {
            Some(UdfType::Wasm)
        } else if self.python_runtime.list().contains(&name.to_string()) {
            Some(UdfType::Python)
        } else {
            None
        }
    }
}

impl Default for UdfManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_manager() {
        let manager = UdfManager::new();
        assert!(manager.list().is_empty());
    }

    #[test]
    fn test_unregister_nonexistent() {
        let mut manager = UdfManager::new();
        let result = manager.unregister("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_execute_nonexistent() {
        let manager = UdfManager::new();
        let result = manager.execute("nonexistent", &serde_json::json!({}));
        assert!(result.is_err());
    }
}
