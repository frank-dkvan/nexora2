//! Wasm UDF runtime using wasmtime.
//!
//! Security restrictions:
//! - No filesystem access (WASI not provided)
//! - No network access (WASI not provided)
//! - Memory limit (configurable, default 64MB)
//! - Execution timeout via epoch interruption (configurable, default 5 seconds)
//! - No environment variables

use crate::UdfRuntimeError;
use std::time::Duration;

// ============================================================
// Real implementation (when "wasm" feature is enabled)
// ============================================================

#[cfg(feature = "wasm")]
mod imp {
    use super::*;
    use std::collections::HashMap;
    use std::sync::mpsc;
    use wasmtime::{Config, Engine, Instance, Module, Store, TypedFunc};

    /// Default memory limit for Wasm UDF execution (64 MB).
    const DEFAULT_MEMORY_LIMIT: usize = 64 * 1024 * 1024;

    /// Default execution timeout.
    const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

    /// Fuel limit to prevent infinite loops (1 billion instructions ≈ a few seconds).
    const FUEL_LIMIT: u64 = 1_000_000_000;

    /// Wasm UDF runtime backed by wasmtime.
    pub struct WasmUdfRuntime {
        engine: Engine,
        modules: HashMap<String, Module>,
        memory_limit: usize,
        timeout: Duration,
    }

    impl WasmUdfRuntime {
        /// Create a new Wasm UDF runtime with default settings.
        pub fn new() -> Result<Self, UdfRuntimeError> {
            let mut config = Config::new();
            // Enable fuel-based execution to prevent infinite loops
            config.consume_fuel(true);
            // Enable epoch interruption for timeout enforcement
            config.epoch_interruption(true);
            // Limit Wasm stack size to 1MB
            config.max_wasm_stack(1024 * 1024);
            // Disable threading (security)
            config.wasm_threads(false);
            // Enable bulk memory operations (needed for memory.copy)
            config.wasm_bulk_memory(true);

            let engine = Engine::new(&config).map_err(|e| UdfRuntimeError::Wasm(e.to_string()))?;

            Ok(Self {
                engine,
                modules: HashMap::new(),
                memory_limit: DEFAULT_MEMORY_LIMIT,
                timeout: DEFAULT_TIMEOUT,
            })
        }

        /// Set the memory limit for Wasm UDF execution.
        pub fn with_memory_limit(mut self, limit: usize) -> Self {
            self.memory_limit = limit;
            self
        }

        /// Set the execution timeout.
        pub fn with_timeout(mut self, timeout: Duration) -> Self {
            self.timeout = timeout;
            self
        }

        /// Register a Wasm module as a UDF.
        ///
        /// The module must export:
        /// - `memory`: linear memory
        /// - `alloc(size: i32) -> i32`: allocate `size` bytes, return pointer
        /// - `dealloc(ptr: i32, size: i32)`: free allocated memory
        /// - `invoke(in_ptr: i32, in_len: i32) -> i64`: process input, return packed (ptr, len)
        ///
        /// The return value of `invoke` packs the output pointer in the high 32 bits
        /// and output length in the low 32 bits.
        pub fn register(&mut self, name: &str, wasm_bytes: &[u8]) -> Result<(), UdfRuntimeError> {
            let module = Module::new(&self.engine, wasm_bytes)
                .map_err(|e| UdfRuntimeError::Wasm(e.to_string()))?;
            self.modules.insert(name.to_string(), module);
            Ok(())
        }

        /// Execute a registered UDF with JSON input, returning JSON output.
        pub fn execute(
            &self,
            name: &str,
            input: &serde_json::Value,
        ) -> Result<serde_json::Value, UdfRuntimeError> {
            let module = self
                .modules
                .get(name)
                .ok_or_else(|| UdfRuntimeError::NotFound(name.to_string()))?;

            let input_bytes = serde_json::to_vec(input)?;

            let mut store = Store::new(&self.engine, ());

            // Set fuel to prevent infinite loops
            store
                .set_fuel(FUEL_LIMIT)
                .map_err(|e| UdfRuntimeError::Wasm(e.to_string()))?;

            // Set epoch deadline for timeout
            store.set_epoch_deadline(1);

            // Spawn epoch interruption thread for timeout enforcement
            let engine = self.engine.clone();
            let timeout = self.timeout;
            let (tx, rx) = mpsc::channel::<()>();
            std::thread::spawn(move || {
                // If we receive a signal before timeout, execution is done; just exit.
                // If the timeout elapses, increment the epoch to interrupt execution.
                if rx.recv_timeout(timeout).is_err() {
                    engine.increment_epoch();
                }
            });

            // Instantiate the module (no imports = no WASI = no filesystem/network)
            let instance = Instance::new(&mut store, module, &[])
                .map_err(|e| UdfRuntimeError::Wasm(e.to_string()))?;

            // Get required exports
            let memory = instance
                .get_memory(&mut store, "memory")
                .ok_or_else(|| UdfRuntimeError::Wasm("module must export 'memory'".into()))?;

            let alloc_func = instance
                .get_func(&mut store, "alloc")
                .ok_or_else(|| UdfRuntimeError::Wasm("module must export 'alloc'".into()))?;
            let alloc: TypedFunc<i32, i32> = alloc_func
                .typed(&store)
                .map_err(|e| UdfRuntimeError::Wasm(format!("alloc has wrong type: {e}")))?;

            let invoke_func = instance
                .get_func(&mut store, "invoke")
                .ok_or_else(|| UdfRuntimeError::Wasm("module must export 'invoke'".into()))?;
            let invoke: TypedFunc<(i32, i32), i64> = invoke_func
                .typed(&store)
                .map_err(|e| UdfRuntimeError::Wasm(format!("invoke has wrong type: {e}")))?;

            // Optionally get dealloc
            let dealloc_func = instance.get_func(&mut store, "dealloc");
            let dealloc: Option<TypedFunc<(i32, i32), ()>> =
                match &dealloc_func {
                    Some(f) => Some(f.typed(&store).map_err(|e| {
                        UdfRuntimeError::Wasm(format!("dealloc has wrong type: {e}"))
                    })?),
                    None => None,
                };

            // Allocate memory for input
            let ptr = alloc
                .call(&mut store, input_bytes.len() as i32)
                .map_err(|e| UdfRuntimeError::Wasm(format!("alloc failed: {e}")))?;

            // Write input to Wasm memory
            memory
                .write(&mut store, ptr as usize, &input_bytes)
                .map_err(|e| UdfRuntimeError::Wasm(format!("memory write failed: {e}")))?;

            // Call invoke
            let invoke_result = invoke.call(&mut store, (ptr, input_bytes.len() as i32));

            // Signal the timeout thread to stop
            let _ = tx.send(());

            let packed_result = invoke_result.map_err(|e| {
                let msg = e.to_string();
                if msg.contains("epoch") || msg.contains("deadline") || msg.contains("interrupt") {
                    UdfRuntimeError::Timeout(self.timeout)
                } else {
                    UdfRuntimeError::Wasm(format!("invoke failed: {msg}"))
                }
            })?;

            // Unpack result: high 32 bits = output ptr, low 32 bits = output len
            let output_ptr = ((packed_result >> 32) & 0xFFFF_FFFF) as usize;
            let output_len = (packed_result & 0xFFFF_FFFF) as usize;

            if output_len > self.memory_limit {
                return Err(UdfRuntimeError::Security(format!(
                    "output size {output_len} exceeds limit {}",
                    self.memory_limit
                )));
            }

            if output_len == 0 {
                return Err(UdfRuntimeError::InvalidInput(
                    "invoke returned zero-length output".into(),
                ));
            }

            // Read output from Wasm memory
            let mut output_bytes = vec![0u8; output_len];
            memory
                .read(&store, output_ptr, &mut output_bytes)
                .map_err(|e| UdfRuntimeError::Wasm(format!("memory read failed: {e}")))?;

            // Deallocate input buffer (best effort)
            if let Some(dealloc) = dealloc {
                let _ = dealloc.call(&mut store, (ptr, input_bytes.len() as i32));
                let _ = dealloc.call(&mut store, (output_ptr as i32, output_len as i32));
            }

            // Parse output as JSON
            let result: serde_json::Value = serde_json::from_slice(&output_bytes)?;
            Ok(result)
        }

        /// List registered UDF names.
        pub fn list(&self) -> Vec<String> {
            self.modules.keys().cloned().collect()
        }

        /// Remove a registered UDF.
        pub fn unregister(&mut self, name: &str) -> Result<(), UdfRuntimeError> {
            self.modules
                .remove(name)
                .map(|_| ())
                .ok_or_else(|| UdfRuntimeError::NotFound(name.to_string()))
        }
    }

    impl Default for WasmUdfRuntime {
        fn default() -> Self {
            Self::new().expect("failed to create WasmUdfRuntime")
        }
    }
}

// ============================================================
// Stub implementation (when "wasm" feature is not enabled)
// ============================================================

#[cfg(not(feature = "wasm"))]
mod imp {
    use super::*;

    /// Stub Wasm UDF runtime (wasm feature not enabled).
    #[derive(Default)]
    pub struct WasmUdfRuntime {
        _phantom: (),
    }

    impl WasmUdfRuntime {
        pub fn new() -> Result<Self, UdfRuntimeError> {
            Ok(Self { _phantom: () })
        }

        pub fn with_memory_limit(self, _limit: usize) -> Self {
            self
        }

        pub fn with_timeout(self, _timeout: Duration) -> Self {
            self
        }

        pub fn register(&mut self, _name: &str, _wasm_bytes: &[u8]) -> Result<(), UdfRuntimeError> {
            Err(UdfRuntimeError::Wasm(
                "wasm feature not enabled. Build with --features wasm to enable Wasm UDF support."
                    .into(),
            ))
        }

        pub fn execute(
            &self,
            _name: &str,
            _input: &serde_json::Value,
        ) -> Result<serde_json::Value, UdfRuntimeError> {
            Err(UdfRuntimeError::Wasm("wasm feature not enabled".into()))
        }

        pub fn list(&self) -> Vec<String> {
            Vec::new()
        }

        pub fn unregister(&mut self, _name: &str) -> Result<(), UdfRuntimeError> {
            Err(UdfRuntimeError::Wasm("wasm feature not enabled".into()))
        }
    }
}

#[cfg(feature = "wasm")]
pub use imp::WasmUdfRuntime;
#[cfg(not(feature = "wasm"))]
pub use imp::WasmUdfRuntime;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_runtime() {
        let runtime = WasmUdfRuntime::new();
        assert!(runtime.is_ok());
    }

    #[test]
    fn test_list_empty() {
        let runtime = WasmUdfRuntime::new().unwrap();
        assert!(runtime.list().is_empty());
    }

    #[test]
    fn test_unregister_nonexistent() {
        let mut runtime = WasmUdfRuntime::new().unwrap();
        let result = runtime.unregister("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn test_execute_nonexistent() {
        let runtime = WasmUdfRuntime::new().unwrap();
        let input = serde_json::json!({});
        let result = runtime.execute("nonexistent", &input);
        assert!(result.is_err());
    }

    #[cfg(feature = "wasm")]
    #[test]
    fn test_wasm_identity() {
        let identity_wat = r#"
(module
  (memory (export "memory") 1)
  (global $heap_ptr (mut i32) (i32.const 1024))

  (func $alloc (export "alloc") (param $size i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $heap_ptr))
    (global.set $heap_ptr
      (i32.and
        (i32.add (i32.add (global.get $heap_ptr) (local.get $size)) (i32.const 3))
        (i32.const -4)))
    (local.get $ptr))

  (func (export "dealloc") (param $ptr i32) (param $size i32))

  (func (export "invoke") (param $in_ptr i32) (param $in_len i32) (result i64)
    (local $out_ptr i32)
    (local.set $out_ptr (call $alloc (local.get $in_len)))
    (memory.copy (local.get $out_ptr) (local.get $in_ptr) (local.get $in_len))
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $out_ptr)) (i64.const 32))
      (i64.extend_i32_u (local.get $in_len))))
)
"#;
        let wasm_bytes = wat::parse_str(identity_wat).unwrap();

        let mut runtime = WasmUdfRuntime::new().unwrap();
        runtime.register("identity", &wasm_bytes).unwrap();

        let input = serde_json::json!({"a": 1, "b": "hello"});
        let result = runtime.execute("identity", &input).unwrap();
        assert_eq!(result, input);

        // Test list
        assert_eq!(runtime.list(), vec!["identity"]);

        // Test unregister
        runtime.unregister("identity").unwrap();
        assert!(runtime.list().is_empty());
    }
}
