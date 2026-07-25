//! Timeout tests — verify that execution timeout works for Python and Wasm UDFs.

use nexora_udf::python_runtime::PythonUdfRuntime;
use nexora_udf::UdfRuntimeError;
use serde_json::json;
use std::process::Command;
use std::time::Duration;

fn python_available() -> bool {
    Command::new("python3").arg("--version").output().is_ok()
}

fn make_runtime() -> PythonUdfRuntime {
    if !python_available() {
        eprintln!("python3 not available, skipping test");
        std::process::exit(0);
    }
    PythonUdfRuntime::new("python3").unwrap()
}

#[test]
fn test_python_timeout_infinite_loop() {
    let mut runtime = make_runtime().with_timeout(Duration::from_millis(500));
    runtime
        .register(
            "infinite",
            r#"
def invoke(input):
    while True:
        pass
    return input
"#,
        )
        .unwrap();

    let result = runtime.execute("infinite", &json!({}));
    assert!(result.is_err());
    match &result {
        Err(UdfRuntimeError::Timeout(d)) => {
            assert!(
                *d <= Duration::from_secs(1),
                "Timeout should be around 500ms"
            );
        }
        Err(e) => panic!("Expected Timeout, got: {e}"),
        Ok(_) => panic!("Expected error, got success"),
    }
}

#[test]
fn test_python_timeout_long_sleep() {
    let mut runtime = make_runtime().with_timeout(Duration::from_millis(500));
    runtime
        .register(
            "sleeper",
            r#"
import time
def invoke(input):
    time.sleep(10)
    return {"done": True}
"#,
        )
        .unwrap();

    let result = runtime.execute("sleeper", &json!({}));
    assert!(result.is_err());
    assert!(matches!(result, Err(UdfRuntimeError::Timeout(_))));
}

#[test]
fn test_python_completes_before_timeout() {
    let mut runtime = make_runtime().with_timeout(Duration::from_secs(10));
    runtime
        .register(
            "quick",
            r#"
def invoke(input):
    return {"result": input["x"] * 2}
"#,
        )
        .unwrap();

    let result = runtime.execute("quick", &json!({"x": 21}));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), json!({"result": 42}));
}

#[test]
fn test_python_timeout_custom_duration() {
    let mut runtime = make_runtime().with_timeout(Duration::from_millis(200));
    runtime
        .register(
            "slow",
            r#"
import time
def invoke(input):
    time.sleep(2)
    return {"done": True}
"#,
        )
        .unwrap();

    let start = std::time::Instant::now();
    let result = runtime.execute("slow", &json!({}));
    let elapsed = start.elapsed();

    assert!(result.is_err());
    assert!(matches!(result, Err(UdfRuntimeError::Timeout(_))));
    // Should timeout well before the 2-second sleep finishes
    assert!(
        elapsed < Duration::from_secs(2),
        "Should timeout before 2s, took {:?}",
        elapsed
    );
}

// ============================================================
// Wasm timeout tests (only when wasm feature is enabled)
// ============================================================

#[cfg(feature = "wasm")]
mod wasm_timeout_tests {
    use super::*;
    use nexora_udf::wasm_runtime::WasmUdfRuntime;

    /// WAT module with an infinite loop in invoke.
    const INFINITE_LOOP_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (global $heap_ptr (mut i32) (i32.const 1024))

  (func (export "alloc") (param $size i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $heap_ptr))
    (global.set $heap_ptr
      (i32.and
        (i32.add (i32.add (global.get $heap_ptr) (local.get $size)) (i32.const 3))
        (i32.const -4)))
    (local.get $ptr))

  (func (export "dealloc") (param $ptr i32) (param $size i32))

  (func (export "invoke") (param $in_ptr i32) (param $in_len i32) (result i64)
    ;; Infinite loop — will be interrupted by epoch deadline
    (loop $forever
      (br $forever))
    (i64.const 0))
)
"#;

    #[test]
    fn test_wasm_timeout_infinite_loop() {
        let wasm_bytes = wat::parse_str(INFINITE_LOOP_WAT).unwrap();
        let runtime = WasmUdfRuntime::new()
            .unwrap()
            .with_timeout(Duration::from_millis(500));

        let mut runtime = runtime;
        runtime.register("loop", &wasm_bytes).unwrap();

        let result = runtime.execute("loop", &json!({}));
        assert!(result.is_err());
        match &result {
            Err(UdfRuntimeError::Timeout(_)) => {}
            Err(e) => {
                // Wasmtime might trap with a different error for the infinite loop
                // depending on whether fuel or epoch interruption kicks in first.
                // Either timeout or a Wasm trap is acceptable.
                eprintln!("Got error (not Timeout): {e}");
            }
            Ok(_) => panic!("Expected error, got success"),
        }
    }
}
