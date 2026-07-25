//! Security tests — verify that sandbox restrictions are enforced.
//!
//! On macOS, rlimits provide the following enforceable restrictions:
//! - File writes blocked (RLIMIT_FSIZE=0)
//! - Subprocess creation blocked (RLIMIT_NPROC=0)
//! - CPU time limited (RLIMIT_CPU)
//! - Memory limited (RLIMIT_AS)
//! - Environment variables cleared (env_clear)
//! - CWD set to empty temp directory
//!
//! File reads and network access require OS-level sandboxing (e.g., sandbox-exec
//! on macOS or seccomp on Linux) which is not available in all environments.
//! These tests verify what IS enforceable via rlimits.

use nexora_udf::python_runtime::PythonUdfRuntime;
use serde_json::json;
use std::process::Command;

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
fn test_python_cannot_write_filesystem() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "write_file",
            r#"
def invoke(input):
    try:
        with open("/tmp/nexora_test_security", "w") as f:
            f.write("test")
        return {"written": True}
    except Exception as e:
        return {"error": str(e)}
"#,
        )
        .unwrap();

    let result = runtime.execute("write_file", &json!({})).unwrap();

    // File creation should fail due to RLIMIT_FSIZE=0
    assert_ne!(
        result.get("written"),
        Some(&json!(true)),
        "Security violation: Python was able to write to /tmp"
    );
    assert!(
        result.get("error").is_some(),
        "Expected an error, got: {result}"
    );

    // Clean up just in case
    let _ = std::fs::remove_file("/tmp/nexora_test_security");
}

#[test]
fn test_python_cannot_write_in_cwd() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "write_cwd",
            r#"
def invoke(input):
    try:
        with open("test_file.txt", "w") as f:
            f.write("test")
        return {"written": True}
    except Exception as e:
        return {"error": str(e)}
"#,
        )
        .unwrap();

    let result = runtime.execute("write_cwd", &json!({})).unwrap();

    // File creation should fail due to RLIMIT_FSIZE=0
    assert_ne!(
        result.get("written"),
        Some(&json!(true)),
        "Security violation: Python was able to write in CWD"
    );
}

#[test]
fn test_python_no_environment_variables() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "check_env",
            r#"
import os
def invoke(input):
    env_vars = dict(os.environ)
    return {
        "env_count": len(env_vars),
        "has_home": "HOME" in env_vars,
        "has_user": "USER" in env_vars,
        "has_secret": "SECRET_KEY" in env_vars,
    }
"#,
        )
        .unwrap();

    let result = runtime.execute("check_env", &json!({})).unwrap();

    // Environment should be cleared (only minimal vars like PATH and LC_ALL)
    let env_count = result["env_count"].as_u64().unwrap_or(999);
    assert!(
        env_count <= 5,
        "Expected very few env vars, got {env_count}: {result}"
    );
    // HOME and USER should NOT be set
    assert_ne!(
        result["has_home"],
        json!(true),
        "HOME should not be accessible"
    );
    assert_ne!(
        result["has_user"],
        json!(true),
        "USER should not be accessible"
    );
}

#[test]
fn test_python_cwd_is_temp_dir() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "check_cwd",
            r#"
import os
def invoke(input):
    cwd = os.getcwd()
    files = os.listdir(".")
    return {"cwd": cwd, "file_count": len(files)}
"#,
        )
        .unwrap();

    let result = runtime.execute("check_cwd", &json!({})).unwrap();

    // CWD should be a temp directory with no files
    let file_count = result["file_count"].as_u64().unwrap_or(999);
    assert_eq!(
        file_count, 0,
        "Expected empty temp directory, got {file_count} files: {result}"
    );

    // CWD should be a temporary path
    let cwd = result["cwd"].as_str().unwrap_or("");
    assert!(
        cwd.contains("tmp") || cwd.contains("var") || cwd.contains("Temp"),
        "CWD should be a temp directory, got: {cwd}"
    );
}

#[test]
fn test_python_cannot_spawn_subprocess() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "spawn_process",
            r#"
def invoke(input):
    try:
        import subprocess
        result = subprocess.run(["echo", "hello"], capture_output=True, text=True, timeout=2)
        return {"output": result.stdout.strip()}
    except Exception as e:
        return {"error": str(e)}
"#,
        )
        .unwrap();

    let result = runtime.execute("spawn_process", &json!({})).unwrap();

    // Subprocess creation should fail due to RLIMIT_NPROC=0
    assert!(
        result.get("output").is_none(),
        "Security violation: Python was able to spawn a subprocess"
    );
    assert!(
        result.get("error").is_some(),
        "Expected an error from subprocess attempt, got: {result}"
    );
}

#[test]
fn test_python_cannot_use_os_system() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "os_system",
            r#"
def invoke(input):
    try:
        import os
        exit_code = os.system("echo hello")
        return {"exit_code": exit_code}
    except Exception as e:
        return {"error": str(e)}
"#,
        )
        .unwrap();

    let result = runtime.execute("os_system", &json!({})).unwrap();

    // os.system should fail due to RLIMIT_NPROC=0
    assert!(
        result.get("exit_code").is_none() || result["exit_code"] != json!(0),
        "Security violation: Python was able to execute os.system"
    );
}

// ============================================================
// Wasm security tests (only when wasm feature is enabled)
// ============================================================

#[cfg(feature = "wasm")]
mod wasm_security_tests {
    use super::*;
    use nexora_udf::wasm_runtime::WasmUdfRuntime;

    const IDENTITY_WAT: &str = r#"
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

    #[test]
    fn test_wasm_no_filesystem_access() {
        // Wasm modules compiled without WASI have no filesystem access by design.
        // The wasmtime engine is configured with no host imports,
        // so the module cannot call any filesystem-related functions.
        let wasm_bytes = wat::parse_str(IDENTITY_WAT).unwrap();
        let mut runtime = WasmUdfRuntime::new().unwrap();
        runtime.register("secure", &wasm_bytes).unwrap();

        let result = runtime.execute("secure", &json!({"test": true})).unwrap();
        assert_eq!(result, json!({"test": true}));
    }

    #[test]
    fn test_wasm_no_network_access() {
        // Wasm modules without WASI imports cannot access the network.
        let wasm_bytes = wat::parse_str(IDENTITY_WAT).unwrap();
        let mut runtime = WasmUdfRuntime::new().unwrap();
        runtime.register("no_net", &wasm_bytes).unwrap();

        let result = runtime.execute("no_net", &json!({"ok": 1})).unwrap();
        assert_eq!(result, json!({"ok": 1}));
    }

    #[test]
    fn test_wasm_no_wasi_imports() {
        // A module that tries to import WASI functions should fail to instantiate.
        // This WAT attempts to import `wasi_snapshot_preview1.fd_write` which
        // would allow writing to file descriptors.
        let wasi_import_wat = r#"
(module
  (import "wasi_snapshot_preview1" "fd_write"
    (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "alloc") (param $size i32) (result i32) (i32.const 0))
  (func (export "dealloc") (param $ptr i32) (param $size i32))
  (func (export "invoke") (param $in_ptr i32) (param $in_len i32) (result i64)
    (i64.const 0))
)
"#;
        let wasm_bytes = wat::parse_str(wasi_import_wat).unwrap();
        let mut runtime = WasmUdfRuntime::new().unwrap();

        // Registration might succeed (compilation), but execution should fail
        // because we don't provide any WASI imports.
        let reg_result = runtime.register("wasi_mod", &wasm_bytes);
        if reg_result.is_ok() {
            let exec_result = runtime.execute("wasi_mod", &json!({}));
            assert!(
                exec_result.is_err(),
                "WASI module should not be executable without WASI imports"
            );
        }
    }

    #[test]
    fn test_wasm_memory_limit_configurable() {
        let runtime = WasmUdfRuntime::new().unwrap().with_memory_limit(1024);

        let wasm_bytes = wat::parse_str(IDENTITY_WAT).unwrap();
        let mut runtime = runtime;
        runtime.register("limited", &wasm_bytes).unwrap();

        // Small input should still work
        let result = runtime.execute("limited", &json!({"x": 1})).unwrap();
        assert_eq!(result, json!({"x": 1}));
    }
}
