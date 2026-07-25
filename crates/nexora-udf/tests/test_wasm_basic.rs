//! Basic Wasm UDF tests — registration, execution, listing, unregistration.
//!
//! Only runs when the "wasm" feature is enabled.

#![cfg(feature = "wasm")]

use nexora_udf::wasm_runtime::WasmUdfRuntime;
use serde_json::json;

/// WAT module that implements an identity function (returns input unchanged).
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

fn compile_identity() -> Vec<u8> {
    wat::parse_str(IDENTITY_WAT).expect("failed to parse WAT")
}

#[test]
fn test_wasm_register_and_execute_identity() {
    let mut runtime = WasmUdfRuntime::new().unwrap();
    let wasm = compile_identity();
    runtime.register("identity", &wasm).unwrap();

    let input = json!({"a": 1, "b": "hello", "c": [1, 2, 3]});
    let result = runtime.execute("identity", &input).unwrap();
    assert_eq!(result, input);
}

#[test]
fn test_wasm_execute_with_numbers() {
    let mut runtime = WasmUdfRuntime::new().unwrap();
    let wasm = compile_identity();
    runtime.register("echo_num", &wasm).unwrap();

    let input = json!({"value": 42, "pi": 3.14159});
    let result = runtime.execute("echo_num", &input).unwrap();
    assert_eq!(result, input);
}

#[test]
fn test_wasm_list() {
    let mut runtime = WasmUdfRuntime::new().unwrap();
    let wasm = compile_identity();

    runtime.register("func1", &wasm).unwrap();
    runtime.register("func2", &wasm).unwrap();
    runtime.register("func3", &wasm).unwrap();

    let mut names = runtime.list();
    names.sort();
    assert_eq!(names, vec!["func1", "func2", "func3"]);
}

#[test]
fn test_wasm_unregister() {
    let mut runtime = WasmUdfRuntime::new().unwrap();
    let wasm = compile_identity();
    runtime.register("temp", &wasm).unwrap();
    assert_eq!(runtime.list().len(), 1);

    runtime.unregister("temp").unwrap();
    assert!(runtime.list().is_empty());

    // Unregistering again should fail
    let result = runtime.unregister("temp");
    assert!(result.is_err());
}

#[test]
fn test_wasm_execute_nonexistent() {
    let runtime = WasmUdfRuntime::new().unwrap();
    let result = runtime.execute("nonexistent", &json!({}));
    assert!(result.is_err());
}

#[test]
fn test_wasm_register_overwrites() {
    let mut runtime = WasmUdfRuntime::new().unwrap();
    let wasm = compile_identity();

    runtime.register("func", &wasm).unwrap();
    // Register again with the same name — should overwrite
    runtime.register("func", &wasm).unwrap();

    assert_eq!(runtime.list().len(), 1);
    let result = runtime.execute("func", &json!({"x": 1})).unwrap();
    assert_eq!(result, json!({"x": 1}));
}

#[test]
fn test_wasm_large_input() {
    let mut runtime = WasmUdfRuntime::new().unwrap();
    let wasm = compile_identity();
    runtime.register("big", &wasm).unwrap();

    // Input with a large array
    let large_array: Vec<i32> = (0..1000).collect();
    let input = json!({"data": large_array});
    let result = runtime.execute("big", &input).unwrap();
    assert_eq!(result, input);
}

#[test]
fn test_wasm_empty_object() {
    let mut runtime = WasmUdfRuntime::new().unwrap();
    let wasm = compile_identity();
    runtime.register("empty", &wasm).unwrap();

    let input = json!({});
    let result = runtime.execute("empty", &input).unwrap();
    assert_eq!(result, input);
}
