//! Basic Python UDF tests — registration, execution, listing, unregistration.

use nexora_udf::python_runtime::PythonUdfRuntime;
use nexora_udf::UdfRuntimeError;
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
fn test_python_register_and_execute_add() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "add",
            r#"
def invoke(input):
    return {"result": input["a"] + input["b"]}
"#,
        )
        .unwrap();

    let input = json!({"a": 3, "b": 4});
    let result = runtime.execute("add", &input).unwrap();
    assert_eq!(result, json!({"result": 7}));
}

#[test]
fn test_python_string_manipulation() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "reverse",
            r#"
def invoke(input):
    text = input.get("text", "")
    return {"result": text[::-1]}
"#,
        )
        .unwrap();

    let input = json!({"text": "hello"});
    let result = runtime.execute("reverse", &input).unwrap();
    assert_eq!(result, json!({"result": "olleh"}));
}

#[test]
fn test_python_uppercase() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "upper",
            r#"
def invoke(input):
    return {"result": input["text"].upper()}
"#,
        )
        .unwrap();

    let result = runtime
        .execute("upper", &json!({"text": "hello world"}))
        .unwrap();
    assert_eq!(result, json!({"result": "HELLO WORLD"}));
}

#[test]
fn test_python_math_operations() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "math_ops",
            r#"
def invoke(input):
    a = input["a"]
    b = input["b"]
    return {
        "sum": a + b,
        "diff": a - b,
        "product": a * b,
        "quotient": a // b if b != 0 else None
    }
"#,
        )
        .unwrap();

    let result = runtime
        .execute("math_ops", &json!({"a": 10, "b": 3}))
        .unwrap();
    assert_eq!(result["sum"], json!(13));
    assert_eq!(result["diff"], json!(7));
    assert_eq!(result["product"], json!(30));
    assert_eq!(result["quotient"], json!(3));
}

#[test]
fn test_python_list_processing() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "process_list",
            r#"
def invoke(input):
    data = input.get("data", [])
    return {
        "count": len(data),
        "sum": sum(data),
        "max": max(data) if data else None,
        "min": min(data) if data else None
    }
"#,
        )
        .unwrap();

    let result = runtime
        .execute("process_list", &json!({"data": [3, 1, 4, 1, 5, 9, 2, 6]}))
        .unwrap();
    assert_eq!(result["count"], json!(8));
    assert_eq!(result["sum"], json!(31));
    assert_eq!(result["max"], json!(9));
    assert_eq!(result["min"], json!(1));
}

#[test]
fn test_python_passthrough() {
    let mut runtime = make_runtime();
    runtime
        .register("passthrough", "def invoke(input):\n    return input\n")
        .unwrap();

    let input = json!({"x": 1, "y": "hello", "z": [true, false]});
    let result = runtime.execute("passthrough", &input).unwrap();
    assert_eq!(result, input);
}

#[test]
fn test_python_list_and_unregister() {
    let mut runtime = make_runtime();
    runtime
        .register("func1", "def invoke(input):\n    return input\n")
        .unwrap();
    runtime
        .register("func2", "def invoke(input):\n    return input\n")
        .unwrap();

    let mut names = runtime.list();
    names.sort();
    assert_eq!(names, vec!["func1", "func2"]);

    runtime.unregister("func1").unwrap();
    assert_eq!(runtime.list(), vec!["func2"]);

    // Unregistering again should fail
    let result = runtime.unregister("func1");
    assert!(result.is_err());
}

#[test]
fn test_python_register_invalid_code() {
    let mut runtime = make_runtime();
    let result = runtime.register("bad", "print('hello')");
    assert!(result.is_err());
    assert!(matches!(result, Err(UdfRuntimeError::InvalidInput(_))));
}

#[test]
fn test_python_execute_nonexistent() {
    let runtime = make_runtime();
    let result = runtime.execute("nonexistent", &json!({}));
    assert!(result.is_err());
    assert!(matches!(result, Err(UdfRuntimeError::NotFound(_))));
}

#[test]
fn test_python_runtime_error_handling() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "error_func",
            r#"
def invoke(input):
    return input["nonexistent_key"]["nested"]
"#,
        )
        .unwrap();

    let result = runtime.execute("error_func", &json!({"a": 1}));
    assert!(result.is_err());
    // Should be a Python error, not a timeout or security error
    assert!(matches!(result, Err(UdfRuntimeError::Python(_))));
}

#[test]
fn test_python_nested_data() {
    let mut runtime = make_runtime();
    runtime
        .register(
            "flatten",
            r#"
def invoke(input):
    nested = input.get("nested", {})
    result = {}
    for key, value in nested.items():
        result[f"flat_{key}"] = value
    return result
"#,
        )
        .unwrap();

    let input = json!({"nested": {"a": 1, "b": 2, "c": 3}});
    let result = runtime.execute("flatten", &input).unwrap();
    assert_eq!(result, json!({"flat_a": 1, "flat_b": 2, "flat_c": 3}));
}
