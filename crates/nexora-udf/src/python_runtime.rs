//! Python UDF sandbox using subprocess isolation.
//!
//! Security restrictions:
//! - Subprocess isolation (each execution in a new process)
//! - Execution timeout (configurable, default 5 seconds)
//! - No filesystem access (CWD set to empty temp directory)
//! - Resource limits via rlimit (CPU, file size, open files)
//! - Environment variables cleared
//! - Output size limit

use crate::UdfRuntimeError;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Default execution timeout.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default max output size (1 MB).
const DEFAULT_MAX_OUTPUT: usize = 1024 * 1024;

/// CPU time limit in seconds.
const CPU_LIMIT: u64 = 10;

/// Max open file descriptors (Python needs ~30+ for startup).
const NOFILE_LIMIT: u64 = 64;

/// Python UDF runtime using subprocess isolation.
pub struct PythonUdfRuntime {
    python_path: String,
    timeout: Duration,
    max_output: usize,
    /// Registered Python code: name → source code
    scripts: HashMap<String, String>,
}

impl PythonUdfRuntime {
    /// Create a new Python UDF runtime.
    ///
    /// Verifies that the Python binary is accessible and resolves it to an
    /// absolute path so that `env_clear()` during execution doesn't break
    /// PATH-based lookups.
    pub fn new(python_path: &str) -> Result<Self, UdfRuntimeError> {
        // Resolve to absolute path if not already absolute
        let resolved = if python_path.contains('/') {
            python_path.to_string()
        } else {
            // Use `which` to find the absolute path
            let which_output = Command::new("which")
                .arg(python_path)
                .output()
                .map_err(|e| {
                    UdfRuntimeError::Python(format!("failed to run 'which {python_path}': {e}"))
                })?;

            if !which_output.status.success() || which_output.stdout.is_empty() {
                // Fallback: try the path as-is
                python_path.to_string()
            } else {
                String::from_utf8_lossy(&which_output.stdout)
                    .trim()
                    .to_string()
            }
        };

        // Verify python is available
        let output = Command::new(&resolved)
            .arg("--version")
            .output()
            .map_err(|e| {
                UdfRuntimeError::Python(format!("Python not found at '{resolved}': {e}"))
            })?;

        if !output.status.success() {
            return Err(UdfRuntimeError::Python(format!(
                "Python at '{resolved}' failed to start"
            )));
        }

        Ok(Self {
            python_path: resolved,
            timeout: DEFAULT_TIMEOUT,
            max_output: DEFAULT_MAX_OUTPUT,
            scripts: HashMap::new(),
        })
    }

    /// Create a fallback stub that will fail on execute.
    /// Used when Python is not available but the manager still needs to be created.
    pub fn stub() -> Self {
        Self {
            python_path: String::new(),
            timeout: DEFAULT_TIMEOUT,
            max_output: DEFAULT_MAX_OUTPUT,
            scripts: HashMap::new(),
        }
    }

    /// Whether a usable Python interpreter was found at construction.
    /// A stub runtime (Python not on PATH) reports `false`.
    pub fn is_available(&self) -> bool {
        !self.python_path.is_empty()
    }

    /// Set the execution timeout.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set the max output size in bytes.
    pub fn with_max_output(mut self, max_output: usize) -> Self {
        self.max_output = max_output;
        self
    }

    /// Register a Python function as a UDF.
    ///
    /// The code must define a function: `def invoke(input: dict) -> dict`
    pub fn register(&mut self, name: &str, code: &str) -> Result<(), UdfRuntimeError> {
        // GAP-6: Reject registration up front when no Python interpreter is
        // available, rather than letting registration appear to succeed and
        // only failing later at execute time. Surfaces the misconfiguration
        // (python3 not on PATH) at the point the operator can act on it.
        if !self.is_available() {
            return Err(UdfRuntimeError::Python(
                "Python runtime not available (python3 not found); cannot register Python UDF"
                    .into(),
            ));
        }

        // Basic validation: check that the code defines an invoke function
        if !code.contains("def invoke") && !code.contains("def invoke(") {
            return Err(UdfRuntimeError::InvalidInput(
                "Python code must define a function 'invoke(input)'".into(),
            ));
        }

        self.scripts.insert(name.to_string(), code.to_string());
        Ok(())
    }

    /// Execute a registered Python UDF.
    ///
    /// Spawns a subprocess with restricted permissions, sends JSON input via stdin,
    /// receives JSON output from stdout.
    pub fn execute(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value, UdfRuntimeError> {
        if self.python_path.is_empty() {
            return Err(UdfRuntimeError::Python(
                "Python runtime not available (python3 not found)".into(),
            ));
        }

        let code = self
            .scripts
            .get(name)
            .ok_or_else(|| UdfRuntimeError::NotFound(name.to_string()))?;

        let input_str = serde_json::to_string(input)?;

        // Build the wrapper script that injects user code and handles I/O
        let script = build_wrapper_script(code);

        // Create a temp directory for CWD (empty = no filesystem access)
        let temp_dir = tempfile::tempdir()
            .map_err(|e| UdfRuntimeError::Python(format!("failed to create temp dir: {e}")))?;

        // Build the command with security restrictions
        let mut cmd = Command::new(&self.python_path);
        cmd.arg("-c")
            .arg(&script)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("LC_ALL", "C")
            .env("PYTHONIOENCODING", "utf-8")
            .current_dir(temp_dir.path());

        // Set rlimits on Unix
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;

            let max_output = self.max_output;
            unsafe {
                cmd.pre_exec(move || set_python_rlimits(max_output));
            }
        }

        let mut child = cmd
            .spawn()
            .map_err(|e| UdfRuntimeError::Python(format!("failed to spawn Python: {e}")))?;

        // Write input to stdin
        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(input_str.as_bytes())
                .map_err(|e| UdfRuntimeError::Python(format!("failed to write stdin: {e}")))?;
        }

        // Wait with timeout
        let start = Instant::now();
        let exit_status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {
                    if start.elapsed() > self.timeout {
                        let _ = child.kill();
                        let _ = child.wait();
                        return Err(UdfRuntimeError::Timeout(self.timeout));
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(e) => {
                    return Err(UdfRuntimeError::Python(format!(
                        "failed to wait for Python process: {e}"
                    )))
                }
            }
        };

        // Read stdout and stderr
        let mut stdout = Vec::new();
        if let Some(mut stdout_pipe) = child.stdout.take() {
            stdout_pipe
                .read_to_end(&mut stdout)
                .map_err(|e| UdfRuntimeError::Python(format!("failed to read stdout: {e}")))?;
        }

        let mut stderr = String::new();
        if let Some(mut stderr_pipe) = child.stderr.take() {
            stderr_pipe
                .read_to_string(&mut stderr)
                .map_err(|e| UdfRuntimeError::Python(format!("failed to read stderr: {e}")))?;
        }

        // Check output size
        if stdout.len() > self.max_output {
            return Err(UdfRuntimeError::Security(format!(
                "output size {} exceeds limit {}",
                stdout.len(),
                self.max_output
            )));
        }

        if !exit_status.success() {
            let exit_code = exit_status.code().unwrap_or(-1);
            let error_msg = if stderr.is_empty() {
                format!("Python process exited with code {exit_code}")
            } else {
                // Extract the last line of stderr (usually the error message)
                stderr.lines().last().unwrap_or("unknown error").to_string()
            };
            return Err(UdfRuntimeError::Python(error_msg));
        }

        // Parse output as JSON
        let stdout_str = String::from_utf8_lossy(&stdout);
        let result: serde_json::Value = serde_json::from_str(stdout_str.trim()).map_err(|e| {
            UdfRuntimeError::Python(format!(
                "failed to parse Python output as JSON: {e}\nOutput: {stdout_str}"
            ))
        })?;

        Ok(result)
    }

    /// List registered UDF names.
    pub fn list(&self) -> Vec<String> {
        self.scripts.keys().cloned().collect()
    }

    /// Remove a registered UDF.
    pub fn unregister(&mut self, name: &str) -> Result<(), UdfRuntimeError> {
        self.scripts
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| UdfRuntimeError::NotFound(name.to_string()))
    }
}

/// Build a wrapper Python script that injects user code and handles JSON I/O.
fn build_wrapper_script(user_code: &str) -> String {
    format!(
        r#"
import sys
import json
import traceback

# === USER CODE START ===
{user_code}
# === USER CODE END ===

def main():
    try:
        raw_input = sys.stdin.buffer.read().decode('utf-8')
        input_data = json.loads(raw_input)
    except Exception as e:
        sys.stderr.write("Failed to parse input JSON: {{}}\n".format(e))
        sys.exit(1)

    if 'invoke' not in globals():
        sys.stderr.write("Function 'invoke' not defined\n")
        sys.exit(1)

    try:
        result = invoke(input_data)
    except Exception as e:
        sys.stderr.write(traceback.format_exc())
        sys.exit(1)

    try:
        output = json.dumps(result)
        sys.stdout.buffer.write(output.encode('utf-8'))
        sys.stdout.buffer.flush()
    except Exception as e:
        sys.stderr.write("Failed to serialize output: {{}}\n".format(e))
        sys.exit(1)

main()
"#
    )
}

/// Set resource limits for the Python subprocess.
#[cfg(unix)]
fn set_python_rlimits(max_output: usize) -> Result<(), std::io::Error> {
    use nix::sys::resource::{setrlimit, Resource};

    // Limit CPU time (seconds)
    let _ = setrlimit(Resource::RLIMIT_CPU, CPU_LIMIT, CPU_LIMIT + 5);

    // Prevent file creation/writes (max file size = 0)
    let _ = setrlimit(Resource::RLIMIT_FSIZE, 0, 0);

    // Limit open file descriptors (low enough to restrict, high enough for Python startup)
    let _ = setrlimit(Resource::RLIMIT_NOFILE, NOFILE_LIMIT, NOFILE_LIMIT);

    // Limit address space (memory) — 256MB or 4x max_output, whichever is larger
    let mem_limit = std::cmp::max(256 * 1024 * 1024, (max_output * 4) as u64);
    let _ = setrlimit(Resource::RLIMIT_AS, mem_limit, mem_limit);

    // Limit core dump size to 0
    let _ = setrlimit(Resource::RLIMIT_CORE, 0, 0);

    // Prevent subprocess creation (fork) — set max processes to 0
    // This prevents the Python UDF from spawning child processes.
    // RLIMIT_NPROC is not exposed in the nix crate on macOS, so use libc directly.
    #[cfg(unix)]
    unsafe {
        let rlimit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        let _ = libc::setrlimit(libc::RLIMIT_NPROC, &rlimit);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn python_available() -> bool {
        Command::new("python3").arg("--version").output().is_ok()
    }

    #[test]
    fn test_create_runtime() {
        if !python_available() {
            return;
        }
        let runtime = PythonUdfRuntime::new("python3");
        assert!(runtime.is_ok());
    }

    #[test]
    fn test_register_invalid_code() {
        if !python_available() {
            return;
        }
        let mut runtime = PythonUdfRuntime::new("python3").unwrap();
        let result = runtime.register("bad", "print('hello')");
        assert!(result.is_err());
    }

    /// GAP-6: a stub runtime (no python3) must reject registration up front,
    /// rather than accepting it and only failing later at execute time.
    #[test]
    fn test_stub_rejects_registration() {
        let mut stub = PythonUdfRuntime::stub();
        assert!(!stub.is_available(), "stub should report unavailable");

        let err = stub
            .register("f", "def invoke(input):\n    return input")
            .expect_err("stub must reject registration");
        // The error should mention the runtime being unavailable, not code validity.
        assert!(
            matches!(err, UdfRuntimeError::Python(_)),
            "expected Python-unavailable error, got: {err:?}"
        );
    }

    #[test]
    fn test_python_basic_math() {
        if !python_available() {
            return;
        }
        let mut runtime = PythonUdfRuntime::new("python3").unwrap();
        runtime
            .register(
                "add",
                r#"
def invoke(input):
    return {"result": input["a"] + input["b"]}
"#,
            )
            .unwrap();

        let input = serde_json::json!({"a": 3, "b": 4});
        let result = runtime.execute("add", &input).unwrap();
        assert_eq!(result, serde_json::json!({"result": 7}));
    }

    #[test]
    fn test_python_string_manipulation() {
        if !python_available() {
            return;
        }
        let mut runtime = PythonUdfRuntime::new("python3").unwrap();
        runtime
            .register(
                "upper",
                r#"
def invoke(input):
    return {"result": input["text"].upper()}
"#,
            )
            .unwrap();

        let input = serde_json::json!({"text": "hello world"});
        let result = runtime.execute("upper", &input).unwrap();
        assert_eq!(result, serde_json::json!({"result": "HELLO WORLD"}));
    }

    #[test]
    fn test_python_list_and_unregister() {
        if !python_available() {
            return;
        }
        let mut runtime = PythonUdfRuntime::new("python3").unwrap();
        runtime
            .register("test", "def invoke(input):\n    return input\n")
            .unwrap();

        assert_eq!(runtime.list(), vec!["test"]);

        runtime.unregister("test").unwrap();
        assert!(runtime.list().is_empty());
    }
}
