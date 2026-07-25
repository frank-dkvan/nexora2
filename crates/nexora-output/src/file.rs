//! File output — appends SQ results to a JSON Lines file.
//!
//! Supports file rotation by max size and max retained files.

use crate::sink_trait::{OutputError, OutputSink, OutputStatus};
use async_trait::async_trait;
use nexora_standing_query::StandingQueryResult;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

/// Configuration for file output rotation.
#[derive(Clone, Debug)]
pub struct FileOutputConfig {
    /// Path to the output file.
    pub path: PathBuf,
    /// Maximum file size in bytes before rotation. 0 = no rotation.
    pub max_size: u64,
    /// Maximum number of rotated files to keep. 0 = unlimited.
    pub max_files: usize,
    /// Whether to pretty-print JSON.
    pub pretty: bool,
}

impl Default for FileOutputConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("nexora-output.jsonl"),
            max_size: 100 * 1024 * 1024, // 100 MB
            max_files: 5,
            pretty: false,
        }
    }
}

/// Output sink that writes SQ results to a file in JSON Lines format.
pub struct FileOutput {
    name: String,
    config: FileOutputConfig,
    writer: Mutex<std::fs::File>,
    bytes_written: Mutex<u64>,
}

impl FileOutput {
    pub fn new(name: &str, config: FileOutputConfig) -> Result<Self, OutputError> {
        let writer = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.path)
            .map_err(OutputError::Io)?;

        let metadata = writer.metadata().ok();
        let current_size = metadata.map(|m| m.len()).unwrap_or(0);

        Ok(Self {
            name: name.to_string(),
            config,
            writer: Mutex::new(writer),
            bytes_written: Mutex::new(current_size),
        })
    }

    fn rotate(&self) -> Result<(), OutputError> {
        if self.config.max_size == 0 {
            return Ok(());
        }

        let base = &self.config.path;
        let stem = base.file_stem().unwrap_or_default().to_string_lossy();
        let ext = base.extension().unwrap_or_default().to_string_lossy();

        // Remove oldest rotation if at max
        if self.config.max_files > 0 {
            let oldest = base.with_file_name(format!("{stem}.{}.{ext}", self.config.max_files - 1));
            let _ = std::fs::remove_file(&oldest);
        }

        for i in (1..self.config.max_files).rev() {
            let old_path = base.with_file_name(format!("{stem}.{i}.{ext}"));
            let new_path = base.with_file_name(format!("{stem}.{}.{ext}", i + 1));
            let _ = std::fs::rename(&old_path, &new_path);
        }

        let backup_path = base.with_file_name(format!("{stem}.1.{ext}"));
        std::fs::rename(base, &backup_path).map_err(OutputError::Io)?;

        let new_writer = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.config.path)
            .map_err(OutputError::Io)?;

        let mut w = self.writer.lock().unwrap();
        *w = new_writer;
        *self.bytes_written.lock().unwrap() = 0;

        Ok(())
    }
}

#[async_trait]
impl OutputSink for FileOutput {
    fn name(&self) -> &str {
        &self.name
    }

    async fn process(&self, result: &StandingQueryResult) -> Result<(), OutputError> {
        let json = if self.config.pretty {
            serde_json::to_string_pretty(&result.to_json())
        } else {
            serde_json::to_string(&result.to_json())
        }
        .map_err(|e| OutputError::Sink(format!("JSON serialization error: {e}")))?;

        let mut writer = self.writer.lock().unwrap();
        writeln!(writer, "{json}").map_err(OutputError::Io)?;
        writer.flush().map_err(OutputError::Io)?;

        let mut written = self.bytes_written.lock().unwrap();
        *written += json.len() as u64 + 1; // +1 for newline

        if self.config.max_size > 0 && *written >= self.config.max_size {
            drop(writer);
            drop(written);
            self.rotate()?;
        }

        Ok(())
    }

    fn status(&self) -> OutputStatus {
        OutputStatus::Active
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexora_id::{NexoraId, PropertyValue};
    use nexora_standing_query::ResultType;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_file_output_append() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test-output.jsonl");

        let config = FileOutputConfig {
            path: path.clone(),
            ..Default::default()
        };
        let sink = FileOutput::new("test-file", config).unwrap();

        let qid = NexoraId::from_bytes(b"node-1".to_vec());
        let qid_hex = qid.to_hex();
        let result = StandingQueryResult::new(
            uuid::Uuid::new_v4(),
            "test-sq",
            qid,
            {
                let mut m = std::collections::HashMap::new();
                m.insert("val".into(), PropertyValue::Integer(42));
                m
            },
            ResultType::Matched,
            chrono::Utc::now(),
        );

        sink.process(&result).await.unwrap();

        // Verify file contents
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("test-sq"));
        assert!(
            content.contains(&qid_hex),
            "Expected content to contain qid hex '{}', got: {content}",
            qid_hex
        );
    }

    #[tokio::test]
    async fn test_file_output_multiple_records() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("multi.jsonl");

        let config = FileOutputConfig {
            path: path.clone(),
            ..Default::default()
        };
        let sink = FileOutput::new("test-multi", config).unwrap();

        for i in 0..5 {
            let result = StandingQueryResult::new(
                uuid::Uuid::new_v4(),
                format!("sq-{i}"),
                NexoraId::from_bytes(b"x".to_vec()),
                std::collections::HashMap::new(),
                ResultType::Matched,
                chrono::Utc::now(),
            );
            sink.process(&result).await.unwrap();
        }

        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content.lines().count(), 5);
    }
}
