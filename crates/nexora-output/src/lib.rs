//! Output sinks for Standing Query results.
//!
//! Provides the [`OutputSink`] trait and implementations for sending
//! Standing Query results to various destinations.
//!
//! # Modules
//!
//! - [`sink_trait`]: Core `OutputSink` trait and `OutputError` type.
//! - [`console`]: `ConsoleOutput` sink that prints results to stdout.
//! - [`webhook`]: `WebhookOutput` sink that POSTs results to an HTTP endpoint.
//! - [`file`]: `FileOutput` sink that writes results to a JSON Lines file.
//! - [`drop`]: `DropOutput` sink that discards all results (no-op).
//! - [`kafka`]: `KafkaOutput` sink that publishes results to a Kafka topic.
//!
//! # Example
//!
//! ```rust,no_run
//! use nexora_output::{OutputSink, ConsoleOutput, FileOutput, FileOutputConfig};
//!
//! let console = ConsoleOutput::new("console");
//! let file = FileOutput::new("file", FileOutputConfig::default()).unwrap();
//! ```
//!
//! Console output with pretty formatting:
//!
//! ```rust,no_run
//! use nexora_output::ConsoleOutput;
//! let sink = ConsoleOutput::new("pretty-console");
//! ```

pub mod console;
pub mod drop;
pub mod file;
pub mod kafka;
pub mod registry;
pub mod sink_trait;
pub mod webhook;

pub use console::ConsoleOutput;
pub use drop::DropOutput;
pub use file::{FileOutput, FileOutputConfig};
pub use kafka::{KafkaOutput, KafkaOutputConfig};
pub use registry::{RegistryStats, SinkRegistry};
pub use sink_trait::{OutputError, OutputSink, OutputStatus};
pub use webhook::{WebhookConfig, WebhookOutput};
