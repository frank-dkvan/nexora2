#!/usr/bin/env python3
"""One-shot terminology refactor: RisingWave/event-streams -> event-streaming.

Protected (never altered):
  - crate path/name `nexora_risingwave` / `nexora-risingwave`
  - vendored deps `risingwave_cmd_all`, `risingwave_common`, ... (word `risingwave_...`)
  - everything under vendor/ and the root workspace Cargo.toml
Narrative comments that reference the RisingWave *product* are intentionally
left intact (they accurately document the underlying implementation).
"""
import re
import sys

ROOT = "/Users/frank/aiCoding/nexora2"

# CamelCase public type tokens (apply to both app + crate sources).
# EmbeddedRisingWave before nothing needed; Distributed* is Distributed+EmbeddedRisingWave
# so replacing EmbeddedRisingWave first yields DistributedEmbeddedEventStreaming. Good.
TYPE_MAP = [
    ("EmbeddedRisingWave", "EmbeddedEventStreaming"),
    ("RisingWaveModule", "EventStreamingModule"),
    ("RisingWaveConfig", "EventStreamingConfig"),
    ("RisingWaveError", "EventStreamingError"),
    ("RisingWaveDdlRequest", "EventStreamingDdlRequest"),
    ("RisingWaveDdlResponse", "EventStreamingDdlResponse"),
    ("RisingWaveQueryRequest", "EventStreamingQueryRequest"),
    ("RisingWaveQueryResponse", "EventStreamingQueryResponse"),
    ("RisingWaveMaterializedView", "EventStreamingMaterializedView"),
    ("RisingWaveSource", "EventStreamingSource"),
    ("RisingWaveStatus", "EventStreamingStatus"),
]

def apply_types(text):
    for a, b in TYPE_MAP:
        text = text.replace(a, b)
    return text

def apply_feature_flag(text):
    # cfg feature strings in .rs
    return text.replace('feature = "risingwave"', 'feature = "event-streaming"')

def apply_snake_kebab(text):
    # event_streams -> event_streaming (snake): fields, fns, serde keys
    text = text.replace("event_streams", "event_streaming")
    # EventStreams -> EventStreaming (Camel): app config struct
    text = text.replace("EventStreams", "EventStreaming")
    # event-streams -> event-streaming (kebab): CLI help, warnings, data_dir path
    text = text.replace("event-streams", "event-streaming")
    return text

def apply_bare_lowercase(text):
    """Handle bare `risingwave` occurrences in app sources with the right casing
    per context. Ordered most-specific first; nexora_risingwave/risingwave_* are
    never matched because \\b requires a non-word boundary and `_` is a word char."""
    # stale feature-name refs in comments/docstrings
    text = text.replace("--features risingwave", "--features event-streaming")
    text = text.replace("--enable-risingwave", "--enable-event-streaming")
    text = text.replace("`risingwave`", "`event-streaming`")
    # route paths and on-disk dir strings (kebab)
    text = text.replace("/api/health/risingwave", "/api/health/event-streaming")
    text = text.replace("/api/risingwave/", "/api/event-streaming/")
    text = text.replace('"risingwave embedded"', '"event-streaming embedded"')
    text = text.replace('"risingwave"', '"event-streaming"')  # FeatureNotEnabled + dep string handled separately
    text = text.replace('risingwave-cluster', 'event-streaming-cluster')
    text = text.replace('risingwave-library', 'event-streaming-library')
    # storage subdir join("risingwave") (single-node embedded default)
    text = text.replace('.join("risingwave")', '.join("event-streaming")')
    # module path `handlers::risingwave` and `pub mod risingwave`
    text = text.replace("handlers::risingwave::", "handlers::event_streaming::")
    text = re.sub(r"\bmod risingwave\b", "mod event_streaming", text)
    # AppState field access `.risingwave` and definition `risingwave:` (snake)
    text = re.sub(r"\.risingwave\b", ".event_streaming", text)
    text = re.sub(r"(?<![\w:])risingwave:", "event_streaming:", text)
    # local variable names (snake) — distributed_/embedded_/library_/<x>_module etc.
    text = re.sub(r"\brisingwave_module\b", "event_streaming_module", text)
    text = re.sub(r"\b_risingwave_module\b", "_event_streaming_module", text)
    text = re.sub(r"\bembedded_risingwave\b", "embedded_event_streaming", text)
    text = re.sub(r"\b_embedded_risingwave\b", "_embedded_event_streaming", text)
    text = re.sub(r"\bdistributed_risingwave\b", "distributed_event_streaming", text)
    text = re.sub(r"\b_distributed_risingwave\b", "_distributed_event_streaming", text)
    text = re.sub(r"\blibrary_risingwave\b", "library_event_streaming", text)
    return text

def apply_user_facing_logs(text):
    """User-facing log/error text: swap the product name for the capability name."""
    text = text.replace('"   RisingWave: started', '"   Event Streaming: started')
    text = text.replace("Failed to start RisingWave module", "Failed to start event streaming engine")
    text = text.replace('"RisingWave initialization failed', '"Event streaming initialization failed')
    text = text.replace("Failed to start in-process RisingWave library",
                        "Failed to start in-process event streaming engine")
    text = text.replace("Shutting down embedded RisingWave", "Shutting down embedded event streaming engine")
    text = text.replace("Failed to shutdown embedded RisingWave", "Failed to shutdown embedded event streaming engine")
    text = text.replace('"Embedded RisingWave shut down', '"Embedded event streaming engine shut down')
    text = text.replace("Shutting down in-process RisingWave library",
                        "Shutting down in-process event streaming engine")
    text = text.replace("Failed to shutdown library RisingWave", "Failed to shutdown library event streaming engine")
    text = text.replace("In-process RisingWave library shut down", "In-process event streaming engine shut down")
    text = text.replace("(RisingWave embedded in nexora binary", "(RisingWave engine embedded in nexora binary")
    # "Event Streams:" prefix -> "Event Streaming:"
    text = text.replace("Event Streams:", "Event Streaming:")
    return text

APP_FILES = [
    "crates/nexora-app/src/main.rs",
    "crates/nexora-app/src/handlers.rs",
    "crates/nexora-app/src/config.rs",
    "crates/nexora-app/src/config_loader.rs",
    "crates/nexora-app/src/handlers/risingwave.rs",
]
CRATE_FILES = [
    "crates/nexora-risingwave/src/lib.rs",
    "crates/nexora-risingwave/src/config.rs",
    "crates/nexora-risingwave/src/error.rs",
    "crates/nexora-risingwave/src/module.rs",
    "crates/nexora-risingwave/src/event_sink.rs",
    "crates/nexora-risingwave/src/frontend_wrapper.rs",
    "crates/nexora-risingwave/src/meta_wrapper.rs",
    "crates/nexora-risingwave/src/ddl_parser.rs",
    "crates/nexora-risingwave/src/embedded_process.rs",
    "crates/nexora-risingwave/src/distributed.rs",
    "crates/nexora-risingwave/src/library.rs",
    "crates/nexora-risingwave/src/catalog.rs",
    "crates/nexora-risingwave/tests/embedded_integration.rs",
]

def process(path, is_app):
    full = f"{ROOT}/{path}"
    try:
        with open(full, "r") as f:
            text = f.read()
    except FileNotFoundError:
        print(f"  SKIP (not found): {path}")
        return
    orig = text
    text = apply_types(text)
    text = apply_feature_flag(text)
    if is_app:
        text = apply_snake_kebab(text)
        text = apply_bare_lowercase(text)
        text = apply_user_facing_logs(text)
    if text != orig:
        with open(full, "w") as f:
            f.write(text)
        print(f"  updated: {path}")
    else:
        print(f"  no change: {path}")

if __name__ == "__main__":
    print("App sources:")
    for p in APP_FILES:
        process(p, is_app=True)
    print("Crate sources:")
    for p in CRATE_FILES:
        process(p, is_app=False)
    print("done")

