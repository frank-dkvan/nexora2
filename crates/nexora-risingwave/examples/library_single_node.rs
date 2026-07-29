//! Library-mode smoke test: boot an in-process RisingWave single-node instance
//! and keep it running so a client (e.g. `psql -h 127.0.0.1 -p 4566 -U root -d dev`)
//! can connect to the embedded frontend.
//!
//! Run with:
//! ```bash
//! cargo run -p nexora-risingwave --features library --example library_single_node
//! ```
//!
//! Then, in another terminal:
//! ```bash
//! psql -h 127.0.0.1 -p 4566 -U root -d dev -c "SELECT 1;"
//! ```

use nexora_risingwave::{EmbeddedLibrary, EmbeddedLibraryConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,risingwave_storage=warn".into()),
        )
        .init();

    let config = EmbeddedLibraryConfig::new()
        .with_frontend_listen_addr("127.0.0.1:4566")
        .in_memory();

    // IMPORTANT: By default, RisingWave's meta service will shut down after a short
    // idle period if no workers connect. In single-node mode, compute/frontend need
    // time to start and register. We disable idle shutdown by omitting max_idle_secs
    // (None = never shut down due to idleness), which is the default for embedded use.

    println!("\n=== Starting embedded RisingWave (in-memory single-node) ===");
    let rw = EmbeddedLibrary::start(config)?;

    println!("\n[1/4] Meta service initializing...");
    println!("[2/4] Compute node starting...");
    println!("[3/4] Frontend starting...");
    println!("[4/4] Waiting for all services to be ready (this takes ~10-15 seconds)...");

    // RisingWave single-node needs time for all services to start and register
    // Meta starts first, then compute/frontend spawn. Give them time to initialize.
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;

    println!("\n✓ RisingWave should now be ready!");
    println!("\nConnect with:");
    println!(
        "  psql -h 127.0.0.1 -p {} -U root -d dev -c \"SELECT 1;\"",
        rw.frontend_listen_addr()
            .split(':')
            .next_back()
            .unwrap_or("4566")
    );
    println!("\nPress Ctrl-C to shut down.");

    // Wait for Ctrl-C, then shut down gracefully.
    tokio::signal::ctrl_c().await?;
    println!("\n\n=== Shutting down RisingWave ===");
    rw.shutdown().await?;
    println!("✓ Shutdown complete.\n");
    Ok(())
}
