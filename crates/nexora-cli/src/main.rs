//! `nex` — thin CLI client for nexora, agent-friendly.
//!
//! All commands output JSON by default (human-readable with `--human` where it
//! makes sense). Exit code 0 = success, 1 = client/network error, 2 = server
//! error or bad input. Non-interactive: reads from stdin/args, prints to
//! stdout/stderr, never prompts. Designed for scripting and agent use.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use nexora_client::{types::*, NexoraClient};
use serde_json::Value;

mod repl;

#[derive(Parser)]
#[command(name = "nex", about = "Nexora CLI client", version)]
struct Cli {
    /// Nexora HTTP API base URL.
    #[arg(long, env = "NEXORA_URL", default_value = "http://localhost:8080")]
    url: String,

    /// Bearer token for authentication (if required).
    #[arg(long, env = "NEXORA_TOKEN")]
    token: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Execute a Cypher query.
    Cypher {
        /// Cypher query string (or '-' to read from stdin).
        query: String,
    },
    /// Execute a SQL query.
    Sql {
        /// SQL query string (or '-' to read from stdin).
        query: String,
    },
    /// Bulk-ingest JSON records.
    Ingest {
        /// JSON array of records (or '-' to read from stdin). Each record is an
        /// object; the id_field extracts the node id, other fields become properties.
        #[arg(default_value = "-")]
        records: String,

        /// JSON field to extract node id from (default: "id").
        #[arg(long, default_value = "id")]
        id_field: String,
    },
    /// Standing query operations.
    #[command(subcommand)]
    Sq(SqCommands),
    /// Node property operations.
    #[command(subcommand)]
    Node(NodeCommands),
    /// Edge operations.
    #[command(subcommand)]
    Edges(EdgesCommands),
    /// Vector search operations.
    #[command(subcommand)]
    Vector(VectorCommands),
    /// Health check (liveness/readiness).
    Health {
        /// Check readiness instead of liveness.
        #[arg(long)]
        readiness: bool,
    },
    /// Start an interactive REPL (read-eval-print loop).
    Repl,
}

#[derive(Subcommand)]
enum SqCommands {
    /// List all standing queries.
    List,
    /// Create a standing query.
    Create {
        /// Name for the standing query.
        name: String,
        /// Pattern (JSON object with type/key/condition/labels fields).
        pattern: String,
    },
    /// Get a standing query by name.
    Get { name: String },
    /// Delete a standing query by name.
    Delete { name: String },
}

#[derive(Subcommand)]
enum NodeCommands {
    /// Get a property value for a node.
    Get {
        /// Node id (hex or string, hashed if not hex).
        qid: String,
        /// Property key.
        key: String,
    },
    /// Set a property value for a node.
    Set {
        /// Node id (hex or string, hashed if not hex).
        qid: String,
        /// Property key.
        key: String,
        /// Property value (JSON).
        value: String,
    },
}

#[derive(Subcommand)]
enum EdgesCommands {
    /// Get edges for a node.
    Get {
        /// Node id (hex or string, hashed if not hex).
        qid: String,
    },
    /// Add an edge.
    Add {
        /// Source node id.
        source: String,
        /// Edge type label.
        #[arg(long)]
        edge_type: String,
        /// Target node id.
        target: String,
        /// Direction: "outgoing" or "incoming".
        #[arg(long, default_value = "outgoing")]
        direction: String,
    },
}

#[derive(Subcommand)]
enum VectorCommands {
    /// Vector search.
    Search {
        /// Query vector (JSON array of floats).
        vector: String,
        /// Max results to return.
        #[arg(long, default_value = "10")]
        k: usize,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let exit_code = match run(cli).await {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("Error: {e:#}");
            if e.to_string().contains("server") || e.to_string().contains("status") {
                2 // server error
            } else {
                1 // client/network error
            }
        }
    };
    std::process::exit(exit_code);
}

async fn run(cli: Cli) -> Result<()> {
    let mut builder = NexoraClient::builder().base_url(cli.url);
    if let Some(token) = cli.token {
        builder = builder.bearer_token(token);
    }
    let client = builder.build().context("Failed to build client")?;

    // The REPL owns its own loop; everything else is a single command dispatch.
    if let Commands::Repl = cli.command {
        return repl::run_repl(&client).await;
    }

    execute(&client, cli.command).await
}

/// Execute a single non-REPL command against an already-built client.
///
/// Split out from `run` so the interactive REPL can dispatch the same command
/// set without rebuilding the client per line.
async fn execute(client: &NexoraClient, command: Commands) -> Result<()> {
    match command {
        Commands::Repl => {
            // `run` intercepts Repl before calling `execute`; reaching here would
            // mean a nested REPL request, which we simply ignore.
        }
        Commands::Cypher { query } => {
            let q = read_input(&query)?;
            let resp = client
                .execute_cypher(&q)
                .await
                .context("Cypher execution failed")?;
            print_json_value(&resp)?;
        }
        Commands::Sql { query } => {
            let q = read_input(&query)?;
            let resp = client
                .execute_sql(&q)
                .await
                .context("SQL execution failed")?;
            print_json_value(&resp)?;
        }
        Commands::Ingest { records, id_field } => {
            let json_str = read_input(&records)?;
            let records_array: Vec<Value> = serde_json::from_str(&json_str)
                .context("Records must be a JSON array of objects")?;
            let resp = client
                .bulk_ingest(records_array, id_field)
                .await
                .context("Bulk ingest failed")?;
            print_json_value(&resp)?;
        }
        Commands::Sq(sq_cmd) => match sq_cmd {
            SqCommands::List => {
                let resp = client
                    .list_standing_queries()
                    .await
                    .context("List standing queries failed")?;
                print_json_value(&resp)?;
            }
            SqCommands::Create { name, pattern } => {
                let pattern_val: SqPatternRequest = serde_json::from_str(&pattern).context(
                    "Pattern must be a valid JSON object with type/key/condition/labels",
                )?;
                let req = CreateSqRequest {
                    name,
                    pattern: pattern_val,
                };
                let resp = client
                    .create_standing_query(&req)
                    .await
                    .context("Create standing query failed")?;
                print_json_value(&resp)?;
            }
            SqCommands::Get { name } => {
                let resp = client
                    .get_standing_query(&name)
                    .await
                    .context("Get standing query failed")?;
                print_json_value(&resp)?;
            }
            SqCommands::Delete { name } => {
                let resp = client
                    .delete_standing_query(&name)
                    .await
                    .context("Delete standing query failed")?;
                print_json_value(&resp)?;
            }
        },
        Commands::Node(node_cmd) => match node_cmd {
            NodeCommands::Get { qid, key } => {
                let resp = client
                    .get_property(&qid, &key)
                    .await
                    .context("Get property failed")?;
                print_json_value(&resp)?;
            }
            NodeCommands::Set { qid, key, value } => {
                let val: Value =
                    serde_json::from_str(&value).context("Value must be valid JSON")?;
                let resp = client
                    .set_property(&qid, &key, val)
                    .await
                    .context("Set property failed")?;
                print_json_value(&resp)?;
            }
        },
        Commands::Edges(edges_cmd) => match edges_cmd {
            EdgesCommands::Get { qid } => {
                let resp = client.get_edges(&qid).await.context("Get edges failed")?;
                print_json_value(&resp)?;
            }
            EdgesCommands::Add {
                source,
                edge_type,
                target,
                direction,
            } => {
                let resp = client
                    .add_edge(&source, edge_type, target, direction)
                    .await
                    .context("Add edge failed")?;
                print_json_value(&resp)?;
            }
        },
        Commands::Vector(vector_cmd) => match vector_cmd {
            VectorCommands::Search { vector, k } => {
                let vec: Vec<f32> = serde_json::from_str(&vector)
                    .context("Vector must be a JSON array of floats")?;
                let resp = client
                    .vector_search(vec, k)
                    .await
                    .context("Vector search failed")?;
                print_json_value(&resp)?;
            }
        },
        Commands::Health { readiness } => {
            if readiness {
                let resp = client.readiness().await.context("Readiness check failed")?;
                print_json_value(&resp)?;
            } else {
                let resp = client.liveness().await.context("Liveness check failed")?;
                print_json_value(&resp)?;
            }
        }
    }
    Ok(())
}

/// Read input: if "-", read from stdin; else return the string as-is.
fn read_input(s: &str) -> Result<String> {
    if s == "-" {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("Failed to read from stdin")?;
        Ok(buf.trim().to_string())
    } else {
        Ok(s.to_string())
    }
}

/// Print a response as JSON. Since not all response types derive Serialize, we
/// use serde_json::to_value to reflectively serialize them.
fn print_json_value<T>(val: &T) -> Result<()>
where
    T: serde::Serialize,
{
    let json_val = serde_json::to_value(val).context("Failed to serialize response")?;
    println!("{}", serde_json::to_string(&json_val)?);
    Ok(())
}
