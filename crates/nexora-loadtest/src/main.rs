//! 负载测试工具
//!
//! 用于 Week 8 的 72小时生产验证测试

use clap::Parser;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "loadtest")]
#[command(about = "Nexora 2 load testing tool", long_about = None)]
struct Args {
    /// Test duration (e.g., "24h", "12h", "5m")
    #[arg(long, default_value = "1h")]
    duration: String,

    /// Write QPS (queries per second)
    #[arg(long, default_value = "1000")]
    write_qps: usize,

    /// Read QPS
    #[arg(long, default_value = "500")]
    read_qps: usize,

    /// Query QPS (Cypher queries)
    #[arg(long, default_value = "100")]
    query_qps: usize,

    /// Load pattern: constant, spike, mixed, longrun
    #[arg(long, default_value = "constant")]
    pattern: String,

    /// Peak QPS for spike pattern
    #[arg(long)]
    peak_qps: Option<usize>,

    /// Spike interval for spike pattern (e.g., "30m")
    #[arg(long)]
    spike_interval: Option<String>,

    /// Spike duration (e.g., "5m")
    #[arg(long)]
    spike_duration: Option<String>,

    /// Report interval (e.g., "1m")
    #[arg(long, default_value = "1m")]
    report_interval: String,

    /// Output file for results (JSON)
    #[arg(long, default_value = "loadtest_results.json")]
    output: String,

    /// Nexora server endpoint
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    endpoint: String,
}

#[derive(Debug, Clone, serde::Serialize)]
struct LoadTestMetrics {
    timestamp: u64,
    elapsed_seconds: u64,

    // Throughput
    writes_completed: u64,
    reads_completed: u64,
    queries_completed: u64,

    // Latency (milliseconds)
    write_latency_p50: f64,
    write_latency_p95: f64,
    write_latency_p99: f64,
    read_latency_p50: f64,
    read_latency_p95: f64,
    read_latency_p99: f64,
    query_latency_p50: f64,
    query_latency_p95: f64,
    query_latency_p99: f64,

    // Errors
    write_errors: u64,
    read_errors: u64,
    query_errors: u64,

    // Resource usage
    cpu_usage_percent: f64,
    memory_usage_mb: f64,
}

struct LoadTester {
    endpoint: String,
    metrics: Arc<tokio::sync::Mutex<Vec<LoadTestMetrics>>>,
    write_latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
    read_latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
    query_latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
}

impl LoadTester {
    fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            metrics: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            write_latencies: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            read_latencies: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            query_latencies: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    async fn run_constant_load(
        &self,
        duration: Duration,
        write_qps: usize,
        read_qps: usize,
        query_qps: usize,
        report_interval: Duration,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!(
            "Starting constant load test: duration={:?}, write_qps={}, read_qps={}, query_qps={}",
            duration, write_qps, read_qps, query_qps
        );

        let start_time = Instant::now();
        let end_time = start_time + duration;

        // Spawn worker tasks
        let write_semaphore = Arc::new(Semaphore::new(write_qps));
        let read_semaphore = Arc::new(Semaphore::new(read_qps));
        let query_semaphore = Arc::new(Semaphore::new(query_qps));

        // Write worker
        let write_worker = {
            let endpoint = self.endpoint.clone();
            let semaphore = Arc::clone(&write_semaphore);
            let latencies = Arc::clone(&self.write_latencies);
            tokio::spawn(async move {
                while Instant::now() < end_time {
                    let _permit = semaphore.acquire().await.unwrap();
                    let start = Instant::now();

                    // Simulate write request
                    match Self::write_node(&endpoint).await {
                        Ok(_) => {
                            let elapsed = start.elapsed();
                            latencies.lock().await.push(elapsed);
                        }
                        Err(e) => {
                            warn!("Write failed: {}", e);
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(1000 / write_qps as u64)).await;
                }
            })
        };

        // Read worker
        let read_worker = {
            let endpoint = self.endpoint.clone();
            let semaphore = Arc::clone(&read_semaphore);
            let latencies = Arc::clone(&self.read_latencies);
            tokio::spawn(async move {
                while Instant::now() < end_time {
                    let _permit = semaphore.acquire().await.unwrap();
                    let start = Instant::now();

                    // Simulate read request
                    match Self::read_node(&endpoint).await {
                        Ok(_) => {
                            let elapsed = start.elapsed();
                            latencies.lock().await.push(elapsed);
                        }
                        Err(e) => {
                            warn!("Read failed: {}", e);
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(1000 / read_qps as u64)).await;
                }
            })
        };

        // Query worker
        let query_worker = {
            let endpoint = self.endpoint.clone();
            let semaphore = Arc::clone(&query_semaphore);
            let latencies = Arc::clone(&self.query_latencies);
            tokio::spawn(async move {
                while Instant::now() < end_time {
                    let _permit = semaphore.acquire().await.unwrap();
                    let start = Instant::now();

                    // Simulate query request
                    match Self::query_graph(&endpoint).await {
                        Ok(_) => {
                            let elapsed = start.elapsed();
                            latencies.lock().await.push(elapsed);
                        }
                        Err(e) => {
                            warn!("Query failed: {}", e);
                        }
                    }

                    tokio::time::sleep(Duration::from_millis(1000 / query_qps as u64)).await;
                }
            })
        };

        // Reporter task
        let reporter = {
            let metrics = Arc::clone(&self.metrics);
            let write_latencies = Arc::clone(&self.write_latencies);
            let read_latencies = Arc::clone(&self.read_latencies);
            let query_latencies = Arc::clone(&self.query_latencies);

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(report_interval);
                while Instant::now() < end_time {
                    interval.tick().await;

                    let metric = Self::compute_metrics(
                        start_time,
                        &write_latencies,
                        &read_latencies,
                        &query_latencies,
                    )
                    .await;

                    info!(
                        "Metrics: writes={}, reads={}, queries={}, write_p99={:.2}ms",
                        metric.writes_completed,
                        metric.reads_completed,
                        metric.queries_completed,
                        metric.write_latency_p99
                    );

                    metrics.lock().await.push(metric);
                }
            })
        };

        // Wait for all workers
        let _ = tokio::join!(write_worker, read_worker, query_worker, reporter);

        info!("Load test completed");
        Ok(())
    }

    async fn write_node(endpoint: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/api/v2/graph/node", endpoint))
            .json(&serde_json::json!({
                "id": format!("user:{}", uuid::Uuid::new_v4()),
                "labels": ["User"],
                "properties": {
                    "name": "Test User",
                    "email": "test@example.com",
                    "created_at": chrono::Utc::now().timestamp()
                }
            }))
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(format!("Write failed: {}", resp.status()).into());
        }

        Ok(())
    }

    async fn read_node(endpoint: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::new();
        let node_id = format!("user:{}", uuid::Uuid::new_v4());
        let resp = client
            .get(format!("{}/api/v2/graph/node/{}", endpoint, node_id))
            .send()
            .await?;

        // 404 is expected for random IDs
        if resp.status() != 404 && !resp.status().is_success() {
            return Err(format!("Read failed: {}", resp.status()).into());
        }

        Ok(())
    }

    async fn query_graph(endpoint: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{}/api/v2/query/cypher", endpoint))
            .json(&serde_json::json!({
                "query": "MATCH (u:User) RETURN u LIMIT 10"
            }))
            .send()
            .await?;

        if !resp.status().is_success() {
            return Err(format!("Query failed: {}", resp.status()).into());
        }

        Ok(())
    }

    async fn compute_metrics(
        start_time: Instant,
        write_latencies: &Arc<tokio::sync::Mutex<Vec<Duration>>>,
        read_latencies: &Arc<tokio::sync::Mutex<Vec<Duration>>>,
        query_latencies: &Arc<tokio::sync::Mutex<Vec<Duration>>>,
    ) -> LoadTestMetrics {
        let write_lats = write_latencies.lock().await;
        let read_lats = read_latencies.lock().await;
        let query_lats = query_latencies.lock().await;

        LoadTestMetrics {
            timestamp: chrono::Utc::now().timestamp() as u64,
            elapsed_seconds: start_time.elapsed().as_secs(),
            writes_completed: write_lats.len() as u64,
            reads_completed: read_lats.len() as u64,
            queries_completed: query_lats.len() as u64,
            write_latency_p50: Self::percentile(&write_lats, 0.50),
            write_latency_p95: Self::percentile(&write_lats, 0.95),
            write_latency_p99: Self::percentile(&write_lats, 0.99),
            read_latency_p50: Self::percentile(&read_lats, 0.50),
            read_latency_p95: Self::percentile(&read_lats, 0.95),
            read_latency_p99: Self::percentile(&read_lats, 0.99),
            query_latency_p50: Self::percentile(&query_lats, 0.50),
            query_latency_p95: Self::percentile(&query_lats, 0.95),
            query_latency_p99: Self::percentile(&query_lats, 0.99),
            write_errors: 0,
            read_errors: 0,
            query_errors: 0,
            cpu_usage_percent: 0.0,
            memory_usage_mb: 0.0,
        }
    }

    fn percentile(durations: &[Duration], percentile: f64) -> f64 {
        if durations.is_empty() {
            return 0.0;
        }

        let mut sorted: Vec<_> = durations.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let idx = ((sorted.len() as f64 - 1.0) * percentile).ceil() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    async fn save_results(
        &self,
        output_path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let metrics = self.metrics.lock().await;
        let json = serde_json::to_string_pretty(&*metrics)?;
        tokio::fs::write(output_path, json).await?;
        info!("Results saved to {}", output_path);
        Ok(())
    }
}

fn parse_duration(s: &str) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let s = s.trim();
    if let Some(hours) = s.strip_suffix('h') {
        Ok(Duration::from_secs(hours.parse::<u64>()? * 3600))
    } else if let Some(minutes) = s.strip_suffix('m') {
        Ok(Duration::from_secs(minutes.parse::<u64>()? * 60))
    } else if let Some(seconds) = s.strip_suffix('s') {
        Ok(Duration::from_secs(seconds.parse::<u64>()?))
    } else {
        Err("Invalid duration format (use 24h, 5m, 30s)".into())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let duration = parse_duration(&args.duration)?;
    let report_interval = parse_duration(&args.report_interval)?;

    let tester = LoadTester::new(args.endpoint);

    match args.pattern.as_str() {
        "constant" => {
            tester
                .run_constant_load(
                    duration,
                    args.write_qps,
                    args.read_qps,
                    args.query_qps,
                    report_interval,
                )
                .await?;
        }
        "spike" => {
            info!("Spike pattern not implemented yet");
        }
        "mixed" => {
            info!("Mixed pattern not implemented yet");
        }
        "longrun" => {
            info!("Longrun pattern not implemented yet");
        }
        _ => {
            return Err(format!("Unknown pattern: {}", args.pattern).into());
        }
    }

    tester.save_results(&args.output).await?;

    Ok(())
}
