//! Task registry for tracking tokio spawned tasks to prevent leaks.
//!
//! Provides a centralized registry for managing background tasks spawned with tokio::spawn.
//! Ensures all tasks can be gracefully shut down and awaited.

use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Registry for tracking spawned tokio tasks
pub struct TaskRegistry {
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl TaskRegistry {
    /// Create a new task registry
    pub fn new() -> Self {
        Self {
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Spawn a task and register its handle (blocking version for sync contexts)
    pub fn spawn_blocking<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(fut);
        self.handles.blocking_lock().push(handle);
    }

    /// Spawn a task and register its handle (async version)
    pub async fn spawn<F>(&self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(fut);
        self.handles.lock().await.push(handle);
    }

    /// Shutdown all registered tasks gracefully
    pub async fn shutdown(&self) {
        let handles = {
            let mut guard = self.handles.lock().await;
            std::mem::take(&mut *guard)
        };

        tracing::info!(count = handles.len(), "Shutting down registered tasks");

        for handle in handles {
            if let Err(e) = handle.await {
                tracing::error!(error = ?e, "Task panicked during shutdown");
            }
        }
    }

    /// Get the count of currently tracked tasks
    pub async fn task_count(&self) -> usize {
        self.handles.lock().await.len()
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_task_registry_tracks_tasks() {
        let registry = TaskRegistry::new();
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();

        registry
            .spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                flag_clone.store(true, Ordering::SeqCst);
            })
            .await;

        assert_eq!(registry.task_count().await, 1);

        registry.shutdown().await;
        assert!(flag.load(Ordering::SeqCst));
        assert_eq!(registry.task_count().await, 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_task_registry_shutdown_waits() {
        let registry = TaskRegistry::new();
        let counter = Arc::new(Mutex::new(0));
        let counter_clone = counter.clone();

        for _ in 0..5 {
            let c = counter_clone.clone();
            registry
                .spawn(async move {
                    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                    *c.lock().await += 1;
                })
                .await;
        }

        assert_eq!(registry.task_count().await, 5);
        registry.shutdown().await;
        assert_eq!(*counter.lock().await, 5);
        assert_eq!(registry.task_count().await, 0);
    }
}
