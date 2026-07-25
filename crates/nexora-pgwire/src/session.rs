//! Per-connection PostgreSQL session state.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{watch, Mutex};
use uuid::Uuid;

static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Idle,
    InTransaction,
    Failed,
}

impl TransactionState {
    pub fn as_char(self) -> char {
        match self {
            Self::Idle => 'I',
            Self::InTransaction => 'T',
            Self::Failed => 'E',
        }
    }
}

#[derive(Debug)]
pub struct PgSession {
    pub connection_id: u64,
    pub session_id: Uuid,
    pub username: String,
    pub database: String,
    pub parameters: HashMap<String, String>,
    pub transaction_state: TransactionState,
    pub created_at: Instant,
    pub last_activity: Instant,
    defaults: HashMap<String, String>,
}

impl PgSession {
    pub fn new(username: String, database: String) -> Self {
        Self::new_with_version(username, database, "14.0")
    }

    pub fn new_with_version(username: String, database: String, server_version: &str) -> Self {
        let defaults = HashMap::from([
            ("client_encoding".to_owned(), "UTF8".to_owned()),
            ("server_version".to_owned(), server_version.to_owned()),
            ("datestyle".to_owned(), "ISO, MDY".to_owned()),
            ("integer_datetimes".to_owned(), "on".to_owned()),
            ("standard_conforming_strings".to_owned(), "on".to_owned()),
            ("timezone".to_owned(), "UTC".to_owned()),
            ("search_path".to_owned(), "public".to_owned()),
            ("extra_float_digits".to_owned(), "1".to_owned()),
            ("application_name".to_owned(), String::new()),
        ]);
        let now = Instant::now();
        Self {
            connection_id: NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed),
            session_id: Uuid::new_v4(),
            username,
            database,
            parameters: defaults.clone(),
            transaction_state: TransactionState::Idle,
            created_at: now,
            last_activity: now,
            defaults,
        }
    }

    pub fn set_identity(&mut self, username: String, database: String) {
        self.username = username;
        self.database = database;
        self.touch();
    }

    pub fn touch(&mut self) {
        self.last_activity = Instant::now();
    }

    pub fn is_idle_timeout(&self, timeout_secs: u64) -> bool {
        self.last_activity.elapsed().as_secs() >= timeout_secs
    }

    pub fn handle_set(&mut self, key: &str, value: &str) {
        self.parameters
            .insert(normalize_parameter(key), normalize_value(value));
        self.touch();
    }

    pub fn handle_show(&self, key: &str) -> Option<&String> {
        self.parameters.get(&normalize_parameter(key))
    }

    pub fn handle_reset(&mut self, key: &str) -> bool {
        let key = normalize_parameter(key);
        let Some(default) = self.defaults.get(&key).cloned() else {
            return false;
        };
        self.parameters.insert(key, default);
        self.touch();
        true
    }

    pub fn discard_all(&mut self) {
        self.parameters.clone_from(&self.defaults);
        self.transaction_state = TransactionState::Idle;
        self.touch();
    }

    pub fn ready_for_query_status(&self) -> char {
        self.transaction_state.as_char()
    }
}

#[derive(Clone)]
pub struct ConnectionContext {
    pub session: Arc<Mutex<PgSession>>,
    activity_tx: watch::Sender<ActivityState>,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ActivityState {
    pub sequence: u64,
    pub busy: bool,
}

impl ConnectionContext {
    pub(crate) fn new(server_version: &str) -> (Self, watch::Receiver<ActivityState>) {
        let (activity_tx, activity_rx) = watch::channel(ActivityState::default());
        (
            Self {
                session: Arc::new(Mutex::new(PgSession::new_with_version(
                    String::new(),
                    String::new(),
                    server_version,
                ))),
                activity_tx,
            },
            activity_rx,
        )
    }

    pub fn touch(&self) {
        self.activity_tx.send_modify(|state| {
            state.sequence = state.sequence.wrapping_add(1);
        });
    }

    pub fn begin_query(&self) {
        self.activity_tx.send_modify(|state| {
            state.sequence = state.sequence.wrapping_add(1);
            state.busy = true;
        });
    }

    pub fn end_query(&self) {
        self.activity_tx.send_modify(|state| {
            state.sequence = state.sequence.wrapping_add(1);
            state.busy = false;
        });
    }
}

pub fn parse_set_value(input: &str) -> Option<(&str, &str)> {
    if let Some(pos) = input.find('=') {
        let key = input[..pos].trim();
        let value = input[pos + 1..].trim();
        return (!key.is_empty() && !value.is_empty()).then_some((key, value));
    }
    let uppercase = input.to_uppercase();
    if let Some(pos) = uppercase.find(" TO ") {
        let key = input[..pos].trim();
        let value = input[pos + 4..].trim();
        return (!key.is_empty() && !value.is_empty()).then_some((key, value));
    }
    None
}

fn normalize_parameter(key: &str) -> String {
    key.trim().trim_matches('"').to_ascii_lowercase()
}

fn normalize_value(value: &str) -> String {
    value
        .trim()
        .trim_end_matches(';')
        .trim()
        .trim_matches('\'')
        .trim_matches('"')
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parameter_names_are_case_insensitive() {
        let mut session = PgSession::new("admin".into(), "nexora".into());
        assert_eq!(session.handle_show("DateStyle").unwrap(), "ISO, MDY");
        session.handle_set("TimeZone", "'Asia/Shanghai'");
        assert_eq!(session.handle_show("timezone").unwrap(), "Asia/Shanghai");
        assert!(session.handle_reset("TIMEZONE"));
        assert_eq!(session.handle_show("timezone").unwrap(), "UTC");
    }

    #[test]
    fn parses_set_forms() {
        assert_eq!(
            parse_set_value("application_name = 'psql'"),
            Some(("application_name", "'psql'"))
        );
        assert_eq!(
            parse_set_value("client_encoding TO 'UTF8'"),
            Some(("client_encoding", "'UTF8'"))
        );
    }

    #[test]
    fn connection_ids_are_monotonic() {
        let first = PgSession::new("a".into(), "db".into());
        let second = PgSession::new("b".into(), "db".into());
        assert!(second.connection_id > first.connection_id);
    }
}
