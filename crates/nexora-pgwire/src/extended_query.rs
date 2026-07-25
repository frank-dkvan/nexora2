//! Extended query protocol policy.
//!
//! Nexora SQL does not yet expose a typed parameter binding API. Performing
//! textual `$1` substitution would be unsafe and would misdecode PostgreSQL
//! binary parameters, so extended queries are rejected explicitly until that
//! API exists. Clients can opt into PostgreSQL simple-query mode.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

#[derive(Debug, Default)]
pub struct NexoraExtendedQueryHandler;

#[async_trait]
impl ExtendedQueryHandler for NexoraExtendedQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser)
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<pgwire::api::results::Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(PgWireError::UserError(Box::new(ErrorInfo::new(
            "ERROR".to_owned(),
            "0A000".to_owned(),
            "extended query protocol is not supported; use simple query mode".to_owned(),
        ))))
    }
}
