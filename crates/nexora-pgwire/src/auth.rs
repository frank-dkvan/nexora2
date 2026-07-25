//! PostgreSQL startup and SCRAM-SHA-256 authentication.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::auth::sasl::scram::{gen_salted_password, ScramAuth, SCRAM_ITERATIONS};
use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::{
    finish_authentication, protocol_negotiation, save_startup_parameters_to_metadata, AuthSource,
    DefaultServerParameterProvider, LoginInfo, Password, StartupHandler,
};
use pgwire::api::{
    ClientInfo, PidSecretKeyGenerator, RandomPidSecretKeyGenerator, METADATA_DATABASE,
    METADATA_USER,
};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use crate::session::ConnectionContext;
use crate::PgAppState;

#[derive(Debug)]
struct NexoraAuthSource {
    users: Arc<std::collections::HashMap<String, (String, String)>>,
}

#[async_trait]
impl AuthSource for NexoraAuthSource {
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        // P1-3 fix: Use fixed salt for all unknown users to prevent timing attacks.
        // Random salt generation takes variable time and leaks user existence.
        const INVALID_USER_SALT: &[u8; 16] = b"nexora-fake-salt";

        let (password, salt) = login
            .user()
            .and_then(|username| self.users.get(username))
            .map(|(password, salt_hex)| {
                let salt = hex::decode(salt_hex).unwrap_or_else(|_| INVALID_USER_SALT.to_vec());
                (password.as_str(), salt)
            })
            .unwrap_or(("nexora-invalid-credential", INVALID_USER_SALT.to_vec()));

        let salted = gen_salted_password(password, &salt, SCRAM_ITERATIONS);
        Ok(Password::new(Some(salt), salted))
    }
}

#[allow(dead_code)] // PG-wire auth stub, UuidSalt reserved for SCRAM-SHA-256 implementation
struct UuidSalt(uuid::Uuid);

#[allow(dead_code)]
impl UuidSalt {
    fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }

    fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

pub struct NexoraStartupHandler {
    trust: bool,
    scram: Option<SASLAuthStartupHandler<DefaultServerParameterProvider>>,
    parameters: DefaultServerParameterProvider,
    pid_generator: RandomPidSecretKeyGenerator,
    context: ConnectionContext,
}

impl NexoraStartupHandler {
    pub fn new(state: &Arc<PgAppState>, context: ConnectionContext) -> Self {
        let mut parameters = DefaultServerParameterProvider::default();
        parameters.server_version = state.server_version.clone();
        parameters.is_superuser = false;

        let scram = if state.trust_auth {
            None
        } else {
            let source = Arc::new(NexoraAuthSource {
                users: state.users.clone(),
            });
            Some(
                SASLAuthStartupHandler::new(Arc::new({
                    let mut provider = DefaultServerParameterProvider::default();
                    provider.server_version = state.server_version.clone();
                    provider.is_superuser = false;
                    provider
                }))
                .with_scram(ScramAuth::new(source)),
            )
        };

        Self {
            trust: state.trust_auth,
            scram,
            parameters,
            pid_generator: RandomPidSecretKeyGenerator::default(),
            context,
        }
    }

    async fn update_session<C>(&self, client: &C)
    where
        C: ClientInfo,
    {
        let username = client
            .metadata()
            .get(METADATA_USER)
            .cloned()
            .unwrap_or_default();
        let database = client
            .metadata()
            .get(METADATA_DATABASE)
            .cloned()
            .unwrap_or_else(|| username.clone());
        self.context
            .session
            .lock()
            .await
            .set_identity(username, database);
    }
}

#[async_trait]
impl StartupHandler for NexoraStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        self.context.touch();

        if let PgWireFrontendMessage::Startup(startup) = &message {
            let database = startup
                .parameters
                .get(METADATA_DATABASE)
                .map(String::as_str)
                .unwrap_or("");
            if database != "nexora" {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "FATAL".to_owned(),
                    "3D000".to_owned(),
                    format!("database {database:?} does not exist"),
                ))));
            }
            if !startup.parameters.contains_key(METADATA_USER) {
                return Err(PgWireError::UserNameRequired);
            }
        }

        if !self.trust {
            let is_startup = matches!(message, PgWireFrontendMessage::Startup(_));
            self.scram
                .as_ref()
                .expect("SCRAM handler is present outside trust mode")
                .on_startup(client, message)
                .await?;
            if is_startup {
                self.update_session(client).await;
            }
            return Ok(());
        }

        if let PgWireFrontendMessage::Startup(startup) = message {
            protocol_negotiation(client, &startup).await?;
            save_startup_parameters_to_metadata(client, &startup);
            self.update_session(client).await;
            let (pid, secret) = self.pid_generator.generate(client);
            client.set_pid_and_secret_key(pid, secret);
            finish_authentication(client, &self.parameters).await?;
        }
        Ok(())
    }
}
