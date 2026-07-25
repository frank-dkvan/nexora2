//! Small, truthful PostgreSQL compatibility surface.
//!
//! Full `pg_catalog` emulation is intentionally not advertised: Nexora does
//! not yet have a relational schema catalog. This module handles only stable
//! startup/session queries whose semantics Nexora can provide accurately.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireResult};

use crate::session::{parse_set_value, ConnectionContext};
use crate::PgAppState;

pub async fn try_handle_compatibility_query(
    state: &Arc<PgAppState>,
    context: &ConnectionContext,
    sql: &str,
) -> Option<PgWireResult<Response>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let uppercase = trimmed.to_ascii_uppercase();

    if uppercase == "BEGIN"
        || uppercase == "BEGIN TRANSACTION"
        || uppercase == "BEGIN WORK"
        || uppercase == "START TRANSACTION"
    {
        return Some(Ok(unsupported(
            "multi-statement transactions are not supported",
        )));
    }
    if matches!(
        uppercase.as_str(),
        "COMMIT"
            | "COMMIT TRANSACTION"
            | "COMMIT WORK"
            | "ROLLBACK"
            | "ROLLBACK TRANSACTION"
            | "ROLLBACK WORK"
    ) {
        return Some(Ok(Response::Execution(Tag::new(
            if uppercase.starts_with("COMMIT") {
                "COMMIT"
            } else {
                "ROLLBACK"
            },
        ))));
    }

    if let Some(key) = trimmed
        .strip_prefix("SHOW ")
        .or_else(|| trimmed.strip_prefix("show "))
    {
        let session = context.session.lock().await;
        return Some(Ok(match session.handle_show(key) {
            Some(value) => text_query(key.trim(), value),
            None => error_response(
                "42704",
                format!("unrecognized configuration parameter {:?}", key.trim()),
            ),
        }));
    }

    if uppercase.starts_with("SET ") {
        let rest = trimmed[4..].trim();
        return Some(Ok(match parse_set_value(rest) {
            Some((key, value)) if supported_parameter(key) => {
                context.session.lock().await.handle_set(key, value);
                Response::Execution(Tag::new("SET"))
            }
            Some((key, _)) => error_response(
                "0A000",
                format!("configuration parameter {key:?} is not supported"),
            ),
            None => error_response("42601", "invalid SET syntax".to_owned()),
        }));
    }

    if uppercase.starts_with("RESET ") {
        let key = trimmed[6..].trim();
        let reset = context.session.lock().await.handle_reset(key);
        return Some(Ok(if reset {
            Response::Execution(Tag::new("RESET"))
        } else {
            error_response(
                "42704",
                format!("unrecognized configuration parameter {key:?}"),
            )
        }));
    }

    if uppercase == "DISCARD ALL" {
        context.session.lock().await.discard_all();
        return Some(Ok(Response::Execution(Tag::new("DISCARD ALL"))));
    }

    if uppercase.contains("VERSION()") {
        return Some(Ok(text_query(
            "version",
            &format!(
                "PostgreSQL {} compatible Nexora Graph Database",
                state.server_version
            ),
        )));
    }
    if uppercase.contains("CURRENT_DATABASE()") {
        let database = context.session.lock().await.database.clone();
        return Some(Ok(text_query("current_database", &database)));
    }

    // P0.2: Support pg_matviews for psql \dv command
    if uppercase.contains("PG_MATVIEWS") {
        return Some(query_pg_matviews(state).await);
    }

    // current_schema()/session_user has no `pg_catalog.` prefix but is an
    // introspection query GUI clients send on connect; the pg_catalog shim must
    // see it before the SQL→Cypher path (which can't evaluate current_schema()).
    if uppercase.contains("CURRENT_SCHEMA()") {
        if let Some(response) =
            crate::pg_catalog::try_handle(state, context, trimmed, &uppercase).await
        {
            return Some(response);
        }
    }

    // Introspection queries: the `pg_catalog.`/`information_schema.` prefix, or a
    // bare reference to a `pg_`-prefixed system catalog (some driver queries use
    // e.g. `FROM pg_shdescription` without the schema qualifier). The pg_catalog
    // shim recognizes the common ones and returns an empty result for the long
    // tail of catalog objects nexora does not have (so GUI clients keep browsing
    // instead of aborting).
    if uppercase.contains("PG_CATALOG.")
        || uppercase.contains("INFORMATION_SCHEMA.")
        || references_bare_system_catalog(&uppercase)
    {
        if let Some(response) =
            crate::pg_catalog::try_handle(state, context, trimmed, &uppercase).await
        {
            return Some(response);
        }
        return Some(Ok(unsupported(
            "pg_catalog and information_schema emulation are not supported",
        )));
    }

    None
}

/// Whether an uppercased SQL statement references a `pg_`-prefixed system
/// catalog table by bare name (`FROM`/`JOIN pg_xxx`), as some driver
/// introspection queries do without the `pg_catalog.` qualifier.
fn references_bare_system_catalog(upper: &str) -> bool {
    const CATALOGS: [&str; 6] = [
        "PG_SHDESCRIPTION",
        "PG_DESCRIPTION",
        "PG_ROLES",
        "PG_DATABASE",
        "PG_NAMESPACE",
        "PG_TABLESPACE",
    ];
    CATALOGS
        .iter()
        .any(|c| upper.contains(&format!("FROM {c}")) || upper.contains(&format!("JOIN {c}")))
}

/// Query pg_matviews to list all materialized views (for psql \dv)
async fn query_pg_matviews(state: &Arc<PgAppState>) -> PgWireResult<Response> {
    let views = state.mv_manager.list_views().await;

    let schema = Arc::new(vec![
        FieldInfo::new(
            "schemaname".to_owned(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "matviewname".to_owned(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "matviewowner".to_owned(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        ),
        FieldInfo::new(
            "definition".to_owned(),
            None,
            None,
            Type::TEXT,
            FieldFormat::Text,
        ),
    ]);

    let schema_clone = schema.clone();
    let rows = views.into_iter().map(move |view| {
        let mut encoder = DataRowEncoder::new(schema_clone.clone());

        // schemaname
        encoder
            .encode_field_with_type_and_format(
                &"public",
                &Type::TEXT,
                FieldFormat::Text,
                &Default::default(),
            )
            .expect("text encoding is infallible");

        // matviewname
        encoder
            .encode_field_with_type_and_format(
                &view.name,
                &Type::TEXT,
                FieldFormat::Text,
                &Default::default(),
            )
            .expect("text encoding is infallible");

        // matviewowner
        encoder
            .encode_field_with_type_and_format(
                &"admin",
                &Type::TEXT,
                FieldFormat::Text,
                &Default::default(),
            )
            .expect("text encoding is infallible");

        // definition (source query)
        encoder
            .encode_field_with_type_and_format(
                &view.source_query,
                &Type::TEXT,
                FieldFormat::Text,
                &Default::default(),
            )
            .expect("text encoding is infallible");

        Ok(encoder.take_row())
    });

    Ok(Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(rows),
    )))
}

fn supported_parameter(key: &str) -> bool {
    matches!(
        key.trim().trim_matches('"').to_ascii_lowercase().as_str(),
        "application_name"
            | "client_encoding"
            | "datestyle"
            | "extra_float_digits"
            | "search_path"
            | "timezone"
    )
}

fn text_query(column: &str, value: &str) -> Response {
    let schema = Arc::new(vec![FieldInfo::new(
        column.to_owned(),
        None,
        None,
        Type::TEXT,
        FieldFormat::Text,
    )]);
    let mut encoder = DataRowEncoder::new(schema.clone());
    // Encoding a UTF-8 string as PostgreSQL text cannot fail.
    encoder
        .encode_field_with_type_and_format(
            &value,
            &Type::TEXT,
            FieldFormat::Text,
            &Default::default(),
        )
        .expect("text encoding is infallible");
    Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter([Ok(encoder.take_row())]),
    ))
}

fn unsupported(message: &str) -> Response {
    error_response("0A000", message.to_owned())
}

fn error_response(code: &str, message: String) -> Response {
    Response::Error(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        message,
    )))
}
