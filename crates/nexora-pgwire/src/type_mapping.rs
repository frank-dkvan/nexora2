//! Type mapping between Nexora PropertyValue and PostgreSQL types.
//!
//! Maps the 16-variant PropertyValue enum to PG OIDs and handles
//! both text-format (for Simple Query) and binary-format (for
//! Extended Query) encoding.

use chrono::Timelike;
use nexora_id::PropertyValue;

// ─── PG OID Constants ───────────────────────────────────────
// From PostgreSQL system catalogs

/// OID for bigint (int8)
pub const OID_INT8: u32 = 20;
/// OID for boolean
pub const OID_BOOL: u32 = 16;
/// OID for double precision (float8)
pub const OID_FLOAT8: u32 = 701;
/// OID for text (varchar → text)
pub const OID_TEXT: u32 = 25;
/// OID for bytea
pub const OID_BYTEA: u32 = 17;
/// OID for jsonb
pub const OID_JSONB: u32 = 3802;
/// OID for date
pub const OID_DATE: u32 = 1082;
/// OID for timestamp without time zone
pub const OID_TIMESTAMP: u32 = 1114;
/// OID for timestamp with time zone (timestamptz)
pub const OID_TIMESTAMPTZ: u32 = 1184;
/// OID for interval
pub const OID_INTERVAL: u32 = 1186;
/// OID for point
pub const OID_POINT: u32 = 600;

/// Determine the PG OID and type name for a PropertyValue.
pub fn property_value_to_pg_type(pv: &PropertyValue) -> (u32, &'static str) {
    match pv {
        PropertyValue::Null => (OID_TEXT, "text"), // NULL has no type, default to text
        PropertyValue::Boolean(_) => (OID_BOOL, "bool"),
        PropertyValue::Integer(_) => (OID_INT8, "int8"),
        PropertyValue::Float(_) => (OID_FLOAT8, "float8"),
        PropertyValue::String(_) => (OID_TEXT, "text"),
        PropertyValue::Bytes(_) => (OID_BYTEA, "bytea"),
        PropertyValue::List(_) => (OID_JSONB, "jsonb"),
        PropertyValue::Map(_) => (OID_JSONB, "jsonb"),
        PropertyValue::Node(_) => (OID_JSONB, "jsonb"),
        PropertyValue::Relationship(_) => (OID_JSONB, "jsonb"),
        PropertyValue::Path(_) => (OID_JSONB, "jsonb"),
        PropertyValue::Date(_) => (OID_DATE, "date"),
        PropertyValue::LocalDateTime(_) => (OID_TIMESTAMP, "timestamp"),
        PropertyValue::ZonedDateTime(_) => (OID_TIMESTAMPTZ, "timestamptz"),
        PropertyValue::Duration(_) => (OID_INTERVAL, "interval"),
        PropertyValue::Point(point) if point.z.is_some() => (OID_JSONB, "jsonb"),
        PropertyValue::Point(_) => (OID_POINT, "point"),
        PropertyValue::BlobRef(_) => (OID_JSONB, "jsonb"),
    }
}

/// Format a PropertyValue as a PG text-format string for DataRow.
///
/// This produces the human-readable representation expected by
/// the PG protocol's "text format" (format code 0).
pub fn property_value_to_text(pv: &PropertyValue) -> Option<String> {
    match pv {
        PropertyValue::Null => None,
        PropertyValue::Boolean(b) => Some(if *b { "t".to_string() } else { "f".to_string() }),
        PropertyValue::Integer(i) => Some(i.to_string()),
        PropertyValue::Float(f) => Some(f.to_string()),
        PropertyValue::String(s) => Some(s.clone()),
        PropertyValue::Bytes(b) => {
            // hex encoding with \x prefix (PG bytea text format)
            Some(format!("\\x{}", hex::encode(b)))
        }
        PropertyValue::List(_)
        | PropertyValue::Map(_)
        | PropertyValue::Node(_)
        | PropertyValue::Relationship(_)
        | PropertyValue::Path(_) => serde_json::to_string(&property_value_to_json(pv)).ok(),
        PropertyValue::Date(d) => Some(d.format("%Y-%m-%d").to_string()),
        PropertyValue::LocalDateTime(dt) => Some(format!(
            "{} {:02}:{:02}:{:02}.{:06}",
            dt.format("%Y-%m-%d"),
            dt.time().hour(),
            dt.time().minute(),
            dt.time().second(),
            dt.time().nanosecond() / 1000,
        )),
        PropertyValue::ZonedDateTime(dt) => {
            // RFC3339 format with timezone
            Some(dt.format("%Y-%m-%d %H:%M:%S%.6f%:z").to_string())
        }
        PropertyValue::Duration(d) => {
            let mut parts = vec![];
            let total_seconds = d.seconds + d.nanoseconds / 1_000_000_000;
            let sign = if d.months < 0 || d.days < 0 || total_seconds < 0 {
                "-"
            } else {
                ""
            };
            if d.months != 0 {
                parts.push(format!("{} mons", d.months.abs()));
            }
            if d.days != 0 {
                parts.push(format!("{} days", d.days.abs()));
            }
            if total_seconds != 0 {
                let hours = total_seconds.abs() / 3600;
                let minutes = (total_seconds.abs() % 3600) / 60;
                let secs = total_seconds.abs() % 60;
                parts.push(format!("{:02}:{:02}:{:02}", hours, minutes, secs));
            }
            Some(format!(
                "{}{}",
                sign,
                if parts.is_empty() {
                    "00:00:00".into()
                } else {
                    parts.join(" ")
                }
            ))
        }
        PropertyValue::Point(p) if p.z.is_some() => {
            serde_json::to_string(&property_value_to_json(pv)).ok()
        }
        PropertyValue::Point(p) => Some(format!("({},{})", p.x, p.y)),
        PropertyValue::BlobRef(b) => Some(b.storage_path()),
    }
}

/// Convert a PropertyValue to a serde_json::Value for JSONB output.
fn property_value_to_json(pv: &PropertyValue) -> serde_json::Value {
    match pv {
        PropertyValue::Null => serde_json::Value::Null,
        PropertyValue::Boolean(b) => serde_json::Value::Bool(*b),
        PropertyValue::Integer(i) => serde_json::json!(i),
        PropertyValue::Float(f) => serde_json::json!(f),
        PropertyValue::String(s) => serde_json::Value::String(s.clone()),
        PropertyValue::Bytes(b) => serde_json::json!(hex::encode(b)),
        PropertyValue::List(l) => {
            serde_json::Value::Array(l.iter().map(property_value_to_json).collect())
        }
        PropertyValue::Map(m) => {
            let obj: serde_json::Map<String, serde_json::Value> = m
                .iter()
                .map(|(k, v)| (k.clone(), property_value_to_json(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
        PropertyValue::Node(n) => serde_json::json!({
            "id": hex::encode(&n.id),
            "labels": n.labels,
            "properties": n.properties.iter().map(|(key, value)| {
                (key.clone(), property_value_to_json(value))
            }).collect::<serde_json::Map<_, _>>(),
        }),
        PropertyValue::Relationship(r) => serde_json::json!({
            "id": hex::encode(&r.id),
            "type": r.rel_type,
            "start": hex::encode(&r.start_node_id),
            "end": hex::encode(&r.end_node_id),
            "properties": r.properties.iter().map(|(key, value)| {
                (key.clone(), property_value_to_json(value))
            }).collect::<serde_json::Map<_, _>>(),
        }),
        PropertyValue::Path(p) => serde_json::json!({
            "nodes": p.nodes.iter().map(|node| {
                property_value_to_json(&PropertyValue::Node(node.clone()))
            }).collect::<Vec<_>>(),
            "relationships": p.relationships.iter().map(|relationship| {
                property_value_to_json(&PropertyValue::Relationship(relationship.clone()))
            }).collect::<Vec<_>>(),
        }),
        PropertyValue::Date(d) => serde_json::Value::String(d.format("%Y-%m-%d").to_string()),
        PropertyValue::LocalDateTime(dt) => serde_json::Value::String(dt.to_string()),
        PropertyValue::ZonedDateTime(dt) => serde_json::Value::String(dt.to_string()),
        PropertyValue::Duration(d) => serde_json::Value::String(format!(
            "P{}M{}DT{}.{}S",
            d.months, d.days, d.seconds, d.nanoseconds
        )),
        PropertyValue::Point(p) => serde_json::json!({
            "x": p.x, "y": p.y, "z": p.z, "srid": p.srid,
        }),
        PropertyValue::BlobRef(b) => serde_json::json!({
            "bucket": b.bucket, "entry": b.entry,
            "timestamp_us": b.timestamp_us, "size": b.size,
            "content_type": b.content_type,
        }),
    }
}

/// Map a PG OID back to a descriptive type name for catalog queries.
pub fn oid_to_type_name(oid: u32) -> &'static str {
    match oid {
        OID_BOOL => "boolean",
        OID_INT8 => "bigint",
        OID_FLOAT8 => "double precision",
        OID_TEXT => "text",
        OID_BYTEA => "bytea",
        OID_JSONB => "jsonb",
        OID_DATE => "date",
        OID_TIMESTAMP => "timestamp without time zone",
        OID_TIMESTAMPTZ => "timestamp with time zone",
        OID_INTERVAL => "interval",
        OID_POINT => "point",
        _ => "text",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_boolean_to_text() {
        assert_eq!(
            property_value_to_text(&PropertyValue::Boolean(true)).unwrap(),
            "t"
        );
        assert_eq!(
            property_value_to_text(&PropertyValue::Boolean(false)).unwrap(),
            "f"
        );
        assert_eq!(property_value_to_text(&PropertyValue::Null), None);
    }

    #[test]
    fn test_integer_to_text() {
        assert_eq!(
            property_value_to_text(&PropertyValue::Integer(42)).unwrap(),
            "42"
        );
        assert_eq!(
            property_value_to_text(&PropertyValue::Integer(-1)).unwrap(),
            "-1"
        );
    }

    #[test]
    fn test_float_to_text() {
        let v = property_value_to_text(&PropertyValue::Float(3.5)).unwrap();
        assert_eq!(v, "3.5");
    }

    #[test]
    fn test_string_to_text() {
        assert_eq!(
            property_value_to_text(&PropertyValue::String("hello".into())).unwrap(),
            "hello"
        );
    }

    #[test]
    fn test_bytes_to_text() {
        let b = vec![0x00, 0x01, 0xFE];
        let text = property_value_to_text(&PropertyValue::Bytes(b)).unwrap();
        assert!(text.starts_with("\\x"));
    }

    #[test]
    fn test_date_to_text() {
        let d = NaiveDate::from_ymd_opt(2026, 7, 4).unwrap();
        assert_eq!(
            property_value_to_text(&PropertyValue::Date(d)).unwrap(),
            "2026-07-04"
        );
    }

    #[test]
    fn test_list_to_jsonb_text() {
        let l = PropertyValue::List(vec![PropertyValue::Integer(1), PropertyValue::Integer(2)]);
        let text = property_value_to_text(&l).unwrap();
        assert!(text.contains("1") && text.contains("2"), "got: {text}");
    }

    #[test]
    fn test_map_to_jsonb_text() {
        let mut m = std::collections::BTreeMap::new();
        m.insert("key".into(), PropertyValue::String("value".into()));
        let map = PropertyValue::Map(m);
        let text = property_value_to_text(&map).unwrap();
        assert!(text.contains("\"key\""));
        assert!(text.contains("\"value\""));
    }

    #[test]
    fn test_type_mapping() {
        assert_eq!(
            property_value_to_pg_type(&PropertyValue::Boolean(true)),
            (OID_BOOL, "bool")
        );
        assert_eq!(
            property_value_to_pg_type(&PropertyValue::Integer(1)),
            (OID_INT8, "int8")
        );
        assert_eq!(
            property_value_to_pg_type(&PropertyValue::Float(1.0)),
            (OID_FLOAT8, "float8")
        );
        assert_eq!(
            property_value_to_pg_type(&PropertyValue::String("".into())),
            (OID_TEXT, "text")
        );
        let d = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        assert_eq!(
            property_value_to_pg_type(&PropertyValue::Date(d)),
            (OID_DATE, "date")
        );
    }
}
