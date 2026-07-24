use pretty_assertions::assert_eq;

use super::table;
use super::validate_identifier;

#[test]
fn accepts_safe_postgres_schema_identifiers() {
    assert!(
        ["codex_state", "_codex2", "tenant42"]
            .into_iter()
            .map(validate_identifier)
            .all(|result| result.is_ok())
    );
}

#[test]
fn rejects_unsafe_postgres_schema_identifiers() {
    let values = ["", "42codex", "codex-state", "codex state", "codex\"state"];

    for value in values {
        assert!(validate_identifier(value).is_err(), "{value}");
    }
}

#[test]
fn quotes_validated_schema_and_static_table_name() {
    assert_eq!(
        table("codex_state", "threads"),
        "\"codex_state\".\"threads\""
    );
}
