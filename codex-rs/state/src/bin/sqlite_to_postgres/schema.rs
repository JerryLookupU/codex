use anyhow::bail;

pub(super) fn validate_identifier(value: &str) -> anyhow::Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        bail!("PostgreSQL schema name cannot be empty");
    };
    if !(first == '_' || first.is_ascii_alphabetic())
        || !chars.all(|character| character == '_' || character.is_ascii_alphanumeric())
    {
        bail!("invalid PostgreSQL schema name `{value}`");
    }
    Ok(())
}

pub(super) fn table(schema: &str, name: &str) -> String {
    format!("\"{schema}\".\"{name}\"")
}

#[cfg(test)]
#[path = "schema_tests.rs"]
mod tests;
