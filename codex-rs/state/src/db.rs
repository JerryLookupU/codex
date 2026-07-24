pub use sqlx::Any;
use sqlx::Arguments;
use sqlx::AssertSqlSafe;
use sqlx::Encode;
use sqlx::FromRow;
use sqlx::Row as _;
use sqlx::Type;
use sqlx::any::AnyArguments;
use sqlx::any::AnyRow;
use sqlx::query::Query;
use sqlx::query::QueryAs;
use sqlx::query::QueryScalar;
use std::fmt::Display;
use std::fmt::Write;

pub type Pool = sqlx::AnyPool;
pub type Connection = sqlx::AnyConnection;
pub type Row = AnyRow;

pub fn bool_from_row(row: &Row, column: &str) -> Result<bool, sqlx::Error> {
    row.try_get::<bool, _>(column)
        .or_else(|_| row.try_get::<i64, _>(column).map(|value| value != 0))
}

pub fn optional_bool_from_row(row: &Row, column: &str) -> Result<Option<bool>, sqlx::Error> {
    row.try_get::<Option<bool>, _>(column).or_else(|_| {
        row.try_get::<Option<i64>, _>(column)
            .map(|value| value.map(|value| value != 0))
    })
}

pub fn query(sql: &'static str) -> Query<'static, Any, AnyArguments> {
    sqlx::query::<Any>(AssertSqlSafe(numbered_placeholders(sql)))
}

pub fn query_as<O>(sql: &'static str) -> QueryAs<'static, Any, O, AnyArguments>
where
    O: for<'row> FromRow<'row, AnyRow>,
{
    sqlx::query_as::<Any, O>(AssertSqlSafe(numbered_placeholders(sql)))
}

pub fn query_scalar<O>(sql: &'static str) -> QueryScalar<'static, Any, O, AnyArguments>
where
    (O,): for<'row> FromRow<'row, AnyRow>,
{
    sqlx::query_scalar::<Any, O>(AssertSqlSafe(numbered_placeholders(sql)))
}

/// SQLite queries in the upstream runtime use anonymous `?` parameters.
/// PostgreSQL requires numbered parameters, while SQLite accepts them, so `$N`
/// is the common representation for both runtime backends.
pub fn numbered_placeholders(sql: &str) -> String {
    let mut output = String::with_capacity(sql.len() + 16);
    let mut index = 0usize;
    let mut single_quoted = false;
    let mut double_quoted = false;
    let mut chars = sql.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '\'' if !double_quoted => {
                output.push(character);
                if single_quoted && chars.peek() == Some(&'\'') {
                    let _ = chars.next();
                    output.push('\'');
                } else {
                    single_quoted = !single_quoted;
                }
            }
            '"' if !single_quoted => {
                output.push(character);
                double_quoted = !double_quoted;
            }
            '?' if !single_quoted && !double_quoted => {
                index += 1;
                let _ = write!(&mut output, "${index}");
            }
            _ => output.push(character),
        }
    }
    output
}

pub struct QueryBuilder {
    sql: String,
    arguments: AnyArguments,
}

impl QueryBuilder {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            arguments: AnyArguments::default(),
        }
    }

    pub fn push(&mut self, sql: impl Display) -> &mut Self {
        let _ = write!(&mut self.sql, "{sql}");
        self
    }

    pub fn push_bind<'value, T>(&mut self, value: T) -> &mut Self
    where
        T: Encode<'value, Any> + Type<Any>,
    {
        let result = self.arguments.add(value);
        debug_assert!(result.is_ok(), "failed to encode SQL argument");
        let index = self.arguments.len();
        let _ = write!(&mut self.sql, "${index}");
        self
    }

    pub fn separated<'builder, Separator>(
        &'builder mut self,
        separator: Separator,
    ) -> Separated<'builder, Separator>
    where
        Separator: Display,
    {
        Separated {
            builder: self,
            separator,
            needs_separator: false,
        }
    }

    pub fn push_values<I, F>(&mut self, values: I, mut push_tuple: F) -> &mut Self
    where
        I: IntoIterator,
        F: FnMut(&mut Separated<'_, &'static str>, I::Item),
    {
        self.push("VALUES ");
        let mut separated = self.separated(", ");
        for value in values {
            separated.push_separator();
            separated.push_unseparated("(");
            let mut tuple = separated.reborrow();
            push_tuple(&mut tuple, value);
            separated.push_unseparated(")");
        }
        self
    }

    pub fn build(self) -> Query<'static, Any, AnyArguments> {
        sqlx::query_with::<Any, _>(AssertSqlSafe(self.sql), self.arguments)
    }

    #[cfg(test)]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn build_query_as<O>(self) -> QueryAs<'static, Any, O, AnyArguments>
    where
        O: for<'row> FromRow<'row, AnyRow>,
    {
        sqlx::query_as_with::<Any, O, _>(AssertSqlSafe(self.sql), self.arguments)
    }
}

pub struct Separated<'builder, Separator>
where
    Separator: Display,
{
    builder: &'builder mut QueryBuilder,
    separator: Separator,
    needs_separator: bool,
}

impl<'builder, Separator> Separated<'builder, Separator>
where
    Separator: Display,
{
    fn reborrow(&mut self) -> Separated<'_, &'static str> {
        Separated {
            builder: self.builder,
            separator: ", ",
            needs_separator: false,
        }
    }

    fn push_separator(&mut self) {
        if self.needs_separator {
            self.builder.push(&self.separator);
        }
        self.needs_separator = true;
    }

    pub fn push(&mut self, sql: impl Display) -> &mut Self {
        self.push_separator();
        self.builder.push(sql);
        self
    }

    pub fn push_unseparated(&mut self, sql: impl Display) -> &mut Self {
        self.builder.push(sql);
        self
    }

    pub fn push_bind<'value, T>(&mut self, value: T) -> &mut Self
    where
        T: Encode<'value, Any> + Type<Any>,
    {
        self.push_separator();
        self.builder.push_bind(value);
        self
    }

    pub fn push_bind_unseparated<'value, T>(&mut self, value: T) -> &mut Self
    where
        T: Encode<'value, Any> + Type<Any>,
    {
        self.builder.push_bind(value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::numbered_placeholders;

    #[test]
    fn numbers_only_parameters_outside_quotes() {
        assert_eq!(
            numbered_placeholders("SELECT '?', \"?\", value FROM t WHERE a = ? AND b = ?"),
            "SELECT '?', \"?\", value FROM t WHERE a = $1 AND b = $2"
        );
    }

    #[test]
    fn builds_multi_row_values_without_extra_parentheses() {
        let mut builder = super::QueryBuilder::new("INSERT INTO logs (id, level) ");
        builder.push_values([(1_i64, "INFO"), (2_i64, "DEBUG")], |row, (id, level)| {
            row.push_bind(id).push_bind(level);
        });

        assert_eq!(
            builder.sql(),
            "INSERT INTO logs (id, level) VALUES ($1, $2), ($3, $4)"
        );
    }

    #[test]
    fn reads_sqlite_integer_booleans_through_any() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        runtime.block_on(async {
            sqlx::any::install_default_drivers();
            let pool = sqlx::any::AnyPoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .expect("open sqlite");
            let row = super::query("SELECT 1 AS enabled, NULL AS inherited")
                .fetch_one(&pool)
                .await
                .expect("query row");

            assert!(super::bool_from_row(&row, "enabled").expect("read boolean"));
            assert_eq!(
                super::optional_bool_from_row(&row, "inherited").expect("read optional boolean"),
                None
            );
        });
    }
}
