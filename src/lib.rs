use sqlparser::dialect::{
    AnsiDialect, BigQueryDialect, ClickHouseDialect, Dialect, DuckDbDialect, GenericDialect, HiveDialect, MsSqlDialect, PostgreSqlDialect, RedshiftSqlDialect, SQLiteDialect, SnowflakeDialect
};

pub mod context;
pub mod storage;
pub mod planner;

pub fn get_dialect(dialect: &str) -> Box<dyn Dialect> {
    match dialect {
        "ansi" => Box::new(AnsiDialect {}),
        "bigquery" => Box::new(BigQueryDialect {}),
        "clickhouse" => Box::new(ClickHouseDialect {}),
        "duckdb" => Box::new(DuckDbDialect {}),
        "hive" => Box::new(HiveDialect {}),
        "mssql" => Box::new(MsSqlDialect {}),
        "postgres" => Box::new(PostgreSqlDialect {}),
        "redshift" => Box::new(RedshiftSqlDialect {}),
        "sqlite" => Box::new(SQLiteDialect {}),
        "snowflake" => Box::new(SnowflakeDialect {}),
        _ => Box::new(GenericDialect {}),
    }
}
