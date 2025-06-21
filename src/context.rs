use std::collections::HashMap;

use anyhow::{Ok, Result, anyhow};
use sqlparser::{dialect::Dialect, parser::Parser};

use crate::planner::Planner;

pub struct LogicalPlan {}

pub struct ExecutionContext {
    pub dialect: Box<dyn Dialect>,
    pub planner: Planner,
}

impl ExecutionContext {
    pub fn new(dialect: Box<dyn Dialect>) -> ExecutionContext {
        let mut catalog = HashMap::new();

        catalog.insert(
            "users".to_string(),
            vec!["id".to_string(), "name".to_string(), "email".to_string()],
        );
        catalog.insert(
            "orders".to_string(),
            vec![
                "id".to_string(),
                "user_id".to_string(),
                "amount".to_string(),
            ],
        );
        catalog.insert(
            "products".to_string(),
            vec!["id".to_string(), "name".to_string(), "price".to_string()],
        );

        Self {
            dialect: dialect,
            planner: Planner::new(catalog),
        }
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        let ast = Parser::parse_sql(self.dialect.as_ref(), sql).unwrap();

        if ast.is_empty() {
            return Err(anyhow!("No statements found from this query: {}", sql));
        }

        let stmt = &ast[0];

        let planner = self.planner.generate(stmt)?;

        println!("{:#?}", planner);

        return Ok(());
    }
}
