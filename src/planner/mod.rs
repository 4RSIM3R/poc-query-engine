use std::collections::HashMap;

use anyhow::{Result, bail};
use sqlparser::ast::Statement;

use crate::planner::logical_plan::LogicalPlan;

pub mod common;
pub mod logical_plan;
pub mod utils;

pub struct Planner {
    pub catalog: HashMap<String, Vec<String>>,
}

impl Planner {
    pub fn new(catalog: HashMap<String, Vec<String>>) -> Self {
        Self { catalog: catalog }
    }

    pub fn generate(&self, stmt: &Statement) -> Result<LogicalPlan> {
        return match stmt {
            Statement::Query(query) => LogicalPlan::query(query),
            Statement::CreateTable(create_table) => LogicalPlan::create_table(create_table),
            Statement::CreateIndex(create_index) => LogicalPlan::create_index(create_index),
            Statement::Insert(insert) => LogicalPlan::insert(insert),
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                returning: _,
                or: _,
            } => LogicalPlan::update(table, assignments, from, selection),
            Statement::Delete(delete) => LogicalPlan::delete(delete),
            _ => bail!("Unsupported Statement"),
        };
    }
}
