use anyhow::Result;
use sqlparser::ast::{
    Assignment, CreateIndex, CreateTable, Delete, Expr, Insert, Query, TableWithJoins,
    UpdateTableFromKind,
};

use crate::planner::common::{
    ColumnDefinition, JoinType, LogicalExpr, SortExpr, TableConstraintDef, UpdateAssignment,
};

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    TableScan {
        table_name: String,
        alias: Option<String>,
        projected_schema: Vec<String>,
    },
    Projection {
        expressions: Vec<LogicalExpr>,
        input: Box<LogicalPlan>,
    },
    Filter {
        predicate: LogicalExpr,
        input: Box<LogicalPlan>,
    },
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        join_constraint: Option<LogicalExpr>,
    },
    Limit {
        skip: Option<usize>,
        fetch: Option<usize>,
        input: Box<LogicalPlan>,
    },
    Sort {
        expressions: Vec<SortExpr>,
        input: Box<LogicalPlan>,
    },
    Aggregate {
        group_expr: Vec<LogicalExpr>,
        aggr_expr: Vec<LogicalExpr>,
        input: Box<LogicalPlan>,
    },
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<LogicalExpr>>,
        source: Option<Box<LogicalPlan>>, // For INSERT ... SELECT
    },
    Update {
        table_name: String,
        assignments: Vec<UpdateAssignment>,
        filter: Option<LogicalExpr>,
        from: Option<Box<LogicalPlan>>,
    },
    Delete {
        table_name: String,
        filter: Option<LogicalExpr>,
    },
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDefinition>,
        constraints: Vec<TableConstraintDef>,
        if_not_exists: bool,
    },
    CreateIndex {
        index_name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    DropTable {
        table_names: Vec<String>,
        if_exists: bool,
        cascade: bool,
    },
    DropIndex {
        index_names: Vec<String>,
        if_exists: bool,
        cascade: bool,
    },
}

impl LogicalPlan {
    pub fn create_table(create_table: &CreateTable) -> Result<LogicalPlan> {
        todo!()
    }

    pub fn create_index(create_index: &CreateIndex) -> Result<LogicalPlan> {
        todo!()
    }

    pub fn query(query: &Query) -> Result<LogicalPlan> {
        todo!("Implementing Query Planner")
    }

    pub fn insert(insert: &Insert) -> Result<LogicalPlan> {
        todo!()
    }

    pub fn delete(delete: &Delete) -> Result<LogicalPlan> {
        todo!()
    }

    pub fn update(
        table: &TableWithJoins,
        assignments: &[Assignment],
        from: &Option<UpdateTableFromKind>,
        selection: &Option<Expr>,
    ) -> Result<LogicalPlan> {
        todo!()
    }
}
