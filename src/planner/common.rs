use sqlparser::ast::BinaryOperator;

use crate::planner::logical_plan::LogicalPlan;

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Utf8(String),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Debug, Clone)]
pub struct UpdateAssignment {
    pub column: String,
    pub value: LogicalExpr,
}

#[derive(Debug, Clone)]
pub struct SortExpr {
    pub expr: LogicalExpr,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone)]
pub enum LogicalExpr {
    Column {
        name: String,
        table: Option<String>,
    },
    Literal(ScalarValue),
    BinaryExpr {
        left: Box<LogicalExpr>,
        op: BinaryOperator,
        right: Box<LogicalExpr>,
    },
    Wildcard {
        table: Option<String>,
    },
    AggregateFunction {
        fun: AggregateFunction,
        args: Vec<LogicalExpr>,
        distinct: bool,
    },
    ScalarFunction {
        fun: String,
        args: Vec<LogicalExpr>,
    },
    Alias {
        expr: Box<LogicalExpr>,
        name: String,
    },
    Subquery {
        subquery: Box<LogicalPlan>,
    },
}

#[derive(Debug, Clone)]
pub enum DataTypeEnum {
    Varchar(Option<u64>),
    Integer,
    BigInt,
    Float,
    Double,
    Boolean,
    Date,
    Timestamp,
    Text,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataTypeEnum,
    pub nullable: bool,
    pub default: Option<LogicalExpr>,
    pub primary_key: bool,
}

#[derive(Debug, Clone)]
pub enum TableConstraintDef {
    PrimaryKey { columns: Vec<String> },
    ForeignKey { 
        columns: Vec<String>, 
        foreign_table: String, 
        foreign_columns: Vec<String> 
    },
    Unique { columns: Vec<String> },
    Check { expr: LogicalExpr },
}