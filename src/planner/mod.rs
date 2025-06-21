use std::collections::HashMap;

use anyhow::{Ok, Result, anyhow, bail};
use sqlparser::ast::{
    BinaryOperator, Expr, ObjectName, ObjectNamePart, Query, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, Value,
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
}

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Utf8(String),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Null,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

pub struct Planner {
    pub catalog: HashMap<String, Vec<String>>,
}

impl Planner {
    pub fn new(catalog: HashMap<String, Vec<String>>) -> Self {
        Self { catalog: catalog }
    }

    pub fn generate(&self, stmt: &Statement) -> Result<LogicalPlan> {
        return match stmt {
            Statement::Query(query) => self.plan_query(&query),
            Statement::CreateTable(_table) => todo!("WIP: CREATE TABLE query"),
            Statement::Insert(_insert) => todo!("WIP: INSERT query"),
            Statement::Update {
                table,
                assignments,
                from,
                selection,
                returning,
                or,
            } => todo!("WIP: UPDATE query"),
            Statement::Delete(delete) => todo!("WIP: DELETE query"),
            _ => {
                return Err(anyhow!("Unsupported Statement"));
            }
        };
    }

    fn plan_query(&self, query: &Query) -> Result<LogicalPlan> {
        let select = match &*query.body {
            SetExpr::Select(select) => select,
            SetExpr::Query(query) => todo!("Nested queries are not supported yet"),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => todo!("Set operations (UNION, INTERSECT, EXCEPT) are not supported yet"),
            SetExpr::Values(values) => todo!(),
            SetExpr::Insert(statement) => todo!(),
            SetExpr::Update(statement) => todo!(),
            SetExpr::Delete(statement) => todo!(),
            SetExpr::Table(table) => todo!(),
        };

        // This will be handling FROM clause, can be both from 'table' or a JOIN
        // Will resulting LogicalPlan::TableScan|LogicalPlan::Join
        let mut plan = self.plan_from_clause(&select.from)?;

        // TODO: Add handling on WHERE clause,
        // Will resulting LogicalPlan::Filter

        // This will be handling Projection / selecting column from query
        let projection_expression = self.plan_projection_clause(&select.projection).unwrap();
        plan = LogicalPlan::Projection {
            expressions: projection_expression,
            input: Box::new(plan),
        };

        println!("{}", self.format_plan(&plan, 1));

        Ok(plan)
    }

    pub fn plan_from_clause(&self, from: &[TableWithJoins]) -> Result<LogicalPlan> {
        if from.is_empty() {
            return Err(anyhow!("FROM clause is required"));
        }

        // Handling FROM clause, can be both from
        let mut plan_table_factor = self.plan_table_factor(&from[0].relation)?;

        // TODO: Add Handling JOIN

        Ok(plan_table_factor)
    }

    pub fn plan_projection_clause(&self, projection: &[SelectItem]) -> Result<Vec<LogicalExpr>> {
        let mut expressions = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    expressions.push(self.expr_to_logical_expr(expr)?);
                }
                SelectItem::Wildcard(qualifier) => {
                    expressions.push(LogicalExpr::Wildcard { table: None });
                }
                // SelectItem::ExprWithAlias { expr, alias } => {
                //     // For now, treat as unnamed expr (alias handling can be added later)
                //     expressions.push(self.expr_to_logical_expr(expr)?);
                // }
                _ => bail!("Unsupported SELECT item, Aliasing Column Still not supported"),
            }
        }

        Ok(expressions)
    }

    pub fn plan_table_factor(&self, table_factor: &TableFactor) -> Result<LogicalPlan> {
        match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = self.extract_table_name(name)?;
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());

                return match self.catalog.get(&table_name) {
                    Some(schema) => Ok(LogicalPlan::TableScan {
                        table_name,
                        alias: alias_name,
                        projected_schema: schema.clone(),
                    }),
                    _ => Err(anyhow!("Table : {} is not exist in database", table_name)),
                };
            }
            _ => Err(anyhow!("Only table scans are supported in FROM clause")),
        }
    }

    pub fn extract_table_name(&self, name: &ObjectName) -> Result<String> {
        if let Some(ObjectNamePart::Identifier(ident)) = name.0.first() {
            Ok(ident.value.clone())
        } else {
            Err(anyhow!("Invalid table name"))
        }
    }

    pub fn expr_to_logical_expr(&self, expr: &Expr) -> Result<LogicalExpr> {
        match expr {
            Expr::Identifier(ident) => Ok(LogicalExpr::Column {
                name: ident.value.clone(),
                table: None,
            }),
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    Ok(LogicalExpr::Column {
                        name: parts[1].value.clone(),
                        table: Some(parts[0].value.clone()),
                    })
                } else {
                    bail!("Complex compound identifiers not supported")
                }
            }
            Expr::Value(value) => {
                let scalar_value = match &value.value {
                    Value::SingleQuotedString(s) => ScalarValue::Utf8(s.clone()),
                    Value::Number(n, _) => {
                        if n.contains('.') {
                            ScalarValue::Float64(n.parse()?)
                        } else {
                            ScalarValue::Int64(n.parse()?)
                        }
                    }
                    Value::Boolean(b) => ScalarValue::Boolean(b.clone()),
                    Value::Null => ScalarValue::Null,
                    _ => bail!("Unsupported literal value"),
                };
                Ok(LogicalExpr::Literal(scalar_value))
            }
            Expr::BinaryOp { left, op, right } => Ok(LogicalExpr::BinaryExpr {
                left: Box::new(self.expr_to_logical_expr(left)?),
                op: op.clone(),
                right: Box::new(self.expr_to_logical_expr(right)?),
            }),
            Expr::Wildcard(_) => Ok(LogicalExpr::Wildcard { table: None }),
            _ => bail!("Unsupported expression: {:?}", expr),
        }
    }

    fn format_plan(&self, plan: &LogicalPlan, indent: usize) -> String {
        let indent_str = " ".repeat(indent);
        match plan {
            LogicalPlan::TableScan {
                table_name,
                alias,
                projected_schema,
            } => {
                let alias_str = alias
                    .as_ref()
                    .map(|a| format!(" AS {}", a))
                    .unwrap_or_default();
                format!(
                    "{}-> TableScan: {}{} projection=[{}]\n",
                    indent_str,
                    table_name,
                    alias_str,
                    projected_schema.join(", ")
                )
            }
            LogicalPlan::Projection { expressions, input } => {
                let expr_strs: Vec<String> =
                    expressions.iter().map(|e| format!("{:?}", e)).collect();
                format!(
                    "{}-> Projection: [{}]\n{}",
                    indent_str,
                    expr_strs.join(", "),
                    self.format_plan(input, indent + 1)
                )
            }
            LogicalPlan::Filter { predicate, input } => {
                format!(
                    "{}-> Filter: {:?}\n{}",
                    indent_str,
                    predicate,
                    self.format_plan(input, indent + 1)
                )
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                join_constraint,
            } => {
                let constraint_str = join_constraint
                    .as_ref()
                    .map(|c| format!(" ON {:?}", c))
                    .unwrap_or_default();
                format!(
                    "{}-> Join: {:?}{}\n{}{}",
                    indent_str,
                    join_type,
                    constraint_str,
                    self.format_plan(left, indent + 1),
                    self.format_plan(right, indent + 1)
                )
            }
        }
    }
}
