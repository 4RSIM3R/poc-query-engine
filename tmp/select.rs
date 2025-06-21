use std::collections::HashMap;

use anyhow::{Ok, Result, anyhow, bail};
use sqlparser::ast::{
    BinaryOperator, Expr, ObjectName, ObjectNamePart, Query, SelectItem, SetExpr, Statement,
    TableFactor, TableWithJoins, Value, JoinOperator, JoinConstraint, OrderByExpr, GroupByExpr,
    Offset, With, Cte, WildcardAdditionalOptions,
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
    SubqueryAlias {
        alias: String,
        input: Box<LogicalPlan>,
    },
    With {
        cte_tables: Vec<(String, LogicalPlan)>,
        input: Box<LogicalPlan>,
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

#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

#[derive(Debug, Clone)]
pub struct SortExpr {
    pub expr: LogicalExpr,
    pub asc: bool,
    pub nulls_first: bool,
}

pub struct Planner {
    pub catalog: HashMap<String, Vec<String>>,
    pub cte_tables: HashMap<String, LogicalPlan>,
}

impl Planner {
    pub fn new(catalog: HashMap<String, Vec<String>>) -> Self {
        Self { 
            catalog,
            cte_tables: HashMap::new(),
        }
    }

    pub fn generate(&mut self, stmt: &Statement) -> Result<LogicalPlan> {
        match stmt {
            Statement::Query(query) => self.plan_query(query),
            Statement::CreateTable(_table) => todo!("WIP: CREATE TABLE query"),
            Statement::Insert(_insert) => todo!("WIP: INSERT query"),
            Statement::Update {
                table: _,
                assignments: _,
                from: _,
                selection: _,
                returning: _,
                or: _,
            } => todo!("WIP: UPDATE query"),
            Statement::Delete(_delete) => todo!("WIP: DELETE query"),
            _ => Err(anyhow!("Unsupported Statement")),
        }
    }

    fn plan_query(&mut self, query: &Query) -> Result<LogicalPlan> {
        // Handle WITH clause (CTEs) first
        let mut plan = if let Some(with) = &query.with {
            self.plan_with_clause(with, query)?
        } else {
            self.plan_query_body(&query.body)?
        };

        // Handle ORDER BY
        if !query.order_by.is_empty() {
            let sort_exprs = self.plan_order_by(&query.order_by)?;
            plan = LogicalPlan::Sort {
                expressions: sort_exprs,
                input: Box::new(plan),
            };
        }

        // Handle LIMIT and OFFSET
        if query.limit.is_some() || query.offset.is_some() {
            let fetch = query.limit.as_ref().map(|l| self.extract_limit_value(l)).transpose()?;
            let skip = query.offset.as_ref().map(|o| self.extract_offset_value(o)).transpose()?;
            
            plan = LogicalPlan::Limit {
                skip,
                fetch,
                input: Box::new(plan),
            };
        }

        println!("{}", self.format_plan(&plan, 1));
        Ok(plan)
    }

    fn plan_query_body(&mut self, body: &SetExpr) -> Result<LogicalPlan> {
        match body {
            SetExpr::Select(select) => {
                // Handle FROM clause
                let mut plan = self.plan_from_clause(&select.from)?;

                // Handle WHERE clause
                if let Some(selection) = &select.selection {
                    let predicate = self.expr_to_logical_expr(selection)?;
                    plan = LogicalPlan::Filter {
                        predicate,
                        input: Box::new(plan),
                    };
                }

                // Handle GROUP BY
                if !select.group_by.is_empty() {
                    let group_expr = self.plan_group_by(&select.group_by)?;
                    let aggr_expr = self.extract_aggregate_expressions(&select.projection)?;
                    
                    plan = LogicalPlan::Aggregate {
                        group_expr,
                        aggr_expr,
                        input: Box::new(plan),
                    };
                }

                // Handle HAVING (would be implemented as another Filter after GROUP BY)
                if let Some(having) = &select.having {
                    let predicate = self.expr_to_logical_expr(having)?;
                    plan = LogicalPlan::Filter {
                        predicate,
                        input: Box::new(plan),
                    };
                }

                // Handle projection
                let projection_expressions = self.plan_projection_clause(&select.projection)?;
                plan = LogicalPlan::Projection {
                    expressions: projection_expressions,
                    input: Box::new(plan),
                };

                Ok(plan)
            }
            SetExpr::Query(query) => {
                // Handle nested queries (subqueries)
                self.plan_query(query)
            }
            SetExpr::SetOperation { op: _, set_quantifier: _, left: _, right: _ } => {
                todo!("Set operations (UNION, INTERSECT, EXCEPT) are not supported yet")
            }
            SetExpr::Values(_values) => todo!("VALUES clause not supported yet"),
            SetExpr::Insert(_statement) => todo!("INSERT in SET expression not supported"),
            SetExpr::Update(_statement) => todo!("UPDATE in SET expression not supported"),
            SetExpr::Delete(_statement) => todo!("DELETE in SET expression not supported"),
            SetExpr::Table(_table) => todo!("TABLE expression not supported"),
        }
    }

    fn plan_with_clause(&mut self, with: &With, query: &Query) -> Result<LogicalPlan> {
        let mut cte_plans = Vec::new();
        
        // Process each CTE
        for cte in &with.cte_tables {
            let cte_plan = self.plan_cte(cte)?;
            let cte_name = cte.alias.name.value.clone();
            
            // Store CTE in our catalog for reference
            self.cte_tables.insert(cte_name.clone(), cte_plan.clone());
            cte_plans.push((cte_name, cte_plan));
        }

        // Plan the main query
        let main_plan = self.plan_query_body(&query.body)?;

        // Wrap with WITH node
        Ok(LogicalPlan::With {
            cte_tables: cte_plans,
            input: Box::new(main_plan),
        })
    }

    fn plan_cte(&mut self, cte: &Cte) -> Result<LogicalPlan> {
        self.plan_query(&cte.query)
    }

    fn plan_from_clause(&mut self, from: &[TableWithJoins]) -> Result<LogicalPlan> {
        if from.is_empty() {
            return Err(anyhow!("FROM clause is required"));
        }

        let mut plan = self.plan_table_factor(&from[0].relation)?;

        // Handle JOINs
        for join in &from[0].joins {
            let right_plan = self.plan_table_factor(&join.relation)?;
            let join_type = self.convert_join_operator(&join.join_operator)?;
            let join_constraint = self.plan_join_constraint(&join.join_operator)?;

            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type,
                join_constraint,
            };
        }

        // Handle multiple table references (implicit cross joins)
        for table_with_joins in from.iter().skip(1) {
            let right_plan = self.plan_table_factor(&table_with_joins.relation)?;
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                join_type: JoinType::Cross,
                join_constraint: None,
            };
        }

        Ok(plan)
    }

    fn plan_projection_clause(&mut self, projection: &[SelectItem]) -> Result<Vec<LogicalExpr>> {
        let mut expressions = Vec::new();

        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    expressions.push(self.expr_to_logical_expr(expr)?);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let logical_expr = self.expr_to_logical_expr(expr)?;
                    expressions.push(LogicalExpr::Alias {
                        expr: Box::new(logical_expr),
                        name: alias.value.clone(),
                    });
                }
                SelectItem::QualifiedWildcard(object_name, _) => {
                    let table_name = self.extract_table_name(object_name)?;
                    expressions.push(LogicalExpr::Wildcard { 
                        table: Some(table_name) 
                    });
                }
                SelectItem::Wildcard(_) => {
                    expressions.push(LogicalExpr::Wildcard { table: None });
                }
            }
        }

        Ok(expressions)
    }

    fn plan_table_factor(&mut self, table_factor: &TableFactor) -> Result<LogicalPlan> {
        match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = self.extract_table_name(name)?;
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());

                // Check if it's a CTE first
                if let Some(cte_plan) = self.cte_tables.get(&table_name) {
                    let mut plan = cte_plan.clone();
                    if let Some(alias) = alias_name {
                        plan = LogicalPlan::SubqueryAlias {
                            alias,
                            input: Box::new(plan),
                        };
                    }
                    return Ok(plan);
                }

                // Check regular catalog
                match self.catalog.get(&table_name) {
                    Some(schema) => Ok(LogicalPlan::TableScan {
                        table_name,
                        alias: alias_name,
                        projected_schema: schema.clone(),
                    }),
                    None => Err(anyhow!("Table: {} does not exist in database", table_name)),
                }
            }
            TableFactor::Derived { lateral: _, subquery, alias } => {
                // Handle subqueries in FROM clause
                let subquery_plan = self.plan_query(subquery)?;
                let alias_name = alias.as_ref()
                    .map(|a| a.name.value.clone())
                    .ok_or_else(|| anyhow!("Subquery in FROM clause must have an alias"))?;

                Ok(LogicalPlan::SubqueryAlias {
                    alias: alias_name,
                    input: Box::new(subquery_plan),
                })
            }
            _ => Err(anyhow!("Unsupported table factor type")),
        }
    }

    fn plan_order_by(&mut self, order_by: &[OrderByExpr]) -> Result<Vec<SortExpr>> {
        let mut sort_exprs = Vec::new();
        for order_expr in order_by {
            let expr = self.expr_to_logical_expr(&order_expr.expr)?;
            sort_exprs.push(SortExpr {
                expr,
                asc: order_expr.asc.unwrap_or(true),
                nulls_first: order_expr.nulls_first.unwrap_or(false),
            });
        }
        Ok(sort_exprs)
    }

    fn plan_group_by(&mut self, group_by: &[GroupByExpr]) -> Result<Vec<LogicalExpr>> {
        let mut group_exprs = Vec::new();
        for group_expr in group_by {
            match group_expr {
                GroupByExpr::Expr(expr) => {
                    group_exprs.push(self.expr_to_logical_expr(expr)?);
                }
                _ => bail!("Complex GROUP BY expressions not supported yet"),
            }
        }
        Ok(group_exprs)
    }

    fn extract_aggregate_expressions(&mut self, projection: &[SelectItem]) -> Result<Vec<LogicalExpr>> {
        let mut aggr_exprs = Vec::new();
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    if self.is_aggregate_expression(expr) {
                        aggr_exprs.push(self.expr_to_logical_expr(expr)?);
                    }
                }
                SelectItem::ExprWithAlias { expr, alias: _ } => {
                    if self.is_aggregate_expression(expr) {
                        aggr_exprs.push(self.expr_to_logical_expr(expr)?);
                    }
                }
                _ => {}
            }
        }
        Ok(aggr_exprs)
    }

    fn is_aggregate_expression(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                matches!(func.name.to_string().to_uppercase().as_str(), 
                    "COUNT" | "SUM" | "MIN" | "MAX" | "AVG")
            }
            _ => false,
        }
    }

    fn convert_join_operator(&self, join_op: &JoinOperator) -> Result<JoinType> {
        match join_op {
            JoinOperator::Inner(_) => Ok(JoinType::Inner),
            JoinOperator::LeftOuter(_) => Ok(JoinType::Left),
            JoinOperator::RightOuter(_) => Ok(JoinType::Right),
            JoinOperator::FullOuter(_) => Ok(JoinType::Full),
            JoinOperator::CrossJoin => Ok(JoinType::Cross),
            _ => Err(anyhow!("Unsupported join type")),
        }
    }

    fn plan_join_constraint(&mut self, join_op: &JoinOperator) -> Result<Option<LogicalExpr>> {
        match join_op {
            JoinOperator::Inner(constraint) |
            JoinOperator::LeftOuter(constraint) |
            JoinOperator::RightOuter(constraint) |
            JoinOperator::FullOuter(constraint) => {
                match constraint {
                    JoinConstraint::On(expr) => {
                        Ok(Some(self.expr_to_logical_expr(expr)?))
                    }
                    JoinConstraint::Using(_) => {
                        todo!("USING constraint not implemented yet")
                    }
                    JoinConstraint::Natural => {
                        todo!("NATURAL JOIN not implemented yet")
                    }
                    JoinConstraint::None => Ok(None),
                }
            }
            JoinOperator::CrossJoin => Ok(None),
            _ => Err(anyhow!("Unsupported join constraint")),
        }
    }

    fn extract_limit_value(&self, limit: &Expr) -> Result<usize> {
        match limit {
            Expr::Value(Value::Number(n, _)) => {
                Ok(n.parse()?)
            }
            _ => Err(anyhow!("LIMIT must be a number")),
        }
    }

    fn extract_offset_value(&self, offset: &Offset) -> Result<usize> {
        match &offset.value {
            Expr::Value(Value::Number(n, _)) => {
                Ok(n.parse()?)
            }
            _ => Err(anyhow!("OFFSET must be a number")),
        }
    }

    pub fn extract_table_name(&self, name: &ObjectName) -> Result<String> {
        if let Some(ObjectNamePart::Identifier(ident)) = name.0.first() {
            Ok(ident.value.clone())
        } else {
            Err(anyhow!("Invalid table name"))
        }
    }

    pub fn expr_to_logical_expr(&mut self, expr: &Expr) -> Result<LogicalExpr> {
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
                let scalar_value = match value {
                    Value::SingleQuotedString(s) => ScalarValue::Utf8(s.clone()),
                    Value::Number(n, _) => {
                        if n.contains('.') {
                            ScalarValue::Float64(n.parse()?)
                        } else {
                            ScalarValue::Int64(n.parse()?)
                        }
                    }
                    Value::Boolean(b) => ScalarValue::Boolean(*b),
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
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                match func_name.as_str() {
                    "COUNT" | "SUM" | "MIN" | "MAX" | "AVG" => {
                        let aggregate_fn = match func_name.as_str() {
                            "COUNT" => AggregateFunction::Count,
                            "SUM" => AggregateFunction::Sum,
                            "MIN" => AggregateFunction::Min,
                            "MAX" => AggregateFunction::Max,
                            "AVG" => AggregateFunction::Avg,
                            _ => unreachable!(),
                        };
                        
                        let mut args = Vec::new();
                        for arg in &func.args {
                            match arg {
                                sqlparser::ast::FunctionArg::Named { name: _, arg } => {
                                    match arg {
                                        sqlparser::ast::FunctionArgExpr::Expr(e) => {
                                            args.push(self.expr_to_logical_expr(e)?);
                                        }
                                        sqlparser::ast::FunctionArgExpr::Wildcard => {
                                            args.push(LogicalExpr::Wildcard { table: None });
                                        }
                                        _ => bail!("Unsupported function argument"),
                                    }
                                }
                                sqlparser::ast::FunctionArg::Unnamed(arg) => {
                                    match arg {
                                        sqlparser::ast::FunctionArgExpr::Expr(e) => {
                                            args.push(self.expr_to_logical_expr(e)?);
                                        }
                                        sqlparser::ast::FunctionArgExpr::Wildcard => {
                                            args.push(LogicalExpr::Wildcard { table: None });
                                        }
                                        _ => bail!("Unsupported function argument"),
                                    }
                                }
                            }
                        }

                        Ok(LogicalExpr::AggregateFunction {
                            fun: aggregate_fn,
                            args,
                            distinct: func.distinct,
                        })
                    }
                    _ => {
                        // Handle as scalar function
                        let mut args = Vec::new();
                        for arg in &func.args {
                            match arg {
                                sqlparser::ast::FunctionArg::Named { name: _, arg } => {
                                    match arg {
                                        sqlparser::ast::FunctionArgExpr::Expr(e) => {
                                            args.push(self.expr_to_logical_expr(e)?);
                                        }
                                        _ => bail!("Unsupported function argument"),
                                    }
                                }
                                sqlparser::ast::FunctionArg::Unnamed(arg) => {
                                    match arg {
                                        sqlparser::ast::FunctionArgExpr::Expr(e) => {
                                            args.push(self.expr_to_logical_expr(e)?);
                                        }
                                        _ => bail!("Unsupported function argument"),
                                    }
                                }
                            }
                        }

                        Ok(LogicalExpr::ScalarFunction {
                            fun: func_name,
                            args,
                        })
                    }
                }
            }
            Expr::Subquery(subquery) => {
                let subquery_plan = self.plan_query(subquery)?;
                Ok(LogicalExpr::Subquery {
                    subquery: Box::new(subquery_plan),
                })
            }
            Expr::Wildcard => Ok(LogicalExpr::Wildcard { table: None }),
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
            LogicalPlan::Limit { skip, fetch, input } => {
                let limit_str = match (skip, fetch) {
                    (Some(s), Some(f)) => format!("OFFSET {} LIMIT {}", s, f),
                    (Some(s), None) => format!("OFFSET {}", s),
                    (None, Some(f)) => format!("LIMIT {}", f),
                    (None, None) => "".to_string(),
                };
                format!(
                    "{}-> Limit: {}\n{}",
                    indent_str,
                    limit_str,
                    self.format_plan(input, indent + 1)
                )
            }
            LogicalPlan::Sort { expressions, input } => {
                let sort_strs: Vec<String> = expressions
                    .iter()
                    .map(|e| {
                        let order = if e.asc { "ASC" } else { "DESC" };
                        format!("{:?} {}", e.expr, order)
                    })
                    .collect();
                format!(
                    "{}-> Sort: [{}]\n{}",
                    indent_str,
                    sort_strs.join(", "),
                    self.format_plan(input, indent + 1)
                )
            }
            LogicalPlan::Aggregate {
                group_expr,
                aggr_expr,
                input,
            } => {
                let group_strs: Vec<String> =
                    group_expr.iter().map(|e| format!("{:?}", e)).collect();
                let aggr_strs: Vec<String> =
                    aggr_expr.iter().map(|e| format!("{:?}", e)).collect();
                format!(
                    "{}-> Aggregate: group=[{}] agg=[{}]\n{}",
                    indent_str,
                    group_strs.join(", "),
                    aggr_strs.join(", "),
                    self.format_plan(input, indent + 1)
                )
            }
            LogicalPlan::SubqueryAlias { alias, input } => {
                format!(
                    "{}-> SubqueryAlias: {}\n{}",
                    indent_str,
                    alias,
                    self.format_plan(input, indent + 1)
                )
            }
            LogicalPlan::With { cte_tables, input } => {
                let mut result = format!("{}-> With: [", indent_str);
                for (name, _) in cte_tables {
                    result.push_str(&format!("{}, ", name));
                }
                result.push_str("]\n");
                result.push_str(&self.format_plan(input, indent + 1));
                result
            }
        }
    }
}