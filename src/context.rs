use std::collections::HashMap;

use sqlparser::{
    ast::{Expr, ObjectNamePart, Query, SelectItem, Statement, TableFactor},
    dialect::Dialect,
    parser::Parser,
};

pub struct ExecutionContext {
    pub dialect: Box<dyn Dialect>,
    pub catalog: HashMap<String, Vec<String>>,
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
            catalog,
        }
    }

    pub fn run(&self, sql: &str) {
        let ast = Parser::parse_sql(self.dialect.as_ref(), sql).unwrap();
        let stmt = &ast[0];

        // just for dummy storing the table name
        let mut table_select = String::new();
        let mut projection_select: Vec<String> = Vec::new();

        println!("Your query : {}", sql);

        match stmt {
            Statement::Query(query) => {
                let query: &Query = query.as_ref();
                let body = query.body.as_select().unwrap();

                // extract like from section, is that from just a plain table
                // or from a function, etc..
                for from in &body.from {
                    let relation = from.relation.clone();

                    match relation {
                        TableFactor::Table {
                            name,
                            alias,
                            args,
                            with_hints,
                            version,
                            with_ordinality,
                            partitions,
                            json_path,
                            sample,
                            index_hints,
                        } => {
                            let table_name = &name.0[0];
                            match table_name {
                                ObjectNamePart::Identifier(ident) => {
                                    table_select = ident.value.clone();
                                }
                                _ => todo!(""),
                            }
                        }
                        _ => todo!("Only support select from table from now on"),
                    }
                }

                // extract the column or value you need to return
                for projection in &body.projection {
                    match projection {
                        SelectItem::UnnamedExpr(expr) => match expr {
                            Expr::Identifier(ident) => {
                                projection_select.push(ident.value.clone());
                            }
                            Expr::Wildcard(ident) => {
                                projection_select.push("wildcard".to_string());
                            }
                            _ => todo!("Only support Identifier e.g. table name or column name"),
                        },
                        SelectItem::Wildcard(expr) => {}
                        _ => todo!("Only support raw column selection, without aliasing"),
                    }
                }
            }
            _ => todo!("Unsupported Query"),
        }

        print!("Projection : |");
        for project in projection_select {
            print!(" {} |", project);
        }
        println!("");
        println!("\tScan: {}", table_select);
    }
}
