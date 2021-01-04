use std::sync::Arc;

use crate::catalog::{Catalog, Column, Schema};
use crate::parser::Stmt;

pub trait Executor {
    fn execute(&self);
}

#[derive(Debug)]
pub struct CreateTableExecutor {
    pub stmt: Stmt,
    pub catalog: Arc<Catalog>,
}

impl Executor for CreateTableExecutor {
    fn execute(&self) {
        if let Stmt::CreateTableStmt(ast) = &self.stmt {
            self.catalog.create_table(
                &ast.table_name,
                &Schema {
                    columns: ast
                        .table_element_list
                        .iter()
                        .map(|table_element| Column {
                            name: table_element.column_name.clone(),
                            column_type: table_element.column_type.clone(),
                        })
                        .collect::<Vec<Column>>(),
                },
            );
        }
    }
}
