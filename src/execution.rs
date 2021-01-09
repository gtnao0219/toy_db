use std::sync::Arc;

use anyhow::Result;

use crate::catalog::{Catalog, Column, Schema};
use crate::disk::DiskManager;
use crate::parser::Stmt;
use crate::storage::table::Table;
use crate::storage::tuple::Tuple;

pub trait Executor {
    fn execute(&self) -> Result<String>;
}

#[derive(Debug)]
pub struct CreateTableExecutor {
    pub stmt: Stmt,
    pub catalog: Arc<Catalog>,
}

impl Executor for CreateTableExecutor {
    fn execute(&self) -> Result<String> {
        if let Stmt::CreateTableStmt(ast) = &self.stmt {
            if self
                .catalog
                .get_first_block_number(&ast.table_name)?
                .is_some()
            {
                return Err(anyhow!("Table({}) exists\n", ast.table_name));
            } else {
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
                )?;
            }
        }
        Ok("Query OK\n".to_string())
    }
}

#[derive(Debug)]
pub struct InsertExecutor {
    pub stmt: Stmt,
    pub catalog: Arc<Catalog>,
    pub disk_manager: Arc<DiskManager>,
}

impl Executor for InsertExecutor {
    fn execute(&self) -> Result<String> {
        if let Stmt::InsertStmt(ast) = &self.stmt {
            if let Some(schema) = self.catalog.get_schema(&ast.table_name)? {
                if let Some(first_block_number) =
                    self.catalog.get_first_block_number(&ast.table_name)?
                {
                    let table = Table::new(&self.disk_manager, &schema, first_block_number);
                    table.insert_tuple(Tuple {
                        values: ast.values.clone(),
                    })?;
                } else {
                    return Err(anyhow!("Table({}) not found\n", ast.table_name));
                }
            } else {
                return Err(anyhow!("Table({}) not found\n", ast.table_name));
            }
        }
        Ok("Query OK\n".to_string())
    }
}

#[derive(Debug)]
pub struct SelectExecutor {
    pub stmt: Stmt,
    pub catalog: Arc<Catalog>,
    pub disk_manager: Arc<DiskManager>,
}

impl Executor for SelectExecutor {
    fn execute(&self) -> Result<String> {
        let mut res = String::new();
        if let Stmt::SelectStmt(ast) = &self.stmt {
            if let Some(schema) = self.catalog.get_schema(&ast.table_name)? {
                if let Some(first_block_number) =
                    self.catalog.get_first_block_number(&ast.table_name)?
                {
                    let table = Table::new(&self.disk_manager, &schema, first_block_number);
                    for page in table {
                        for tuple in page.tuples.iter() {
                            for (i, _) in schema.columns.iter().enumerate() {
                                if i == 0 {
                                    res = format!("{}{}", res, &tuple.values[i]);
                                } else {
                                    res = format!("{}, {}", res, &tuple.values[i]);
                                }
                            }
                            res = format!("{}\n", res);
                        }
                    }
                } else {
                    return Err(anyhow!("Table({}) not found\n", ast.table_name));
                }
            } else {
                return Err(anyhow!("Table({}) not found\n", ast.table_name));
            }
        }
        Ok(res)
    }
}
