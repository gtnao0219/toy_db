use std::io;
use std::sync::{Arc, Mutex};

use crate::disk::DiskManager;
use crate::storage::table::Table;
use crate::storage::tuple::Tuple;
use crate::value::Value;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Schema {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Column {
    pub name: String,
    pub column_type: ColumnType,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub enum ColumnType {
    Int,
    Varchar,
}

#[derive(Debug)]
pub struct Catalog {
    disk_manager: Arc<DiskManager>,
    catalog_schema_map: CatalogSchemaMap,
    oid_counter: Arc<Mutex<usize>>,
}

impl Catalog {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        Catalog {
            disk_manager: disk_manager,
            catalog_schema_map: CatalogSchemaMap::new(),
            oid_counter: Arc::new(Mutex::new(0)),
        }
    }
    pub fn create_table(&self, table_name: &String, schema: &Schema) -> io::Result<()> {
        let table = Table::create(&self.disk_manager, schema)?;
        let new_oid: usize;
        {
            let mut v = self.oid_counter.lock().unwrap();
            *v = *v + 1;
            new_oid = *v;
        }
        let catalog_tables = Table::new(
            &self.disk_manager,
            &self.catalog_schema_map.catalog_table,
            1,
        );
        catalog_tables.insert_tuple(Tuple {
            values: vec![
                Value::Int(new_oid as i32),
                Value::Varchar(table_name.clone()),
            ],
        })?;
        let header = Table::new(&self.disk_manager, &self.catalog_schema_map.header, 0);
        header.insert_tuple(Tuple {
            values: vec![
                Value::Int(new_oid as i32),
                Value::Int(table.first_block_number as i32),
            ],
        })?;
        let catalog_attributes = Table::new(
            &self.disk_manager,
            &self.catalog_schema_map.catalog_attribute,
            2,
        );
        for c in schema.columns.iter() {
            catalog_attributes.insert_tuple(Tuple {
                values: vec![
                    Value::Int(new_oid as i32),
                    Value::Varchar(c.name.clone()),
                    Value::Varchar(match c.column_type {
                        ColumnType::Int => "integer".to_string(),
                        ColumnType::Varchar => "varchar".to_string(),
                    }),
                ],
            })?;
        }
        Ok(())
    }
    pub fn initialize(&self) -> io::Result<()> {
        let header_table = Table::create(&self.disk_manager, &self.catalog_schema_map.header)?;
        header_table.insert_tuple(Tuple {
            values: vec![Value::Int(0), Value::Int(1)],
        })?;
        header_table.insert_tuple(Tuple {
            values: vec![Value::Int(1), Value::Int(2)],
        })?;
        let catalog_table_table =
            Table::create(&self.disk_manager, &self.catalog_schema_map.catalog_table)?;
        catalog_table_table.insert_tuple(Tuple {
            values: vec![Value::Int(0), Value::Varchar("catalog_tables".to_string())],
        })?;
        catalog_table_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(1),
                Value::Varchar("catalog_attributes".to_string()),
            ],
        })?;
        let catalog_attribute_table = Table::create(
            &self.disk_manager,
            &self.catalog_schema_map.catalog_attribute,
        )?;
        catalog_attribute_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(0),
                Value::Varchar("object_id".to_string()),
                Value::Varchar("integer".to_string()),
            ],
        })?;
        catalog_attribute_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(0),
                Value::Varchar("name".to_string()),
                Value::Varchar("varchar".to_string()),
            ],
        })?;
        catalog_attribute_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(1),
                Value::Varchar("object_id".to_string()),
                Value::Varchar("integer".to_string()),
            ],
        })?;
        catalog_attribute_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(1),
                Value::Varchar("name".to_string()),
                Value::Varchar("varchar".to_string()),
            ],
        })?;
        catalog_attribute_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(1),
                Value::Varchar("type".to_string()),
                Value::Varchar("varchar".to_string()),
            ],
        })?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
struct CatalogSchemaMap {
    header: Schema,
    catalog_table: Schema,
    catalog_attribute: Schema,
}

impl CatalogSchemaMap {
    fn new() -> Self {
        CatalogSchemaMap {
            header: Schema {
                columns: vec![
                    Column {
                        name: "object_id".to_string(),
                        column_type: ColumnType::Int,
                    },
                    Column {
                        name: "first_block_number".to_string(),
                        column_type: ColumnType::Int,
                    },
                ],
            },
            catalog_table: Schema {
                columns: vec![
                    Column {
                        name: "object_id".to_string(),
                        column_type: ColumnType::Int,
                    },
                    Column {
                        name: "name".to_string(),
                        column_type: ColumnType::Varchar,
                    },
                ],
            },
            catalog_attribute: Schema {
                columns: vec![
                    Column {
                        name: "object_id".to_string(),
                        column_type: ColumnType::Int,
                    },
                    Column {
                        name: "name".to_string(),
                        column_type: ColumnType::Varchar,
                    },
                    Column {
                        name: "type".to_string(),
                        column_type: ColumnType::Varchar,
                    },
                ],
            },
        }
    }
}
