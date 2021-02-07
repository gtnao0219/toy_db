use std::cmp;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::Result;

use crate::buffer::BufferPoolManager;
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
    buffer_pool_manager: Arc<BufferPoolManager>,
    catalog_schema_map: CatalogSchemaMap,
    oid_counter: Arc<AtomicUsize>,
}

const HEADER_FIRST_BLOCK_NUMBER: usize = 0;
const CATALOG_TABLE_FIRST_BLOCK_NUMBER: usize = 1;
const CATALOG_ATTRIBUTE_FIRST_BLOCK_NUMBER: usize = 2;
const HEADER_OID: usize = 0;
const CATALOG_TABLE_OID: usize = 1;
const CATALOG_ATTRIBUTE_OID: usize = 1;

impl Catalog {
    pub fn new(buffer_pool_manager: Arc<BufferPoolManager>) -> Self {
        let catalog_schema_map = CatalogSchemaMap::new();
        Catalog {
            buffer_pool_manager,
            catalog_schema_map,
            oid_counter: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn initialize(&self) -> Result<()> {
        let header_table =
            Table::create(&self.buffer_pool_manager, &self.catalog_schema_map.header)?;
        header_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(HEADER_OID as i32),
                Value::Int(HEADER_FIRST_BLOCK_NUMBER as i32),
            ],
        })?;
        header_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(CATALOG_TABLE_OID as i32),
                Value::Int(CATALOG_TABLE_FIRST_BLOCK_NUMBER as i32),
            ],
        })?;
        header_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(CATALOG_ATTRIBUTE_OID as i32),
                Value::Int(CATALOG_ATTRIBUTE_FIRST_BLOCK_NUMBER as i32),
            ],
        })?;
        let catalog_table_table = Table::create(
            &self.buffer_pool_manager,
            &self.catalog_schema_map.catalog_table,
        )?;
        catalog_table_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(CATALOG_TABLE_OID as i32),
                Value::Varchar("catalog_tables".to_string()),
            ],
        })?;
        catalog_table_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(CATALOG_ATTRIBUTE_OID as i32),
                Value::Varchar("catalog_attributes".to_string()),
            ],
        })?;
        let catalog_attribute_table = Table::create(
            &self.buffer_pool_manager,
            &self.catalog_schema_map.catalog_attribute,
        )?;
        catalog_attribute_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(CATALOG_TABLE_OID as i32),
                Value::Varchar("object_id".to_string()),
                Value::Varchar("integer".to_string()),
            ],
        })?;
        catalog_attribute_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(CATALOG_TABLE_OID as i32),
                Value::Varchar("name".to_string()),
                Value::Varchar("varchar".to_string()),
            ],
        })?;
        catalog_attribute_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(CATALOG_ATTRIBUTE_OID as i32),
                Value::Varchar("object_id".to_string()),
                Value::Varchar("integer".to_string()),
            ],
        })?;
        catalog_attribute_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(CATALOG_ATTRIBUTE_OID as i32),
                Value::Varchar("name".to_string()),
                Value::Varchar("varchar".to_string()),
            ],
        })?;
        catalog_attribute_table.insert_tuple(Tuple {
            values: vec![
                Value::Int(CATALOG_ATTRIBUTE_OID as i32),
                Value::Varchar("type".to_string()),
                Value::Varchar("varchar".to_string()),
            ],
        })?;
        self.buffer_pool_manager.flush_all_pages()?;
        Ok(())
    }
    pub fn bootstrap(&self) {
        self.set_oid();
    }
    fn set_oid(&self) {
        let header = Table::new(
            &self.buffer_pool_manager,
            &self.catalog_schema_map.header,
            HEADER_FIRST_BLOCK_NUMBER,
        );
        let mut max: usize = 0;
        for page in header {
            for tuple in page.tuples.iter() {
                if let Value::Int(v) = tuple.values[0] {
                    max = cmp::max(max, v as usize);
                }
            }
        }
        self.oid_counter.store(max, Ordering::Relaxed);
    }
    pub fn create_table(&self, table_name: &str, schema: &Schema) -> Result<()> {
        // TODO: validations
        // table name dup, attribute name dup
        // create table page.
        let table = Table::create(&self.buffer_pool_manager, schema)?;
        // insert into catalog_table.
        self.oid_counter.fetch_add(1, Ordering::Relaxed);
        let new_oid = self.oid_counter.load(Ordering::Relaxed);
        let catalog_tables = Table::new(
            &self.buffer_pool_manager,
            &self.catalog_schema_map.catalog_table,
            CATALOG_TABLE_FIRST_BLOCK_NUMBER,
        );
        catalog_tables.insert_tuple(Tuple {
            values: vec![
                Value::Int(new_oid as i32),
                Value::Varchar(table_name.to_string()),
            ],
        })?;
        // insert into header.
        let header = Table::new(
            &self.buffer_pool_manager,
            &self.catalog_schema_map.header,
            HEADER_FIRST_BLOCK_NUMBER,
        );
        header.insert_tuple(Tuple {
            values: vec![
                Value::Int(new_oid as i32),
                Value::Int(table.first_block_number as i32),
            ],
        })?;
        // insert into catalog_attribute.
        let catalog_attributes = Table::new(
            &self.buffer_pool_manager,
            &self.catalog_schema_map.catalog_attribute,
            CATALOG_ATTRIBUTE_FIRST_BLOCK_NUMBER,
        );
        for c in schema.columns.iter() {
            catalog_attributes.insert_tuple(Tuple {
                values: vec![
                    Value::Int(new_oid as i32),
                    Value::Varchar(c.name.clone()),
                    Value::Varchar(match c.column_type {
                        ColumnType::Int => "int".to_string(),
                        ColumnType::Varchar => "varchar".to_string(),
                    }),
                ],
            })?;
        }
        Ok(())
    }
    pub fn get_schema(&self, table_name: &str) -> Result<Option<Schema>> {
        match self.get_oid(table_name)? {
            Some(oid) => {
                let table = Table::new(
                    &self.buffer_pool_manager,
                    &self.catalog_schema_map.catalog_attribute,
                    CATALOG_ATTRIBUTE_FIRST_BLOCK_NUMBER,
                );
                let mut columns = Vec::new();
                for page in table {
                    for tuple in page.tuples.iter() {
                        if let Value::Int(v) = tuple.values[0] {
                            if v as usize == oid {
                                if let Value::Varchar(name) = &tuple.values[1] {
                                    if let Value::Varchar(column_type_string) = &tuple.values[2] {
                                        columns.push(Column {
                                            name: name.clone(),
                                            column_type: match &**column_type_string {
                                                "int" => ColumnType::Int,
                                                "varchar" => ColumnType::Varchar,
                                                _ => ColumnType::Varchar,
                                            },
                                        })
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(Some(Schema { columns }))
            }
            None => Ok(None),
        }
    }
    pub fn get_first_block_number(&self, table_name: &str) -> Result<Option<usize>> {
        match self.get_oid(table_name)? {
            Some(oid) => {
                let header = Table::new(
                    &self.buffer_pool_manager,
                    &self.catalog_schema_map.header,
                    HEADER_FIRST_BLOCK_NUMBER,
                );
                for page in header {
                    for tuple in page.tuples.iter() {
                        if let Value::Int(v) = tuple.values[0] {
                            if v as usize == oid {
                                if let Value::Int(first_block_number) = tuple.values[1] {
                                    return Ok(Some(first_block_number as usize));
                                }
                            }
                        }
                    }
                }
                Ok(None)
            }
            None => Ok(None),
        }
    }
    pub fn get_oid(&self, table_name: &str) -> Result<Option<usize>> {
        let table = Table::new(
            &self.buffer_pool_manager,
            &self.catalog_schema_map.catalog_table,
            CATALOG_TABLE_FIRST_BLOCK_NUMBER,
        );
        for page in table {
            for tuple in page.tuples.iter() {
                if let Value::Varchar(v) = &tuple.values[1] {
                    if v == table_name {
                        if let Value::Int(oid) = tuple.values[0] {
                            return Ok(Some(oid as usize));
                        }
                    }
                }
            }
        }
        Ok(None)
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
