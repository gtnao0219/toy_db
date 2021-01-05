use std::cmp;
use std::sync::{Arc, Mutex};

use anyhow;

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
        let catalog_schema_map = CatalogSchemaMap::new();
        Catalog {
            disk_manager: disk_manager,
            catalog_schema_map: catalog_schema_map,
            oid_counter: Arc::new(Mutex::new(0)),
        }
    }
    pub fn create_table(&self, table_name: &String, schema: &Schema) -> anyhow::Result<()> {
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
                        ColumnType::Int => "int".to_string(),
                        ColumnType::Varchar => "varchar".to_string(),
                    }),
                ],
            })?;
        }
        Ok(())
    }
    pub fn get_schema(&self, table_name: &String) -> anyhow::Result<Option<Schema>> {
        match self.get_oid(table_name)? {
            Some(oid) => {
                let table = Table::new(
                    &self.disk_manager,
                    &self.catalog_schema_map.catalog_attribute,
                    2,
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
                Ok(Some(Schema { columns: columns }))
            }
            None => Ok(None),
        }
    }
    pub fn get_first_block_number(&self, table_name: &String) -> anyhow::Result<Option<usize>> {
        match self.get_oid(table_name)? {
            Some(oid) => {
                let table = Table::new(&self.disk_manager, &self.catalog_schema_map.header, 0);
                for page in table {
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
    pub fn get_oid(&self, table_name: &String) -> anyhow::Result<Option<usize>> {
        let table = Table::new(
            &self.disk_manager,
            &self.catalog_schema_map.catalog_table,
            1,
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
    pub fn initialize(&self) -> anyhow::Result<()> {
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
    pub fn set_oid(&self) {
        let header = Table::new(&self.disk_manager, &self.catalog_schema_map.header, 0);
        let mut max: usize = 0;
        for page in header {
            for tuple in page.tuples.iter() {
                if let Value::Int(v) = tuple.values[0] {
                    max = cmp::max(max, v as usize);
                }
            }
        }
        {
            let mut v = self.oid_counter.lock().unwrap();
            *v = max;
        }
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
