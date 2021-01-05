use std::sync::Arc;

use anyhow::Result;

use super::page::TablePage;
use super::tuple::Tuple;
use crate::catalog::Schema;
use crate::disk::DiskManager;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct Table<'a> {
    disk_manager: &'a Arc<DiskManager>,
    schema: &'a Schema,
    pub first_block_number: usize,
    current_block_number: i32,
}

impl<'a> Table<'a> {
    pub fn new(
        disk_manager: &'a Arc<DiskManager>,
        schema: &'a Schema,
        first_block_number: usize,
    ) -> Self {
        Table {
            disk_manager,
            schema,
            first_block_number,
            current_block_number: first_block_number as i32,
        }
    }
    pub fn create(disk_manager: &'a Arc<DiskManager>, schema: &'a Schema) -> Result<Self> {
        let first_block_number = disk_manager.write_new_page(&TablePage::new().serialize()?)?;
        Ok(Table {
            disk_manager,
            schema,
            first_block_number,
            current_block_number: first_block_number as i32,
        })
    }
    pub fn insert_tuple(&self, tuple: Tuple) -> Result<()> {
        let mut block_number = self.first_block_number;
        loop {
            let mut page =
                TablePage::deserialize(&self.disk_manager.read_page(block_number)?, self.schema)?;
            if page.insert_tuple(&tuple)? {
                self.disk_manager
                    .write_page(block_number, &page.serialize()?)?;
                return Ok(());
            }
            if page.header.next_block_number == -1 {
                let mut new_page = TablePage::new();
                new_page.insert_tuple(&tuple)?;
                let next_block_number = self.disk_manager.write_new_page(&new_page.serialize()?)?;
                page.header.next_block_number = next_block_number as i32;
                self.disk_manager
                    .write_page(block_number, &page.serialize()?)?;
            } else {
                block_number = page.header.next_block_number as usize;
            }
        }
    }
}

impl<'a> Iterator for Table<'a> {
    type Item = TablePage;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_block_number == -1 {
            return None;
        }
        // TODO: remove unwrap
        let page = TablePage::deserialize(
            &self
                .disk_manager
                .read_page(self.current_block_number as usize)
                .unwrap(),
            self.schema,
        )
        .unwrap();
        self.current_block_number = page.header.next_block_number;
        Some(page)
    }
}
