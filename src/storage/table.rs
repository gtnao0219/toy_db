use std::io::Write;
use std::sync::Arc;

use anyhow::Result;

use super::page::TablePage;
use super::tuple::Tuple;
use crate::buffer::BufferPoolManager;
use crate::catalog::Schema;

#[derive(Debug)]
pub struct Table<'a> {
    buffer_pool_manager: &'a Arc<BufferPoolManager>,
    schema: &'a Schema,
    pub first_block_number: usize,
    current_block_number: i32,
}

impl<'a> Table<'a> {
    pub fn new(
        buffer_pool_manager: &'a Arc<BufferPoolManager>,
        schema: &'a Schema,
        first_block_number: usize,
    ) -> Self {
        Table {
            buffer_pool_manager,
            schema,
            first_block_number,
            current_block_number: first_block_number as i32,
        }
    }
    pub fn create(
        buffer_pool_manager: &'a Arc<BufferPoolManager>,
        schema: &'a Schema,
    ) -> Result<Self> {
        let res = buffer_pool_manager.new_page(&TablePage::new().serialize()?)?;
        buffer_pool_manager.unpin_frame(res.1, true);
        Ok(Table {
            buffer_pool_manager,
            schema,
            first_block_number: res.0,
            current_block_number: res.0 as i32,
        })
    }
    pub fn insert_tuple(&self, tuple: Tuple) -> Result<()> {
        let mut block_number = self.first_block_number;
        loop {
            let res = self.buffer_pool_manager.fetch_page(block_number)?;
            let mut page = TablePage::deserialize(&res.1.read().unwrap(), self.schema)?;
            if page.insert_tuple(&tuple)? {
                let mut page_data = res.1.write().unwrap();
                page_data.clear();
                page_data.write_all(&page.serialize()?)?;
                self.buffer_pool_manager.unpin_frame(res.0, true);
                return Ok(());
            }
            if page.header.next_block_number == -1 {
                let mut new_page = TablePage::new();
                new_page.insert_tuple(&tuple)?;
                let res = self.buffer_pool_manager.new_page(&new_page.serialize()?)?;
                page.header.next_block_number = res.0 as i32;
                let mut page_data = res.2.write().unwrap();
                page_data.clear();
                page_data.write_all(&page.serialize()?)?;
                self.buffer_pool_manager.unpin_frame(res.1, true);
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
        let res = self
            .buffer_pool_manager
            .fetch_page(self.current_block_number as usize)
            .unwrap();
        let data = res.1.read().unwrap();
        if let Ok(page) = TablePage::deserialize(&data, self.schema) {
            self.current_block_number = page.header.next_block_number;
            self.buffer_pool_manager.unpin_frame(res.0, false);
            Some(page)
        } else {
            self.buffer_pool_manager.unpin_frame(res.0, false);
            None
        }
    }
}
