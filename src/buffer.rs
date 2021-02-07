use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use anyhow::Result;

use crate::disk::{DiskManager, PAGE_SIZE};

const POOL_SIZE: usize = 5;

#[derive(Debug)]
pub struct BufferPool {
    // block_number -> frame_id
    page_table: HashMap<usize, usize>,
    frames: Vec<Frame>,
    free_frame_ids: Vec<usize>,
}

#[derive(Debug)]
pub struct Frame {
    data: Arc<RwLock<Vec<u8>>>,
    dirty: bool,
    pin_count: u64,
    block_number: Option<usize>,
}

#[derive(Debug)]
pub struct BufferPoolManager {
    disk_manager: Arc<DiskManager>,
    buffer_pool: Mutex<BufferPool>,
}

impl BufferPoolManager {
    pub fn new(disk_manager: Arc<DiskManager>) -> Self {
        BufferPoolManager {
            disk_manager,
            buffer_pool: Mutex::new(BufferPool {
                page_table: HashMap::new(),
                frames: (0..POOL_SIZE)
                    .map(|_| Frame {
                        data: Arc::new(RwLock::new(vec![0u8; PAGE_SIZE])),
                        dirty: false,
                        pin_count: 0,
                        block_number: None,
                    })
                    .collect(),
                free_frame_ids: (0..POOL_SIZE).collect(),
            }),
        }
    }
    pub fn fetch_page(&self, block_number: usize) -> Result<(usize, Arc<RwLock<Vec<u8>>>)> {
        let mut buffer_pool = self.buffer_pool.lock().unwrap();
        // If the page(P) exists in the pool, pin it and return it immediately.
        if let Some(&frame_id) = buffer_pool.page_table.get(&block_number) {
            buffer_pool.frames[frame_id].pin_count += 1;
            return Ok((frame_id, buffer_pool.frames[frame_id].data.clone()));
        }
        let frame_id: usize;
        // If P does not exist in the pool, find a replacement page(R) from the free list.
        if let Some(last_v) = buffer_pool.free_frame_ids.last() {
            frame_id = *last_v;
            buffer_pool.free_frame_ids.pop();
        } else {
            // If the free list is empty, find a replacement page(R) from the replacer.
            // TODO: implement replacer. Following replacer is random one.
            loop {
                let mut rng = thread_rng();
                let v: usize = rng.gen_range(0..POOL_SIZE);
                if buffer_pool.frames[v].pin_count == 0 {
                    frame_id = v;
                    break;
                }
            }
            println!("evict {}", frame_id);
        }
        // If the R is dirty, write it back to the disk.
        if buffer_pool.frames[frame_id].dirty {
            // TODO: implement flush.
            let old_block_number = buffer_pool.frames[frame_id].block_number;
            println!(
                "flush frame_id: {}, block_number {:?}",
                frame_id, old_block_number
            );
            self.disk_manager.write_page(
                old_block_number.unwrap(),
                &buffer_pool.frames[frame_id].data.clone().read().unwrap(),
            )?;
        }
        if let Some(old_block_number) = buffer_pool.frames[frame_id].block_number {
            buffer_pool.page_table.remove(&old_block_number);
        }
        buffer_pool.page_table.insert(block_number, frame_id);
        // Delete R from the page table and insert P.
        let data = self.disk_manager.read_page(block_number)?;
        buffer_pool.frames[frame_id] = Frame {
            data: Arc::new(RwLock::new(data)),
            dirty: false,
            pin_count: 1,
            block_number: Some(block_number),
        };
        Ok((frame_id, buffer_pool.frames[frame_id].data.clone()))
    }
    pub fn unpin_frame(&self, frame_id: usize, dirty: bool) {
        let mut buffer_pool = self.buffer_pool.lock().unwrap();
        if buffer_pool.frames[frame_id].pin_count > 0 {
            buffer_pool.frames[frame_id].pin_count -= 1;
            buffer_pool.frames[frame_id].dirty = dirty;
        }
    }
    pub fn new_page(&self, data: &[u8]) -> Result<(usize, usize, Arc<RwLock<Vec<u8>>>)> {
        // Allocate page throught disk_manager.
        let block_number = self.disk_manager.write_new_page(&data)?;
        let res = self.fetch_page(block_number)?;
        Ok((block_number, res.0, res.1))
    }
    pub fn flush_all_pages(&self) -> Result<()> {
        let buffer_pool = self.buffer_pool.lock().unwrap();
        for frame in buffer_pool.frames.iter() {
            if let Some(block_number) = frame.block_number {
                self.disk_manager
                    .write_page(block_number, &frame.data.clone().read().unwrap())?;
            }
        }
        Ok(())
    }
}
