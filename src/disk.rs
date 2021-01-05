use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::Result;

pub const PAGE_SIZE: usize = 4096;
const DATAFILE_NAME: &str = "data";

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct DiskManager {
    home_dir: String,
}

impl DiskManager {
    pub fn new(home_dir: String) -> Self {
        Self { home_dir }
    }
    pub fn write_page(&self, block_number: usize, data: &[u8]) -> Result<()> {
        let file_path_buf = Path::new(&self.home_dir).join(DATAFILE_NAME);
        let file_path = file_path_buf.as_path();
        if !file_path.exists() {
            File::create(file_path)?;
        }
        let mut file = OpenOptions::new().write(true).open(file_path)?;
        file.seek(SeekFrom::Start((block_number * PAGE_SIZE) as u64))?;
        file.write_all(data)?;
        Ok(())
    }

    pub fn write_new_page(&self, data: &[u8]) -> Result<usize> {
        let file_path_buf = Path::new(&self.home_dir).join(DATAFILE_NAME);
        let file_path = file_path_buf.as_path();
        let metadata = file_path.metadata()?;
        let block_number = (metadata.len() / PAGE_SIZE as u64) as usize;
        self.write_page(block_number, data)?;
        Ok(block_number)
    }

    pub fn read_page(&self, block_number: usize) -> Result<Vec<u8>> {
        let file_path_buf = Path::new(&self.home_dir).join(DATAFILE_NAME);
        let file_path = file_path_buf.as_path();
        let mut file = File::open(file_path)?;
        file.seek(SeekFrom::Start((block_number * PAGE_SIZE) as u64))?;
        let mut buf = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use crate::disk::{DiskManager, PAGE_SIZE};
    use anyhow::Result;
    #[test]
    fn write_and_read_page() -> Result<()> {
        let disk_manager = DiskManager::new("tmp/".to_string());
        disk_manager.write_page(0, &[65u8; PAGE_SIZE])?;
        disk_manager.write_page(1, &[66u8; PAGE_SIZE])?;
        disk_manager.write_page(2, &[67u8; PAGE_SIZE])?;
        let data = disk_manager.read_page(1)?;
        assert_eq!(data, vec![66u8; PAGE_SIZE]);
        Ok(())
    }
}
