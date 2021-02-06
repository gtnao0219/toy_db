use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use anyhow::Result;

pub const PAGE_SIZE: usize = 4096;
pub const DATAFILE_NAME: &str = "data";

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
pub struct DiskManager {
    home_dir: String,
    datafile_path_buf: PathBuf,
}

impl DiskManager {
    pub fn new(home_dir: String) -> Self {
        let datafile_path_buf = Path::new(&home_dir).join(DATAFILE_NAME);
        Self {
            home_dir,
            datafile_path_buf,
        }
    }
    pub fn init_data_file(&self) -> Result<()> {
        let datafile_path = self.datafile_path_buf.as_path();
        File::create(datafile_path)?;
        Ok(())
    }
    pub fn write_page(&self, block_number: usize, data: &[u8]) -> Result<()> {
        let datafile_path = self.datafile_path_buf.as_path();
        let mut file = OpenOptions::new().write(true).open(datafile_path)?;
        file.seek(SeekFrom::Start((block_number * PAGE_SIZE) as u64))?;
        file.write_all(data)?;
        Ok(())
    }

    pub fn write_new_page(&self, data: &[u8]) -> Result<usize> {
        let datafile_path = self.datafile_path_buf.as_path();
        let metadata = datafile_path.metadata()?;
        // TODO: get block number safely.
        let block_number = (metadata.len() / PAGE_SIZE as u64) as usize;
        self.write_page(block_number, data)?;
        Ok(block_number)
    }

    pub fn read_page(&self, block_number: usize) -> Result<Vec<u8>> {
        let datafile_path = self.datafile_path_buf.as_path();
        let mut file = File::open(datafile_path)?;
        file.seek(SeekFrom::Start((block_number * PAGE_SIZE) as u64))?;
        let mut buf = vec![0u8; PAGE_SIZE];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use crate::disk::{DiskManager, DATAFILE_NAME, PAGE_SIZE};
    use anyhow::Result;
    use std::fs;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    #[test]
    fn init_data_file_create() -> Result<()> {
        let disk_manager = DiskManager::new("tmp/".to_string());
        let datafile_path_buf = Path::new("tmp/").join(DATAFILE_NAME);
        let datafile_path = datafile_path_buf.as_path();
        if datafile_path.exists() {
            fs::remove_file(datafile_path)?;
        }
        disk_manager.init_data_file()?;
        assert!(datafile_path.exists());
        Ok(())
    }
    #[test]
    fn init_new_data_file_truncate() -> Result<()> {
        let disk_manager = DiskManager::new("tmp/".to_string());
        let datafile_path_buf = Path::new("tmp/").join(DATAFILE_NAME);
        let datafile_path = datafile_path_buf.as_path();
        if datafile_path.exists() {
            fs::remove_file(datafile_path)?;
        }
        let mut file = File::create(datafile_path)?;
        write!(file, "foo")?;
        disk_manager.init_data_file()?;
        assert_eq!(datafile_path.metadata()?.len(), 0);
        Ok(())
    }
    #[test]
    fn write_and_read_page() -> Result<()> {
        let disk_manager = DiskManager::new("tmp/".to_string());
        disk_manager.init_data_file()?;
        disk_manager.write_page(0, &[65u8; PAGE_SIZE])?;
        disk_manager.write_page(1, &[66u8; PAGE_SIZE])?;
        disk_manager.write_page(2, &[67u8; PAGE_SIZE])?;
        let data = disk_manager.read_page(1)?;
        assert_eq!(data, vec![66u8; PAGE_SIZE]);
        Ok(())
    }
}
