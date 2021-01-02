use std::fs::{File, OpenOptions};
use std::io;
use std::io::{SeekFrom, Seek, Write, Read};
use std::path::Path;

use crate::common::config::{Config};
use super::page::table_page::TABLE_PAGE_SIZE;

pub const DATAFILE_NAME: &str = "data";

#[derive(Debug, Clone, PartialEq, PartialOrd, Hash, Eq, Ord)]
pub struct DiskManager<'a> {
  config: &'a Config
}

impl <'a> DiskManager<'a> {
  pub fn write_page(&self, block_number: usize, data: &[u8]) -> io::Result<()> {
    let file_path_buf = Path::new(&self.config.data_dir).join(DATAFILE_NAME);
    let file_path = file_path_buf.as_path();
    if !file_path.exists() {
      File::create(file_path)?;
    }
    let mut file = OpenOptions::new().write(true).open(file_path)?;
    file.seek(SeekFrom::Start((block_number * TABLE_PAGE_SIZE) as u64))?;
    file.write(data)?;
    Ok(())
  }

  pub fn write_new_page(&self, data: &[u8]) -> io::Result<usize> {
    let file_path_buf = Path::new(&self.config.data_dir).join(DATAFILE_NAME);
    let file_path = file_path_buf.as_path();
    let metadata = file_path.metadata()?;
    let prev_block_number = (metadata.len() / TABLE_PAGE_SIZE as u64) as usize;
    self.write_page(prev_block_number + 1, data)?;
    Ok(prev_block_number + 1)
  }

  pub fn read_page(&self, block_number: usize) -> io::Result<Vec<u8>> {
    let file_path_buf = Path::new(&self.config.data_dir).join(DATAFILE_NAME);
    let file_path = file_path_buf.as_path();
    let mut file = File::open(file_path)?;
    file.seek(SeekFrom::Start((block_number * TABLE_PAGE_SIZE) as u64))?;
    let mut buf = vec![0u8; TABLE_PAGE_SIZE];
    file.read_exact(&mut buf)?;
    Ok(buf)
  }
}


#[cfg(test)]
mod tests {
  use std::io;
  use crate::storage::disk::{DiskManager};
  use crate::storage::page::table_page::{TABLE_PAGE_SIZE};
  use crate::common::config::{Config};
  #[test]
  fn write_and_read_page() -> io::Result<()> {
    let disk_manager = DiskManager {
      config: &Config {
        data_dir: "tmp/".to_string()
      }
    };
    disk_manager.write_page(0, &[65u8; TABLE_PAGE_SIZE])?;
    disk_manager.write_page(1, &[66u8; TABLE_PAGE_SIZE])?;
    disk_manager.write_page(2, &[67u8; TABLE_PAGE_SIZE])?;
    let data = disk_manager.read_page(1)?;
    assert_eq!(data, vec![66u8; TABLE_PAGE_SIZE]);
    Ok(())
  }
}
