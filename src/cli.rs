use std::io;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::str;

use anyhow::Result;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Default)]
pub struct Cli {}

impl Cli {
    pub fn new() -> Self {
        Cli {}
    }
    pub fn start(&mut self) -> Result<()> {
        loop {
            print!(">> ");
            io::stdout().flush()?;
            let mut query = String::new();
            // not support multi lines query.
            io::stdin().read_line(&mut query)?;
            if query.trim() == "quit" {
                break;
            }
            let mut stream = TcpStream::connect("127.0.0.1:3305")?;
            stream.write_all(query.as_bytes())?;
            // 4096 bytes limit
            let mut buffer = [0u8; 4096];
            let size = stream.read(&mut buffer)?;
            print!("{}", str::from_utf8(&buffer[0..size])?);
            io::stdout().flush()?;
        }
        Ok(())
    }
}
