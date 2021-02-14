use std::io;
use std::io::Write;

use anyhow::Result;
use serde_json::{json, Value};

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Default)]
pub struct Cli {}

impl Cli {
    pub fn new() -> Self {
        Cli {}
    }
    pub async fn start(&mut self) -> Result<()> {
        loop {
            print!(">> ");
            io::stdout().flush()?;
            let mut query = String::new();
            // not support multi lines query.
            io::stdin().read_line(&mut query)?;
            if query.trim() == "quit" {
                break;
            }
            let client = reqwest::Client::new();
            let body = json!({
                "query": query.trim(),
            });
            let resp = client
                .post("http://127.0.0.1:3305/")
                .body(body.to_string())
                .send()
                .await?
                .text()
                .await?;
            let v: Value = serde_json::from_str(&resp)?;
            if let Value::String(s) = &v["result"] {
                for l in s.split('\n') {
                    println!("{}", l.trim());
                }
                io::stdout().flush()?;
            }
        }
        Ok(())
    }
}
