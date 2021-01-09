extern crate toy_db;

use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use anyhow::Result;

use toy_db::catalog::Catalog;
use toy_db::disk::DiskManager;
use toy_db::execution::{CreateTableExecutor, Executor, InsertExecutor, SelectExecutor};
use toy_db::parser::token;
use toy_db::parser::{Parser, Stmt};

fn main() -> Result<()> {
    let args = env::args().collect::<Vec<String>>();
    let disk_manager = Arc::new(DiskManager::new("data/".to_string()));
    let catalog = Arc::new(Catalog::new(disk_manager.clone()));
    match &*args[1] {
        "init" => {
            catalog.initialize()?;
            return Ok(());
        }
        "start" => {
            catalog.bootstrap();
        }
        _ => panic!("unknown subcomand"),
    }
    let listner = TcpListener::bind("127.0.0.1:3305")?;
    for streams in listner.incoming() {
        match streams {
            Err(e) => {
                eprintln!("Error: {:?}", e)
            }
            Ok(stream) => {
                let catalog_clone = catalog.clone();
                let disk_manager_clone = disk_manager.clone();
                thread::spawn(move || {
                    handle(stream, catalog_clone, disk_manager_clone)
                        .unwrap_or_else(|e| eprintln!("Error: {:?}", e))
                });
            }
        }
    }
    Ok(())
}

fn handle(
    mut stream: TcpStream,
    catalog: Arc<Catalog>,
    disk_manager: Arc<DiskManager>,
) -> Result<()> {
    let mut buf = String::new();
    stream.read_to_string(&mut buf)?;
    let tokens = token::tokenize(&mut buf.chars().peekable())?;
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse()?;

    let result = match stmt {
        Stmt::CreateTableStmt(_) => CreateTableExecutor { stmt, catalog }.execute()?,
        Stmt::InsertStmt(_) => InsertExecutor {
            stmt,
            catalog,
            disk_manager,
        }
        .execute()?,
        Stmt::SelectStmt(_) => SelectExecutor {
            stmt,
            catalog,
            disk_manager,
        }
        .execute()?,
    };
    stream.write_all(result.as_bytes())?;
    stream.flush()?;
    Ok(())
}
