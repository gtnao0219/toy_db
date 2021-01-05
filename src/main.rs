extern crate toy_db;

use std::env;
use std::io;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process;
use std::sync::Arc;
use std::thread;

use toy_db::catalog::Catalog;
use toy_db::disk::DiskManager;
use toy_db::execution::{CreateTableExecutor, Executor, InsertExecutor, SelectExecutor};
use toy_db::parser::token;
use toy_db::parser::{Parser, Stmt};

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let disk_manager_arc = Arc::new(DiskManager::new("data/".to_string()));
    let catalog_arc = Arc::new(Catalog::new(disk_manager_arc.clone()));
    match &*args[1] {
        "init" => {
            catalog_arc.clone().initialize()?;
            return Ok(());
        }
        "start" => {
            let catalog = catalog_arc.clone();
            catalog.set_oid();
        }
        _ => panic!("unknown subcomand"),
    }
    let listner = TcpListener::bind("127.0.0.1:3305").expect("Error: failed to bind.");
    for streams in listner.incoming() {
        match streams {
            Err(e) => {
                eprintln!("Error: {:?}", e)
            }
            Ok(stream) => {
                let catalog = catalog_arc.clone();
                let disk_manager = disk_manager_arc.clone();
                thread::spawn(move || {
                    handle(stream, catalog, disk_manager)
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
) -> io::Result<()> {
    let mut buf = String::new();
    stream.read_to_string(&mut buf)?;
    let tokens = match token::tokenize(&mut buf.chars().peekable()) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("failed tokenize: {}", e);
            process::exit(1);
        }
    };
    println!("token is {:?}", tokens);
    let mut parser = Parser::new(tokens);
    let ast = match parser.parse() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("failed parse: {}", e);
            process::exit(1);
        }
    };
    println!("ast is {:?}", ast);

    let result = match ast {
        Stmt::CreateTableStmt(_) => CreateTableExecutor {
            stmt: ast,
            catalog: catalog,
        }
        .execute()?,
        Stmt::InsertStmt(_) => InsertExecutor {
            stmt: ast,
            catalog: catalog,
            disk_manager: disk_manager,
        }
        .execute()?,
        Stmt::SelectStmt(_) => SelectExecutor {
            stmt: ast,
            catalog: catalog,
            disk_manager: disk_manager,
        }
        .execute()?,
    };
    stream.write(result.as_bytes())?;
    stream.flush()?;
    Ok(())
}
