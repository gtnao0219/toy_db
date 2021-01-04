extern crate toy_db;

use std::io;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process;
use std::sync::Arc;
use std::thread;

use toy_db::catalog::Catalog;
use toy_db::disk::DiskManager;
use toy_db::execution::{CreateTableExecutor, Executor};
use toy_db::parser::token;
use toy_db::parser::Parser;

fn main() -> io::Result<()> {
    let disk_manager_arc = Arc::new(DiskManager::new("data/".to_string()));
    let catalog_arc = Arc::new(Catalog::new(disk_manager_arc.clone()));
    // catalog_arc.clone().initialize()?;
    let listner = TcpListener::bind("127.0.0.1:3305").expect("Error: failed to bind.");
    for streams in listner.incoming() {
        match streams {
            Err(e) => {
                eprintln!("Error: {:?}", e)
            }
            Ok(stream) => {
                let catalog = catalog_arc.clone();
                thread::spawn(move || {
                    handle(stream, catalog).unwrap_or_else(|e| eprintln!("Error: {:?}", e))
                });
            }
        }
    }
    Ok(())
}

fn handle(mut stream: TcpStream, catalog: Arc<Catalog>) -> io::Result<()> {
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

    let executor = CreateTableExecutor {
        stmt: ast,
        catalog: catalog,
    };
    executor.execute();
    stream.write("finished\n".as_bytes())?;
    stream.flush()?;
    Ok(())
}
