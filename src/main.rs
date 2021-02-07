extern crate toy_db;

use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;

use anyhow::Result;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use signal_hook::iterator::Signals;

use toy_db::buffer::BufferPoolManager;
use toy_db::catalog::Catalog;
use toy_db::disk::DiskManager;
use toy_db::execution::{CreateTableExecutor, Executor, InsertExecutor, SelectExecutor};
use toy_db::parser::token;
use toy_db::parser::{Parser, Stmt};

fn main() -> Result<()> {
    let args = env::args().collect::<Vec<String>>();
    let disk_manager = Arc::new(DiskManager::new("data/".to_string()));
    let buffer_pool_manager = Arc::new(BufferPoolManager::new(disk_manager.clone()));
    let catalog = Arc::new(Catalog::new(buffer_pool_manager.clone()));
    match &*args[1] {
        "init" => {
            disk_manager.init_data_file()?;
            catalog.initialize()?;
            return Ok(());
        }
        "start" => {
            catalog.bootstrap();
        }
        _ => panic!("unknown subcomand"),
    }
    let listner = TcpListener::bind("127.0.0.1:3305")?;

    // Make sure double CTRL+C and similar kills
    let term_now = Arc::new(AtomicBool::new(false));
    for sig in TERM_SIGNALS {
        // When terminated by a second term signal, exit with exit code 1.
        // This will do nothing the first time (because term_now is false).
        flag::register_conditional_shutdown(*sig, 1, Arc::clone(&term_now))?;
        // But this will "arm" the above for the second time, by setting it to true.
        // The order of registering these is important, if you put this one first, it will
        // first arm and then terminate ‒ all in the first round.
        flag::register(*sig, Arc::clone(&term_now))?;
    }
    let mut signals = Signals::new(TERM_SIGNALS)?;
    let buffer_pool_manager_for_signals = buffer_pool_manager.clone();
    thread::spawn(move || {
        for _ in signals.forever() {
            match buffer_pool_manager_for_signals.flush_all_pages() {
                Ok(()) => {
                    println!("Succeeded to flush pages.");
                }
                Err(e) => {
                    eprintln!("Failed to flush pages. Error: {:?}", e);
                }
            }
        }
    });

    for streams in listner.incoming() {
        match streams {
            Err(e) => {
                eprintln!("Error: {:?}", e)
            }
            Ok(stream) => {
                let catalog_clone = catalog.clone();
                let buffer_pool_manager_clone = buffer_pool_manager.clone();
                thread::spawn(move || {
                    handle(stream, catalog_clone, buffer_pool_manager_clone)
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
    buffer_pool_manager: Arc<BufferPoolManager>,
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
            buffer_pool_manager,
        }
        .execute()?,
        Stmt::SelectStmt(_) => SelectExecutor {
            stmt,
            catalog,
            buffer_pool_manager,
        }
        .execute()?,
    };
    stream.write_all(result.as_bytes())?;
    stream.flush()?;
    Ok(())
}
