extern crate toy_db;

use std::collections::HashMap;
use std::env;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;

use anyhow::Result;
use serde_json::json;
use signal_hook::consts::TERM_SIGNALS;
use signal_hook::flag;
use signal_hook::iterator::Signals;
use warp::Filter;

use toy_db::buffer::BufferPoolManager;
use toy_db::catalog::Catalog;
use toy_db::cli::Cli;
use toy_db::disk::DiskManager;
use toy_db::execution::{CreateTableExecutor, Executor, InsertExecutor, SelectExecutor};
use toy_db::parser::token;
use toy_db::parser::{Parser, Stmt};

#[tokio::main]
async fn main() -> Result<()> {
    let args = env::args().collect::<Vec<String>>();
    if &*args[1] == "cli" {
        Cli::new().start().await?;
        return Ok(());
    }
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

    let routes = warp::post()
        .and(warp::body::json())
        .map(move |body: HashMap<String, String>| {
            let catalog_clone = catalog.clone();
            let buffer_pool_manager_clone = buffer_pool_manager.clone();
            handle(body, catalog_clone, buffer_pool_manager_clone)
                .unwrap_or_else(|e| format!("Error: {:?}", e))
        });
    warp::serve(routes).run(([127, 0, 0, 1], 3305)).await;
    Ok(())
}

fn handle(
    body: HashMap<String, String>,
    catalog: Arc<Catalog>,
    buffer_pool_manager: Arc<BufferPoolManager>,
) -> Result<String> {
    let query = body.get("query").unwrap();
    let tokens = token::tokenize(&mut query.chars().peekable())?;
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
    let resp = json!({
        "result": result,
    });
    Ok(resp.to_string())
}
