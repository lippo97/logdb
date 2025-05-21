use std::sync::Arc;

use core::net::SocketAddr;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, Error, Result},
    net::{TcpListener, TcpStream},
    sync::watch::{self, Receiver},
    task::JoinSet,
};

use my_database::{Config, Controller, DatabaseImpl, Value};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let database = Controller::new(
        DatabaseImpl::build(Config {
            data_dir: "data".into(),
            sparse_stride: 20,
            memtable_capacity: 1000,
            create_if_missing: true,
        })
        .await?,
        50000
    );

    let db = Arc::new(database);

    let listener = TcpListener::bind("127.0.0.1:2345").await?;

    let (shutdown_tx, shutdown_rx) = watch::channel(());

    let db_clone = db.clone();
    let listener_handle = tokio::spawn(async move {
        let _ = accept_connections(listener, &db_clone, shutdown_rx).await;
    });

    let stdin = BufReader::new(tokio::io::stdin());
    let mut stdout = tokio::io::stdout();
    repl(&db, stdin, &mut stdout).await?;

    let _ = shutdown_tx.send(());

    listener_handle.await?;
    log::info!("Closed network socket.");
    db.shutdown().await?;

    Ok(())
}

async fn accept_connections(
    listener: TcpListener,
    db: &Arc<Controller>,
    shutdown_rx: Receiver<()>,
) -> Result<()> {
    let mut shutdown_rx_main = shutdown_rx.clone();
    let mut connections = JoinSet::new();

    tokio::select! {
        Ok::<_, Error>(()) = async {
            loop {
                let (socket, conn) = listener.accept().await?;

                let db = db.clone();
                let mut shutdown_rx_task = shutdown_rx.clone();
                connections.spawn(async move {
                    tokio::select! {
                        _ = handle_connection(socket, conn, &db) => {},
                        _ = shutdown_rx_task.changed() => {
                            log::info!("Socket {}:{} shutdown requested", conn.ip(), conn.port());
                        }
                    }
                });
            }
        } => {
            Ok(())
        },
        _ = shutdown_rx_main.changed() => {
            log::info!("Socket shutdown requested.");

            while let Some(res) = connections.join_next().await {
                if let Err(e) = res {
                    log::warn!("Connection handler exited with error: {:?}", e)
                }
            }

            Ok(())
        },
    }
}

async fn handle_connection(socket: TcpStream, addr: SocketAddr, database: &Controller) -> Result<()> {
    let (read, mut write) = tokio::io::split(socket);
    let read = BufReader::new(read);
    log::info!("Client connection from {}:{}", addr.ip(), addr.port());
    repl(database, read, &mut write).await?;
    log::info!("Closed connection from {}:{}", addr.ip(), addr.port());
    Ok::<_, Error>(())
}

async fn repl<R, W>(database: &Controller, input: R, output: &mut W) -> Result<()>
where
    R: AsyncBufRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut lines = input.lines();

    loop {
        output.write_all(b"> ").await?;
        output.flush().await?;

        if let Some(line) = lines.next_line().await? {
            let line = line.trim();
            if line == "exit" {
                output.write_all(b"bye.\n").await?;
                break;
            }
            parse(line, &database, output).await?;
        } else {
            break;
        }
    }
    Ok(())
}

async fn parse<W: AsyncWrite + Unpin>(command: &str, database: &Controller, output: &mut W) -> Result<()> {
    let args: Vec<_> = command.split_whitespace().collect();

    match args.get(0) {
        Some(&"get") => {
            let value = database
                .get(args.get(1).unwrap())
                .await?
                .map(|x| match x {
                    Value::Str(s) => s,
                    Value::Int64(i) => format!("i:{}", i.to_string()),
                    Value::Float64(f) => format!("f:{}", f.to_string()),
                })
                .unwrap_or("(none)".to_string())
                + "\n";

            output.write_all(value.as_bytes()).await?;
            output.flush().await
        }
        Some(&"set") => {
            database
                .set(
                    args.get(1).unwrap().to_string(),
                    parse_value(args.get(2).unwrap()),
                )
                .await
        }
        Some(&"delete") => {
            database
                .delete(args.get(1).unwrap().to_string())
                .await
        }
        // Some(&"compact") => database.compact().await,
        // Some(&"flush") => database.flush().await,
        // Some(&"dump") => database.dump().await,
        Some(&"words") => load_words_into_db(database).await,
        _ => Ok(()),
    }
}

fn parse_value(input: &str) -> Value {
    if let Some(rest) = input.strip_prefix("i:") {
        if let Ok(num) = rest.parse::<i64>() {
            return Value::Int64(num);
        }
    } else if let Some(rest) = input.strip_prefix("f:") {
        if let Ok(num) = rest.parse::<f64>() {
            return Value::Float64(num);
        }
    }
    Value::Str(input.to_string())
}

async fn load_words_into_db(database: &Controller) -> Result<()> {
    let content = tokio::fs::read_to_string("words.txt").await?;
    let lines: Vec<_> = content.lines().map(|line| line.to_string()).collect();

    for line in lines {
        // let strip = Value::Str(line.chars().take(3).collect());
        let reversed = Value::Str(line.chars().rev().collect());
        database.set(line, reversed).await?;
    }
    
    Ok(())
}
