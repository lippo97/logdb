use std::sync::Arc;

use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, Error, Result},
    net::TcpListener,
    sync::RwLock,
};

use my_database::{Config, Database, Value};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let database = Arc::new(RwLock::new(
        Database::build(Config {
            data_dir: "data".into(),
            sparse_stride: 20,
            memtable_capacity: 1000,
            create_if_missing: true,
        })
        .await?,
    ));

    let listener = TcpListener::bind("127.0.0.1:2345").await?;

    loop {
        let (socket, conn) = listener.accept().await?;
        let (read, mut write) = tokio::io::split(socket);
        let read = BufReader::new(read);

        let db = database.clone();
        tokio::spawn(async move {
            log::info!("Client connection from {}:{}", conn.ip(), conn.port());
            repl(db, read, &mut write).await?;
            log::info!("Closed connection from {}:{}", conn.ip(), conn.port());
            Ok::<_, Error>(())
        });
    }
}

async fn repl<R, W>(database: Arc<RwLock<Database>>, input: R, output: &mut W) -> Result<()>
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

async fn parse<W: AsyncWrite + Unpin>(
    command: &str,
    database: &Arc<RwLock<Database>>,
    output: &mut W,
) -> Result<()> {
    let args: Vec<_> = command.split_whitespace().collect();

    match args.get(0) {
        Some(&"get") => {
            let value = database
                .read()
                .await
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
                .write()
                .await
                .set(
                    args.get(1).unwrap().to_string(),
                    parse_value(args.get(2).unwrap()),
                )
                .await
        }
        Some(&"delete") => {
            database
                .write()
                .await
                .delete(args.get(1).unwrap().to_string())
                .await
        }
        Some(&"flush") => database.write().await.flush().await,
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

    // Fallback to string
    Value::Str(input.to_string())
}

// async fn load_words_into_db(database: &mut Database) -> Result<()> {
//     let content = tokio::fs::read_to_string("words.txt").await?;
//     let lines: Vec<_> = content.lines().map(|line| line.to_string()).collect();
//
//     for line in lines {
//         let reversed = Value::Str(line.chars().rev().collect());
//         database.set(line, reversed).await?;
//     }
//
//     database.flush().await
// }
