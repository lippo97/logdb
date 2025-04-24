use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Result};

use my_database::{Config, Database, Value};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let mut database = Database::build(Config {
        data_dir: "data".into(),
        sparse_stride: 20,
        memtable_capacity: 1000,
        create_if_missing: true,
    })
    .await?;

    // load_words_into_db(&mut database).await
    repl(&mut database).await
}

async fn repl(database: &mut Database) -> Result<()> {
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut stdout = tokio::io::stdout();

    loop {
        stdout.write_all(b"> ").await?;
        stdout.flush().await?;

        if let Some(line) = lines.next_line().await? {
            let line = line.trim();
            if line == "exit" {
                stdout.write_all(b"bye.\n").await?;
                break;
            }
            parse(line, database).await?;
        } else {
            break;
        }
    }
    Ok(())
}

async fn parse(command: &str, database: &mut Database) -> Result<()> {
    let args: Vec<_> = command.split_whitespace().collect();

    match args.get(0) {
        Some(&"get") => {
            let value = database.get(args.get(1).unwrap()).await?.map(|x| match x {
                Value::Str(s) => s,
                Value::Int64(i) => format!("i:{}", i.to_string()),
                Value::Float64(f) => format!("f:{}", f.to_string()),
            });

            println!("{}", value.unwrap_or("(none)".to_string()));
        }
        Some(&"set") => {
            database
                .set(
                    args.get(1).unwrap().to_string(),
                    parse_value(args.get(2).unwrap()),
                )
                .await?;
        }
        Some(&"delete") => database.delete(args.get(1).unwrap().to_string()).await?,
        Some(&"flush") => database.flush().await?,
        _ => (),
    };

    Ok(())
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

async fn load_words_into_db(database: &mut Database) -> Result<()> {
    let content = tokio::fs::read_to_string("words.txt").await?;
    let lines: Vec<_> = content.lines().map(|line| line.to_string()).collect();

    for line in lines {
        let reversed = Value::Str(line.chars().rev().collect());
        database.set(line, reversed).await?;
    }

    database.flush().await
}
