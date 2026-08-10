use dremel_rs::{Options, run_bench_server, run_query};
use std::env;

fn value(args: &[String], name: &str, default: &str) -> String {
    args.windows(2)
        .find(|w| w[0] == name)
        .map_or_else(|| default.to_owned(), |w| w[1].clone())
}

fn main() {
    if let Err(error) = real_main() {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

fn real_main() -> Result<(), String> {
    let args: Vec<String> = env::args().collect();
    let command = args.get(1).map(String::as_str).unwrap_or("help");
    let options = Options {
        data: value(&args, "--data", "data/events.csv"),
        threads: value(&args, "--threads", "4")
            .parse()
            .map_err(|_| "invalid --threads")?,
        batch_size: value(&args, "--batch-size", "4096")
            .parse()
            .map_err(|_| "invalid --batch-size")?,
        memory_limit_mb: value(&args, "--memory-limit-mb", "0")
            .parse()
            .map_err(|_| "invalid --memory-limit-mb")?,
        max_result_rows: value(&args, "--max-result-rows", "0")
            .parse()
            .map_err(|_| "invalid --max-result-rows")?,
        max_active_queries: value(&args, "--max-active-queries", "4")
            .parse()
            .map_err(|_| "invalid --max-active-queries")?,
        admission_queue_capacity: value(&args, "--queue-capacity", "128")
            .parse()
            .map_err(|_| "invalid --queue-capacity")?,
        scheduler_memory_mb: value(&args, "--scheduler-memory-mb", "1024")
            .parse()
            .map_err(|_| "invalid --scheduler-memory-mb")?,
    };
    match command {
        "bench-server" => run_bench_server(options),
        "query" => {
            let sql = value(&args, "--sql", "SELECT COUNT(*) FROM events");
            run_query(
                options,
                &sql,
                args.iter().any(|a| a == "--explain"),
                args.iter().any(|a| a == "--stats"),
            )
        }
        _ => {
            println!(
                "dremel-rs query|bench-server --data PATH --threads N --batch-size N [--sql SQL] [--explain]"
            );
            Ok(())
        }
    }
}
