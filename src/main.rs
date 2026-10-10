use dremel::{
    Options, run_bench_server, run_columnar_bench_server, run_columnar_query, run_query,
    run_scan_cli,
};
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
    if command == "scan" {
        return run_scan_cli(&args[2..]);
    }
    let options = Options {
        data: value(&args, "--data", "data/events.csv"),
        direct_parquet: args.iter().any(|arg| arg == "--direct-parquet"),
        streaming_parquet: args.iter().any(|arg| arg == "--streaming-parquet"),
        threads: value(&args, "--threads", "4")
            .parse()
            .map_err(|_| "invalid --threads")?,
        batch_size: value(&args, "--batch-size", "4096")
            .parse()
            .map_err(|_| "invalid --batch-size")?,
        memory_limit_mb: value(&args, "--memory-limit-mb", "0")
            .parse()
            .map_err(|_| "invalid --memory-limit-mb")?,
        query_memory_limit_mb: value(&args, "--query-memory-limit-mb", "0")
            .parse()
            .map_err(|_| "invalid --query-memory-limit-mb")?,
        spill_dir: args
            .windows(2)
            .find(|values| values[0] == "--spill-dir")
            .map(|values| values[1].clone()),
        stream_results: args.iter().any(|arg| arg == "--stream-results"),
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
    let table_name = args
        .iter()
        .position(|arg| arg == "--table")
        .map(|index| args.get(index + 1).ok_or("missing --table name"))
        .transpose()?;
    match command {
        "bench-server" => {
            if let Some(table) = table_name {
                run_columnar_bench_server(options, table)
            } else {
                run_bench_server(options)
            }
        }
        "query" => {
            let default_sql = format!(
                "SELECT COUNT(*) FROM {}",
                table_name.map(String::as_str).unwrap_or("events")
            );
            let sql = value(&args, "--sql", &default_sql);
            if let Some(table) = table_name {
                return run_columnar_query(
                    options,
                    table,
                    &sql,
                    args.iter().any(|a| a == "--explain"),
                    args.iter().any(|a| a == "--stats"),
                );
            }
            run_query(
                options,
                &sql,
                args.iter().any(|a| a == "--explain"),
                args.iter().any(|a| a == "--stats"),
            )
        }
        _ => {
            println!(
                "dremel query|bench-server --data PATH [--table NAME] --threads N --batch-size N [--direct-parquet|--streaming-parquet] [--query-memory-limit-mb N] [--spill-dir PATH] [--stream-results] [--sql SQL] [--explain]"
            );
            println!("dremel scan --help");
            Ok(())
        }
    }
}
