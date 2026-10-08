use super::{ColumnarQuery, load_table};
use crate::execution::row_json;
use crate::types::{Options, Scalar};
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::time::Instant;

/// Serves prepared named-table queries over the local benchmark protocol.
pub fn run_columnar_bench_server(options: Options, table_name: &str) -> Result<(), String> {
    let started = Instant::now();
    let table = load_table(&options)?;
    let mut prepared = HashMap::<String, ColumnarQuery>::new();
    let mut output = std::io::BufWriter::new(std::io::stdout().lock());
    writeln!(output, "READY\t{}", started.elapsed().as_nanos())
        .map_err(|error| error.to_string())?;
    output.flush().map_err(|error| error.to_string())?;
    for line in std::io::stdin().lock().lines() {
        let line = line.map_err(|error| error.to_string())?;
        let parts: Vec<_> = line.split('\t').collect();
        if parts == ["SHUTDOWN"] {
            writeln!(output, "BYE").map_err(|error| error.to_string())?;
            output.flush().map_err(|error| error.to_string())?;
            break;
        }
        let response = (|| -> Result<String, String> {
            match parts.as_slice() {
                ["CONFIG"] => Ok(format!(
                    "CONFIG\t1\t{}\t1\t{}",
                    options.batch_size,
                    table.row_count()
                )),
                ["PREPARE", id, sql] => {
                    let started = Instant::now();
                    let query = ColumnarQuery::prepare(table_name, table.schema().clone(), sql)?;
                    prepared.insert((*id).into(), query);
                    Ok(format!("OK\t{id}\t{}", started.elapsed().as_nanos()))
                }
                ["EXEC", id, include_rows] => {
                    let query = prepared.get(*id).ok_or("unknown prepared query")?;
                    let started = Instant::now();
                    let result =
                        query.execute_with_memory_limit(&table, options.query_memory_limit_mb)?;
                    let elapsed = started.elapsed().as_nanos();
                    if options.max_result_rows > 0 && result.rows.len() > options.max_result_rows {
                        return Err(
                            "RESOURCE_EXHAUSTED named-table result exceeds --max-result-rows"
                                .into(),
                        );
                    }
                    let rows = if *include_rows == "1" {
                        result
                            .rows
                            .iter()
                            .map(|row| row_json(row))
                            .collect::<Vec<_>>()
                            .join(",")
                    } else {
                        String::new()
                    };
                    Ok(format!(
                        "RESULT\t{elapsed}\t{}\t[{rows}]",
                        result.rows.len()
                    ))
                }
                ["EXPLAIN", id] => {
                    let query = prepared.get(*id).ok_or("unknown prepared query")?;
                    let lines = query
                        .explain()
                        .lines()
                        .map(|line| Scalar::Str(line.trim().into()).json())
                        .collect::<Vec<_>>()
                        .join(",");
                    Ok(format!("EXPLAIN\t[{lines}]"))
                }
                _ => Err("unsupported named-table benchmark command".into()),
            }
        })();
        match response {
            Ok(response) => writeln!(output, "{response}"),
            Err(error) => {
                eprintln!("{error}");
                writeln!(output, "ERROR\t{error}")
            }
        }
        .map_err(|error| error.to_string())?;
        output.flush().map_err(|error| error.to_string())?;
    }
    Ok(())
}
