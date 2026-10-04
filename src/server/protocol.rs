use super::*;

fn execute_prepared(
    o: &Options,
    query: &Query,
    table: &Arc<Table>,
    pool: &Pool,
    catalog: &mut Option<Arc<Catalog>>,
) -> Result<(Vec<Vec<Scalar>>, ParquetScanMetrics, SpillMetrics), String> {
    if o.streaming_parquet {
        return execute_parquet_stream(query, &o.data, pool, o)
            .map(|(rows, scan)| (rows, scan, SpillMetrics::default()));
    }
    let (execution_table, scan) = if o.direct_parquet {
        let (table, scan) = Table::load_parquet_direct(&o.data, query, o.batch_size)?;
        (Arc::new(table), scan)
    } else {
        (table.clone(), ParquetScanMetrics::default())
    };
    enforce_table_limit(o, &execution_table)?;
    let (rows, spill) = if is_relational(query) {
        let rows = if o.direct_parquet {
            execute_rel(query, &Catalog::load(&o.data, execution_table)?)?
        } else {
            if catalog.is_none() {
                *catalog = Some(Arc::new(Catalog::load(&o.data, table.clone())?));
            }
            execute_rel(query, catalog.as_ref().expect("catalog loaded"))?
        };
        (rows, SpillMetrics::default())
    } else {
        execute_with_spill(
            query,
            execution_table,
            pool,
            o.batch_size,
            o.spill_dir.as_deref(),
        )?
    };
    Ok((rows, scan, spill))
}

pub fn run_bench_server(o: Options) -> Result<(), String> {
    if o.direct_parquet && o.streaming_parquet {
        return Err("choose either --direct-parquet or --streaming-parquet".into());
    }
    let parquet_query = o.direct_parquet || o.streaming_parquet;
    if parquet_query && !o.data.ends_with(".parquet") {
        return Err("direct and streaming Parquet execution require a .parquet data file".into());
    }
    let load = Instant::now();
    let table = Arc::new(if parquet_query {
        Table::parquet_metadata(&o.data)?
    } else {
        Table::load(&o.data)?
    });
    enforce_table_limit(&o, &table)?;
    let load_ns = load.elapsed().as_nanos();
    let pool = Pool::new(o.threads);
    let mut catalog: Option<Arc<Catalog>> = None;
    let mut scheduler: Option<AsyncScheduler> = None;
    let mut prepared = std::collections::HashMap::<String, Query>::new();
    println!("READY\t{load_ns}");
    io::stdout().flush().map_err(|e| e.to_string())?;
    for line in io::stdin().lock().lines() {
        let line = line.map_err(|e| e.to_string())?;
        let p: Vec<&str> = line.split('\t').collect();
        match p[0] {
            "CONFIG" => println!(
                "CONFIG\t{}\t{}\t{}\t{}",
                o.threads,
                o.batch_size,
                o.threads * 4,
                table.len()
            ),
            "PREPARE" => {
                let mut q = prepare(
                    Parser::new(p.get(2).ok_or("missing sql")?)?.parse()?,
                    &table,
                );
                if q.ctes.is_empty() {
                    bind_query(&q)?;
                }
                enforce_result_limit(&o, &q, &table)?;
                if parquet_query {
                    let scan = Table::parquet_scan_plan(&o.data, &q)?;
                    q.physical.insert(
                        1,
                        format!(
                            "ParquetScanExec(columns={}/{};row_groups={}/{};rows={}/{};compressed_bytes={})",
                            scan.columns_read,
                            scan.total_columns,
                            scan.row_groups_read,
                            scan.total_row_groups,
                            scan.rows_read,
                            scan.total_rows,
                            scan.compressed_bytes_read
                        ),
                    );
                    if o.streaming_parquet {
                        q.physical.insert(
                            2,
                            format!(
                                "ParquetStreamExec(batch_size={};fallback={})",
                                o.batch_size,
                                parquet_streaming_fallback(&q)
                            ),
                        );
                    }
                }
                if o.spill_dir.is_some() && !o.streaming_parquet && spillable_aggregate(&q) {
                    q.physical
                        .insert(1, "SpillAggregateExec(partitions=auto)".into());
                }
                prepared.insert(p[1].into(), q);
                println!("OK\t{}", p[1])
            }
            "EXEC" => {
                let q = prepared.get(p[1]).ok_or("unknown query")?;
                let now = Instant::now();
                let (rows, memory) = with_query_memory(o.query_memory_limit_mb, || {
                    execute_prepared(&o, q, &table, &pool, &mut catalog)
                });
                let ns = now.elapsed().as_nanos();
                match rows {
                    Ok((rows, scan, spill)) => println!(
                        "RESULT\t{ns}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                        rows.len(),
                        if p.get(2) == Some(&"1") {
                            rows_json(&rows)
                        } else {
                            "[]".into()
                        },
                        memory.limit_bytes(),
                        memory.accounted_bytes(),
                        scan.total_rows,
                        scan.rows_read,
                        scan.total_row_groups,
                        scan.row_groups_read,
                        scan.total_columns,
                        scan.columns_read,
                        scan.compressed_bytes_read,
                        scan.batches_read,
                        scan.peak_decoded_batch_bytes,
                        scan.streaming_fallback,
                        memory.peak_accounted_bytes(),
                        spill.files_created,
                        spill.partitions,
                        spill.bytes_written,
                        spill.bytes_read,
                        spill.passes,
                        spill.spilled()
                    ),
                    Err(error) => println!("ERROR\t{}", error.replace(['\t', '\n'], " ")),
                }
            }
            "E2E" => {
                let now = Instant::now();
                let mut q = prepare(
                    Parser::new(p.get(2).ok_or("missing sql")?)?.parse()?,
                    &table,
                );
                if q.ctes.is_empty() {
                    bind_query(&q)?;
                }
                if parquet_query {
                    let scan = Table::parquet_scan_plan(&o.data, &q)?;
                    q.physical.insert(
                        1,
                        format!(
                            "ParquetScanExec(columns={}/{};row_groups={}/{};rows={}/{};compressed_bytes={})",
                            scan.columns_read,
                            scan.total_columns,
                            scan.row_groups_read,
                            scan.total_row_groups,
                            scan.rows_read,
                            scan.total_rows,
                            scan.compressed_bytes_read
                        ),
                    );
                    if o.streaming_parquet {
                        q.physical.insert(
                            2,
                            format!(
                                "ParquetStreamExec(batch_size={};fallback={})",
                                o.batch_size,
                                parquet_streaming_fallback(&q)
                            ),
                        );
                    }
                }
                if o.spill_dir.is_some() && !o.streaming_parquet && spillable_aggregate(&q) {
                    q.physical
                        .insert(1, "SpillAggregateExec(partitions=auto)".into());
                }
                let (rows, memory) = with_query_memory(o.query_memory_limit_mb, || {
                    execute_prepared(&o, &q, &table, &pool, &mut catalog)
                });
                let ns = now.elapsed().as_nanos();
                match rows {
                    Ok((rows, scan, spill)) => println!(
                        "RESULT\t{ns}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                        rows.len(),
                        if p.get(3) == Some(&"1") {
                            rows_json(&rows)
                        } else {
                            "[]".into()
                        },
                        memory.limit_bytes(),
                        memory.accounted_bytes(),
                        scan.total_rows,
                        scan.rows_read,
                        scan.total_row_groups,
                        scan.row_groups_read,
                        scan.total_columns,
                        scan.columns_read,
                        scan.compressed_bytes_read,
                        scan.batches_read,
                        scan.peak_decoded_batch_bytes,
                        scan.streaming_fallback,
                        memory.peak_accounted_bytes(),
                        spill.files_created,
                        spill.partitions,
                        spill.bytes_written,
                        spill.bytes_read,
                        spill.passes,
                        spill.spilled()
                    ),
                    Err(error) => println!("ERROR\t{}", error.replace(['\t', '\n'], " ")),
                }
            }
            "EXPLAIN" => println!(
                "EXPLAIN\t{}",
                strings_json(&prepared.get(p[1]).ok_or("unknown query")?.physical)
            ),
            "CONFIG_ASYNC" => println!(
                "ASYNC_CONFIG\t{}\t{}\t{}\t{}\t{}",
                o.max_active_queries.max(1),
                o.admission_queue_capacity.max(1),
                o.scheduler_memory_mb.max(1),
                o.max_active_queries.div_ceil(2).max(1),
                o.scheduler_memory_mb.div_ceil(2).max(1)
            ),
            "SUBMIT" => {
                let result = (|| -> Result<(), String> {
                    let request_id = *p.get(1).ok_or("missing request id")?;
                    let query_id = *p.get(2).ok_or("missing query id")?;
                    let priority = p
                        .get(3)
                        .ok_or("missing priority")?
                        .parse()
                        .map_err(|_| "bad priority")?;
                    let group = *p.get(4).ok_or("missing group")?;
                    let deadline_ms = p
                        .get(5)
                        .ok_or("missing deadline")?
                        .parse()
                        .map_err(|_| "bad deadline")?;
                    let memory_mb = p
                        .get(6)
                        .ok_or("missing memory")?
                        .parse()
                        .map_err(|_| "bad memory")?;
                    let include_rows = *p.get(7).unwrap_or(&"0") == "1";
                    let query = prepared.get(query_id).cloned().ok_or("unknown query")?;
                    if scheduler.is_none() {
                        let execute: Arc<QueryExecutor> = if parquet_query {
                            let options = o.clone();
                            let metadata = table.clone();
                            Arc::new(move |query| {
                                let pool = Pool::new(options.threads);
                                let mut catalog = None;
                                execute_prepared(&options, query, &metadata, &pool, &mut catalog)
                                    .map(|(rows, _, _)| rows)
                            })
                        } else {
                            if catalog.is_none() {
                                catalog = Some(Arc::new(Catalog::load(&o.data, table.clone())?));
                            }
                            let shared_catalog = catalog.as_ref().expect("catalog loaded").clone();
                            Arc::new(move |query| execute_rel(query, &shared_catalog))
                        };
                        scheduler = Some(AsyncScheduler::new(execute, &o));
                    }
                    scheduler.as_ref().expect("scheduler created").submit(
                        request_id,
                        query,
                        SubmitOptions {
                            priority,
                            group,
                            deadline_ms,
                            memory_mb,
                            include_rows,
                        },
                    )
                })();
                match result {
                    Ok(()) => println!("ACCEPTED\t{}", p[1]),
                    Err(error) => println!("REJECTED\t{}", error),
                }
            }
            "POLL" | "WAIT" => {
                let result = scheduler
                    .as_ref()
                    .ok_or_else(|| "scheduler not initialized".to_string())
                    .and_then(|scheduler| {
                        scheduler.status(p.get(1).copied().unwrap_or(""), p[0] == "WAIT")
                    });
                match result {
                    Ok(status) => println!("{status}"),
                    Err(error) => println!("ERROR\t{error}"),
                }
            }
            "CANCEL" => {
                let result = scheduler
                    .as_ref()
                    .ok_or_else(|| "scheduler not initialized".to_string())
                    .and_then(|scheduler| scheduler.cancel(p.get(1).copied().unwrap_or("")));
                match result {
                    Ok(()) => println!("CANCELLED\t{}", p.get(1).copied().unwrap_or("")),
                    Err(error) => println!("ERROR\t{error}"),
                }
            }
            "SHUTDOWN" => {
                println!("BYE");
                io::stdout().flush().map_err(|e| e.to_string())?;
                break;
            }
            _ => println!("ERROR\tunknown command"),
        };
        io::stdout().flush().map_err(|e| e.to_string())?
    }
    Ok(())
}
