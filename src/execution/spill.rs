use super::aggregate::*;
use super::row_bytes;
use super::scalar::eval;
use crate::relational::{compare_output_rows, finalize_rows, query_output_order};
use crate::sql::*;
use crate::storage::Table;
use crate::types::*;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

static SPILL_DIRECTORY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub(super) struct SpillDirectory {
    path: PathBuf,
}

impl SpillDirectory {
    pub(super) fn create(root: &str) -> Result<Self, String> {
        let root = Path::new(root);
        std::fs::create_dir_all(root).map_err(|error| {
            format!("cannot create spill directory {}: {error}", root.display())
        })?;
        for _ in 0..1000 {
            let sequence = SPILL_DIRECTORY_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
            let path = root.join(format!(".dremel-spill-{}-{sequence}", std::process::id()));
            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(format!(
                        "cannot create spill workspace {}: {error}",
                        path.display()
                    ));
                }
            }
        }
        Err("cannot allocate a unique spill workspace".into())
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SpillDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct QueryReservation {
    memory: Arc<QueryMemory>,
    bytes: usize,
}

impl QueryReservation {
    fn new(memory: Arc<QueryMemory>) -> Self {
        Self { memory, bytes: 0 }
    }

    fn try_add(&mut self, bytes: usize, budget: usize) -> bool {
        if self.bytes.saturating_add(bytes) > budget || !self.memory.try_account(bytes) {
            return false;
        }
        self.bytes += bytes;
        true
    }
}

impl Drop for QueryReservation {
    fn drop(&mut self) {
        self.memory.release(self.bytes);
    }
}

pub(crate) fn spillable_aggregate(query: &Query) -> bool {
    query.from.name == "events"
        && query.joins.is_empty()
        && query.having.is_none()
        && query.union.is_none()
        && query.ctes.is_empty()
        && !query
            .select
            .iter()
            .any(|item| contains_window(&item.expr) || contains_subquery(&item.expr))
        && !query.filter.as_ref().is_some_and(contains_subquery)
        && !query.group_by.is_empty()
        && query.group_by.len() <= 3
        && query.limit.is_some()
        && !query.order_by.is_empty()
        && !query.distinct
}

pub(crate) fn should_spill_aggregate(
    query: &Query,
    table: &Table,
    spill_dir: Option<&str>,
) -> bool {
    let Some(memory) = current_query_memory() else {
        return false;
    };
    if spill_dir.is_none() || memory.limit_bytes() == 0 || !spillable_aggregate(query) {
        return false;
    }
    let group_bytes = 512usize;
    table
        .group_upper_bound(&query.group_by)
        .saturating_mul(group_bytes)
        > memory
            .limit_bytes()
            .saturating_sub(memory.accounted_bytes())
            / 2
}

fn group_output_row(
    query: &Query,
    table: &Table,
    key: GroupKey,
    states: &[AggState],
) -> Vec<Scalar> {
    let mut aggregate_index = 0;
    query
        .select
        .iter()
        .map(|item| {
            if is_agg(&item.expr) {
                let value = finish(&states[aggregate_index]);
                aggregate_index += 1;
                value
            } else if let Expr::Column(column) = &item.expr {
                let key_index = query
                    .group_by
                    .iter()
                    .position(|group| group == column)
                    .expect("grouped column");
                table.key_scalar(column, key.v[key_index])
            } else {
                Scalar::Null
            }
        })
        .collect()
}

fn retain_top_k_row(
    order: &[(usize, &OrderSpec)],
    top_k: usize,
    rows: &mut Vec<Vec<Scalar>>,
    row: Vec<Scalar>,
) -> Result<(), String> {
    if top_k == 0 {
        return Ok(());
    }
    if rows.len() < top_k {
        account_query_memory(row_bytes(&row), "spill result Top-K")?;
        rows.push(row);
        return Ok(());
    }
    let worst = (0..rows.len())
        .max_by(|&left, &right| compare_output_rows(&rows[left], &rows[right], order))
        .expect("non-empty spill Top-K");
    if compare_output_rows(&row, &rows[worst], order) == std::cmp::Ordering::Less {
        let old_bytes = row_bytes(&rows[worst]);
        let new_bytes = row_bytes(&row);
        if new_bytes > old_bytes {
            account_query_memory(new_bytes - old_bytes, "spill result Top-K")?;
        } else {
            current_query_memory()
                .expect("query memory configured")
                .release(old_bytes - new_bytes);
        }
        rows[worst] = row;
    }
    Ok(())
}

pub(crate) fn execute_spilled_aggregate(
    query: &Query,
    table: &Table,
    batch: usize,
    spill_root: &str,
) -> Result<(Vec<Vec<Scalar>>, SpillMetrics), String> {
    let memory = current_query_memory().ok_or("spill requires query memory accounting")?;
    let available = memory
        .limit_bytes()
        .saturating_sub(memory.accounted_bytes());
    if available < 2 * 1024 * 1024 {
        return Err("RESOURCE_EXHAUSTED spill aggregation requires at least 2 MiB of available query memory".into());
    }
    let partition_budget = available / 2;
    let estimated_groups = table.group_upper_bound(&query.group_by).max(1);
    let groups_per_partition = (partition_budget / 512).max(1);
    let required = estimated_groups.div_ceil(groups_per_partition).max(2);
    let partitions = required.next_power_of_two().min(1024);
    let workspace = SpillDirectory::create(spill_root)?;
    let paths = (0..partitions)
        .map(|partition| workspace.path.join(format!("partition-{partition:04}.bin")))
        .collect::<Vec<_>>();
    let mut created = vec![false; partitions];
    let scratch_bytes = batch
        .max(1)
        .saturating_mul(std::mem::size_of::<u64>())
        .saturating_add(partitions.saturating_mul(std::mem::size_of::<Vec<u64>>() + 4096));
    account_query_memory(scratch_bytes, "spill partition buffers")?;
    let mut buckets = vec![Vec::<u64>::new(); partitions];
    let mut writers = (0..partitions)
        .map(|_| None)
        .collect::<Vec<Option<BufWriter<File>>>>();
    let mut metrics = SpillMetrics {
        partitions,
        passes: 2,
        ..SpillMetrics::default()
    };
    for start in (0..table.len()).step_by(batch.max(1)) {
        if execution_cancelled() {
            return Err("query cancelled during spill partitioning".into());
        }
        for bucket in &mut buckets {
            bucket.clear();
        }
        for row in start..(start + batch.max(1)).min(table.len()) {
            if query
                .filter
                .as_ref()
                .is_some_and(|filter| !eval(filter, table, row).truthy())
            {
                continue;
            }
            let mut key = GroupKey {
                v: [0; 3],
                n: query.group_by.len() as u8,
            };
            for (index, column) in query.group_by.iter().enumerate() {
                key.v[index] = table.raw_key(column, row);
            }
            buckets[(key_hash(key) as usize) & (partitions - 1)].push(row as u64);
        }
        for (partition, rows) in buckets
            .iter()
            .enumerate()
            .filter(|(_, rows)| !rows.is_empty())
        {
            if writers[partition].is_none() {
                let file = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&paths[partition])
                    .map_err(|error| format!("cannot create spill partition: {error}"))?;
                writers[partition] = Some(BufWriter::with_capacity(4096, file));
                created[partition] = true;
                metrics.files_created += 1;
            }
            for row in rows {
                writers[partition]
                    .as_mut()
                    .expect("spill writer opened")
                    .write_all(&row.to_le_bytes())
                    .map_err(|error| format!("cannot write spill partition: {error}"))?;
            }
            metrics.bytes_written += (rows.len() * 8) as u64;
        }
    }
    for writer in writers.iter_mut().flatten() {
        writer
            .flush()
            .map_err(|error| format!("cannot flush spill partition: {error}"))?;
    }
    drop(writers);
    current_query_memory()
        .expect("query memory configured")
        .release(scratch_bytes);
    drop(buckets);

    let template = states(query);
    let group_reservation_bytes = std::mem::size_of::<GroupKey>()
        + template
            .len()
            .saturating_mul(std::mem::size_of::<AggState>())
        + 128;
    let order = query_output_order(query);
    let top_k = query
        .limit
        .expect("spill eligibility requires limit")
        .saturating_add(query.offset);
    let mut rows = Vec::with_capacity(top_k.min(1024));
    let mut read_buffer = vec![0u8; 64 * 1024];
    account_query_memory(read_buffer.len(), "spill read buffer")?;
    for (partition, path) in paths.iter().enumerate() {
        if !created[partition] {
            continue;
        }
        if execution_cancelled() {
            return Err("query cancelled during spill aggregation".into());
        }
        let mut file =
            File::open(path).map_err(|error| format!("cannot read spill partition: {error}"))?;
        let mut groups = HashMap::<GroupKey, Vec<AggState>>::new();
        let mut reservation = QueryReservation::new(memory.clone());
        loop {
            let bytes = file
                .read(&mut read_buffer)
                .map_err(|error| format!("cannot read spill partition: {error}"))?;
            if bytes == 0 {
                break;
            }
            if bytes % 8 != 0 {
                return Err("corrupt spill partition length".into());
            }
            metrics.bytes_read += bytes as u64;
            for encoded in read_buffer[..bytes].chunks_exact(8) {
                let row = usize::try_from(u64::from_le_bytes(encoded.try_into().expect("8 bytes")))
                    .map_err(|_| "spill row index exceeds platform size")?;
                let mut key = GroupKey {
                    v: [0; 3],
                    n: query.group_by.len() as u8,
                };
                for (index, column) in query.group_by.iter().enumerate() {
                    key.v[index] = table.raw_key(column, row);
                }
                let aggregate_states = match groups.entry(key) {
                    std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        if !reservation.try_add(group_reservation_bytes, partition_budget) {
                            return Err(format!(
                                "RESOURCE_EXHAUSTED spill partition {partition} exceeded its {partition_budget} byte memory budget"
                            ));
                        }
                        entry.insert(template.clone())
                    }
                };
                update(aggregate_states, query, table, row);
            }
        }
        for (key, aggregate_states) in groups.drain() {
            let row = group_output_row(query, table, key, &aggregate_states);
            retain_top_k_row(&order, top_k, &mut rows, row)?;
        }
        drop(groups);
        drop(reservation);
    }
    current_query_memory()
        .expect("query memory configured")
        .release(read_buffer.len());
    drop(read_buffer);
    finalize_rows(query, &mut rows);
    Ok((rows, metrics))
}
