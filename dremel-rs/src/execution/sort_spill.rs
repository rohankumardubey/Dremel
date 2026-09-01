use super::row_bytes;
use super::scalar::eval;
use super::spill::SpillDirectory;
use crate::optimizer::filter_always_false;
use crate::relational::{compare_output_rows, query_output_order};
use crate::sql::*;
use crate::storage::Table;
use crate::types::*;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const IO_BUFFER_BYTES: usize = 4096;

pub(crate) fn spillable_sort(query: &Query) -> bool {
    query.from.name == "events"
        && query.joins.is_empty()
        && query.group_by.is_empty()
        && query.having.is_none()
        && query.union.is_none()
        && query.ctes.is_empty()
        && !query.distinct
        && !query.order_by.is_empty()
        && !query.select.iter().any(|item| {
            contains_agg(&item.expr) || contains_window(&item.expr) || contains_subquery(&item.expr)
        })
        && !query.filter.as_ref().is_some_and(contains_subquery)
}

fn push_u32(output: &mut Vec<u8>, value: usize) -> Result<(), String> {
    let value = u32::try_from(value).map_err(|_| "sort spill value exceeds 4 GiB")?;
    output.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn encode_row(row: &[Scalar]) -> Result<Vec<u8>, String> {
    let mut payload = Vec::new();
    push_u32(&mut payload, row.len())?;
    for value in row {
        match value {
            Scalar::Null => payload.push(0),
            Scalar::Int(value) => {
                payload.push(1);
                payload.extend_from_slice(&value.to_le_bytes());
            }
            Scalar::Decimal(value) => {
                payload.push(2);
                payload.extend_from_slice(&value.to_le_bytes());
            }
            Scalar::Float(value) => {
                payload.push(3);
                payload.extend_from_slice(&value.to_bits().to_le_bytes());
            }
            Scalar::Bool(value) => {
                payload.push(4);
                payload.push(u8::from(*value));
            }
            Scalar::Str(value) => {
                payload.push(5);
                push_u32(&mut payload, value.len())?;
                payload.extend_from_slice(value.as_bytes());
            }
        }
    }
    let mut encoded = Vec::with_capacity(payload.len() + 4);
    push_u32(&mut encoded, payload.len())?;
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

fn take<const N: usize>(input: &[u8], offset: &mut usize) -> Result<[u8; N], String> {
    let end = (*offset).saturating_add(N);
    let bytes = input.get(*offset..end).ok_or("corrupt external sort row")?;
    *offset = end;
    Ok(bytes.try_into().expect("fixed-size slice"))
}

fn decode_row(payload: &[u8]) -> Result<Vec<Scalar>, String> {
    let mut offset = 0;
    let columns = u32::from_le_bytes(take::<4>(payload, &mut offset)?) as usize;
    let mut row = Vec::with_capacity(columns);
    for _ in 0..columns {
        let tag = take::<1>(payload, &mut offset)?[0];
        row.push(match tag {
            0 => Scalar::Null,
            1 => Scalar::Int(i64::from_le_bytes(take::<8>(payload, &mut offset)?)),
            2 => Scalar::Decimal(i64::from_le_bytes(take::<8>(payload, &mut offset)?)),
            3 => Scalar::Float(f64::from_bits(u64::from_le_bytes(take::<8>(
                payload,
                &mut offset,
            )?))),
            4 => Scalar::Bool(take::<1>(payload, &mut offset)?[0] != 0),
            5 => {
                let length = u32::from_le_bytes(take::<4>(payload, &mut offset)?) as usize;
                let end = offset.saturating_add(length);
                let bytes = payload
                    .get(offset..end)
                    .ok_or("corrupt external sort string")?;
                offset = end;
                Scalar::Str(
                    std::str::from_utf8(bytes)
                        .map_err(|_| "corrupt UTF-8 in external sort row")?
                        .to_owned(),
                )
            }
            _ => return Err("corrupt external sort scalar tag".into()),
        });
    }
    if offset != payload.len() {
        return Err("corrupt external sort row length".into());
    }
    Ok(row)
}

fn read_row(
    reader: &mut BufReader<File>,
    memory: &Arc<QueryMemory>,
) -> Result<Option<(Vec<Scalar>, u64)>, String> {
    let mut length = [0u8; 4];
    match reader.read(&mut length[..1]) {
        Ok(0) => return Ok(None),
        Ok(1) => {}
        Ok(_) => unreachable!("one-byte read"),
        Err(error) => return Err(format!("cannot read external sort run: {error}")),
    }
    reader
        .read_exact(&mut length[1..])
        .map_err(|error| format!("corrupt external sort record length: {error}"))?;
    let length = u32::from_le_bytes(length) as usize;
    memory.account(length, "external sort encoded merge row")?;
    let mut payload = vec![0u8; length];
    let read = reader
        .read_exact(&mut payload)
        .map_err(|error| format!("cannot read external sort run: {error}"));
    let decoded = read.and_then(|()| decode_row(&payload));
    memory.release(length);
    Ok(Some((decoded?, (length + 4) as u64)))
}

fn write_run(
    workspace: &Path,
    run_number: usize,
    query: &Query,
    rows: &mut Vec<Vec<Scalar>>,
    memory: &Arc<QueryMemory>,
    reserved: &mut usize,
    metrics: &mut SpillMetrics,
) -> Result<PathBuf, String> {
    let order = query_output_order(query);
    rows.sort_by(|left, right| compare_output_rows(left, right, &order));
    let path = workspace.join(format!("sort-run-{run_number:04}.bin"));
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(|error| format!("cannot create external sort run: {error}"))?;
    memory.account(IO_BUFFER_BYTES, "external sort write buffer")?;
    let result = (|| {
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
        for row in rows.iter() {
            let encoded = encode_row(row)?;
            memory.account(encoded.len(), "external sort row encoding")?;
            let write = writer
                .write_all(&encoded)
                .map_err(|error| format!("cannot write external sort run: {error}"));
            memory.release(encoded.len());
            write?;
            metrics.bytes_written = metrics.bytes_written.saturating_add(encoded.len() as u64);
        }
        writer
            .flush()
            .map_err(|error| format!("cannot flush external sort run: {error}"))
    })();
    memory.release(IO_BUFFER_BYTES);
    result?;
    memory.release(*reserved);
    *reserved = 0;
    rows.clear();
    metrics.files_created += 1;
    Ok(path)
}

struct RunCursor {
    reader: BufReader<File>,
    current: Option<Vec<Scalar>>,
    current_bytes: usize,
    memory: Arc<QueryMemory>,
    bytes_read: u64,
}

impl RunCursor {
    fn open(path: &Path, memory: Arc<QueryMemory>) -> Result<Self, String> {
        let file =
            File::open(path).map_err(|error| format!("cannot open external sort run: {error}"))?;
        memory.account(IO_BUFFER_BYTES, "external sort read buffer")?;
        let mut cursor = Self {
            reader: BufReader::with_capacity(IO_BUFFER_BYTES, file),
            current: None,
            current_bytes: 0,
            memory,
            bytes_read: 0,
        };
        cursor.advance()?;
        Ok(cursor)
    }

    fn advance(&mut self) -> Result<(), String> {
        self.memory.release(self.current_bytes);
        self.current_bytes = 0;
        self.current = None;
        if let Some((row, bytes_read)) = read_row(&mut self.reader, &self.memory)? {
            let bytes = row_bytes(&row);
            self.memory.account(bytes, "external sort merge row")?;
            self.current_bytes = bytes;
            self.current = Some(row);
            self.bytes_read = self.bytes_read.saturating_add(bytes_read);
        }
        Ok(())
    }
}

impl Drop for RunCursor {
    fn drop(&mut self) {
        self.memory.release(self.current_bytes + IO_BUFFER_BYTES);
    }
}

fn heap_precedes(
    cursors: &[RunCursor],
    left: usize,
    right: usize,
    order: &[(usize, &OrderSpec)],
) -> bool {
    let comparison = compare_output_rows(
        cursors[left].current.as_ref().expect("heap row"),
        cursors[right].current.as_ref().expect("heap row"),
        order,
    );
    comparison.is_lt() || (comparison.is_eq() && left < right)
}

fn heap_push(
    heap: &mut Vec<usize>,
    cursors: &[RunCursor],
    index: usize,
    order: &[(usize, &OrderSpec)],
) {
    heap.push(index);
    let mut child = heap.len() - 1;
    while child > 0 {
        let parent = (child - 1) / 2;
        if !heap_precedes(cursors, heap[child], heap[parent], order) {
            break;
        }
        heap.swap(child, parent);
        child = parent;
    }
}

fn heap_pop(
    heap: &mut Vec<usize>,
    cursors: &[RunCursor],
    order: &[(usize, &OrderSpec)],
) -> Option<usize> {
    let result = heap.first().copied()?;
    let tail = heap.pop().expect("non-empty heap");
    if !heap.is_empty() {
        heap[0] = tail;
        let mut parent = 0;
        loop {
            let left = parent * 2 + 1;
            if left >= heap.len() {
                break;
            }
            let right = left + 1;
            let child =
                if right < heap.len() && heap_precedes(cursors, heap[right], heap[left], order) {
                    right
                } else {
                    left
                };
            if !heap_precedes(cursors, heap[child], heap[parent], order) {
                break;
            }
            heap.swap(child, parent);
            parent = child;
        }
    }
    Some(result)
}

fn merge_runs_to_file(
    paths: &[PathBuf],
    output: &Path,
    query: &Query,
    memory: &Arc<QueryMemory>,
    metrics: &mut SpillMetrics,
) -> Result<(), String> {
    let mut cursors = paths
        .iter()
        .map(|path| RunCursor::open(path, memory.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    let order = query_output_order(query);
    let mut heap = Vec::with_capacity(cursors.len());
    for index in 0..cursors.len() {
        if cursors[index].current.is_some() {
            heap_push(&mut heap, &cursors, index, &order);
        }
    }
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output)
        .map_err(|error| format!("cannot create external sort merge run: {error}"))?;
    memory.account(IO_BUFFER_BYTES, "external sort merge write buffer")?;
    let result = (|| {
        let mut writer = BufWriter::with_capacity(IO_BUFFER_BYTES, file);
        while let Some(index) = heap_pop(&mut heap, &cursors, &order) {
            let encoded = encode_row(cursors[index].current.as_ref().expect("merge row"))?;
            memory.account(encoded.len(), "external sort merge encoding")?;
            let write = writer
                .write_all(&encoded)
                .map_err(|error| format!("cannot write external sort merge run: {error}"));
            memory.release(encoded.len());
            write?;
            metrics.bytes_written = metrics.bytes_written.saturating_add(encoded.len() as u64);
            cursors[index].advance()?;
            if cursors[index].current.is_some() {
                heap_push(&mut heap, &cursors, index, &order);
            }
        }
        writer
            .flush()
            .map_err(|error| format!("cannot flush external sort merge run: {error}"))
    })();
    memory.release(IO_BUFFER_BYTES);
    metrics.bytes_read = metrics
        .bytes_read
        .saturating_add(cursors.iter().map(|cursor| cursor.bytes_read).sum());
    result?;
    metrics.files_created += 1;
    Ok(())
}

pub(crate) fn stream_external_sort<Sink>(
    query: &Query,
    table: &Table,
    batch_size: usize,
    spill_root: &str,
    mut sink: Sink,
) -> Result<(ResultStreamMetrics, SpillMetrics), String>
where
    Sink: FnMut(&[Scalar]) -> Result<usize, String>,
{
    if query.limit == Some(0) {
        return Ok((ResultStreamMetrics::default(), SpillMetrics::default()));
    }
    let memory = current_query_memory().ok_or("external sort requires query memory accounting")?;
    let available = memory
        .limit_bytes()
        .saturating_sub(memory.accounted_bytes());
    if memory.limit_bytes() == 0 || available < 1024 * 1024 {
        return Err(
            "RESOURCE_EXHAUSTED external sort requires at least 1 MiB of available query memory"
                .into(),
        );
    }
    let run_budget = available / 2;
    let workspace = SpillDirectory::create(spill_root)?;
    let mut paths = Vec::new();
    let mut rows = Vec::new();
    let mut reserved = 0usize;
    let mut largest_row = 0usize;
    let mut stream = ResultStreamMetrics::default();
    let mut spill = SpillMetrics {
        passes: 1,
        ..SpillMetrics::default()
    };
    if !filter_always_false(query.filter.as_ref()) {
        for start in (0..table.len()).step_by(batch_size.max(1)) {
            if execution_cancelled() {
                return Err("query cancelled during external sort run generation".into());
            }
            stream.batches_scanned += 1;
            for row_index in start..(start + batch_size.max(1)).min(table.len()) {
                if query
                    .filter
                    .as_ref()
                    .is_some_and(|filter| !eval(filter, table, row_index).truthy())
                {
                    continue;
                }
                let row = query
                    .select
                    .iter()
                    .map(|item| eval(&item.expr, table, row_index))
                    .collect::<Vec<_>>();
                let bytes = row_bytes(&row);
                largest_row = largest_row.max(bytes);
                if !rows.is_empty() && reserved.saturating_add(bytes) > run_budget {
                    let path = write_run(
                        workspace.path(),
                        paths.len(),
                        query,
                        &mut rows,
                        &memory,
                        &mut reserved,
                        &mut spill,
                    )?;
                    paths.push(path);
                }
                if bytes > run_budget {
                    return Err(format!(
                        "RESOURCE_EXHAUSTED external sort row requires {bytes} bytes; run budget is {run_budget} bytes"
                    ));
                }
                memory.account(bytes, "external sort run buffer")?;
                reserved += bytes;
                rows.push(row);
            }
        }
    }
    if !rows.is_empty() {
        let path = write_run(
            workspace.path(),
            paths.len(),
            query,
            &mut rows,
            &memory,
            &mut reserved,
            &mut spill,
        )?;
        paths.push(path);
    }
    spill.partitions = paths.len();
    if paths.is_empty() {
        spill.passes = 0;
        return Ok((stream, spill));
    }
    let cursor_budget = available / 2;
    let bytes_per_cursor = IO_BUFFER_BYTES.saturating_add(largest_row.max(1));
    let fan_in = (cursor_budget / bytes_per_cursor).min(32);
    if paths.len() > 1 && fan_in < 2 {
        return Err(
            "RESOURCE_EXHAUSTED external sort cannot merge two rows within the query memory limit"
                .into(),
        );
    }
    while paths.len() > fan_in.max(1) {
        let mut next_paths = Vec::new();
        for (group, chunk) in paths.chunks(fan_in).enumerate() {
            let output = workspace
                .path()
                .join(format!("sort-merge-{:02}-{group:04}.bin", spill.passes));
            merge_runs_to_file(chunk, &output, query, &memory, &mut spill)?;
            next_paths.push(output);
        }
        for path in &paths {
            std::fs::remove_file(path)
                .map_err(|error| format!("cannot remove external sort run: {error}"))?;
        }
        paths = next_paths;
        spill.passes += 1;
    }
    let mut cursors = paths
        .iter()
        .map(|path| RunCursor::open(path, memory.clone()))
        .collect::<Result<Vec<_>, _>>()?;
    let order = query_output_order(query);
    let mut heap = Vec::with_capacity(cursors.len());
    for index in 0..cursors.len() {
        if cursors[index].current.is_some() {
            heap_push(&mut heap, &cursors, index, &order);
        }
    }
    let mut remaining_offset = query.offset;
    let mut remaining_limit = query.limit;
    loop {
        if execution_cancelled() {
            return Err("query cancelled during external sort merge".into());
        }
        let Some(index) = heap_pop(&mut heap, &cursors, &order) else {
            break;
        };
        if remaining_offset > 0 {
            remaining_offset -= 1;
        } else if remaining_limit != Some(0) {
            let row = cursors[index].current.as_ref().expect("merge row");
            stream.output_bytes = stream.output_bytes.saturating_add(sink(row)? as u64);
            stream.rows_returned += 1;
            if let Some(limit) = &mut remaining_limit {
                *limit -= 1;
            }
        }
        cursors[index].advance()?;
        if cursors[index].current.is_some() {
            heap_push(&mut heap, &cursors, index, &order);
        }
        if remaining_limit == Some(0) {
            break;
        }
    }
    spill.bytes_read = spill
        .bytes_read
        .saturating_add(cursors.iter().map(|cursor| cursor.bytes_read).sum());
    spill.passes += 1;
    Ok((stream, spill))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spill_row_codec_round_trips_every_scalar_type() {
        let row = vec![
            Scalar::Null,
            Scalar::Int(i64::MIN),
            Scalar::Decimal(-12345),
            Scalar::Float(-0.0),
            Scalar::Bool(true),
            Scalar::Str("line\n\t\"\\ and unicode \u{2603}".into()),
        ];
        let encoded = encode_row(&row).expect("encode");
        let payload_length = u32::from_le_bytes(encoded[..4].try_into().unwrap()) as usize;
        assert_eq!(payload_length + 4, encoded.len());
        assert_eq!(decode_row(&encoded[4..]).expect("decode"), row);
    }

    #[test]
    fn spill_row_codec_rejects_truncated_input() {
        assert!(decode_row(&[1, 0, 0, 0, 1]).is_err());
    }
}
