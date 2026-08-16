use crate::execution::scalar::{cmp, eval};
use crate::optimizer::filter_always_false;
use crate::sql::*;
use crate::storage::Table;
use crate::types::*;
use std::cmp::Ordering;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

#[derive(Clone)]
pub(crate) struct SumState {
    pub(crate) value: f64,
    pub(crate) decimal_units: i128,
    pub(crate) floating: bool,
    pub(crate) decimal: bool,
    pub(crate) has: bool,
}
impl SumState {
    pub(crate) fn new() -> Self {
        Self {
            value: 0.0,
            decimal_units: 0,
            floating: false,
            decimal: false,
            has: false,
        }
    }
    pub(crate) fn add(&mut self, value: &Scalar) {
        match value {
            Scalar::Decimal(units) if !self.floating => {
                if !self.decimal {
                    self.decimal_units = (self.value as i128) * 100;
                    self.decimal = true;
                }
                self.decimal_units += i128::from(*units);
                self.has = true;
            }
            Scalar::Int(value) if self.decimal && !self.floating => {
                self.decimal_units += i128::from(*value) * 100;
                self.has = true;
            }
            value if value.number().is_some() => {
                if self.decimal {
                    self.value = self.decimal_units as f64 / 100.0;
                    self.decimal = false;
                }
                self.value += value.number().expect("numeric");
                self.floating |= matches!(value, Scalar::Float(_));
                self.has = true;
            }
            _ => {}
        }
    }
    pub(crate) fn merge(&mut self, other: &Self) {
        if !other.has {
            return;
        }
        if self.decimal && other.decimal && !self.floating && !other.floating {
            self.decimal_units += other.decimal_units;
            self.has = true;
            return;
        }
        if !self.has && other.decimal && !other.floating {
            *self = other.clone();
            return;
        }
        if self.decimal {
            self.value = self.decimal_units as f64 / 100.0;
            self.decimal = false;
        }
        self.value += if other.decimal {
            other.decimal_units as f64 / 100.0
        } else {
            other.value
        };
        self.floating |= other.floating;
        self.has = true;
    }
    pub(crate) fn finish(&self) -> Scalar {
        if !self.has {
            Scalar::Null
        } else if self.decimal && !self.floating {
            i64::try_from(self.decimal_units).map_or(Scalar::Null, Scalar::Decimal)
        } else if self.floating {
            Scalar::Float(self.value)
        } else {
            Scalar::Int(self.value as i64)
        }
    }
}

#[derive(Clone)]
pub(crate) enum AggState {
    Count(u64),
    Sum(SumState),
    Avg { sum: f64, count: u64 },
    Min(Option<Scalar>),
    Max(Option<Scalar>),
}
pub(crate) fn states(q: &Query) -> Vec<AggState> {
    q.select
        .iter()
        .filter(|s| is_agg(&s.expr))
        .map(|s| match &s.expr {
            Expr::Func(n, _) if n == "count" => AggState::Count(0),
            Expr::Func(n, _) if n == "sum" => AggState::Sum(SumState::new()),
            Expr::Func(n, _) if n == "avg" => AggState::Avg { sum: 0.0, count: 0 },
            Expr::Func(n, _) if n == "min" => AggState::Min(None),
            Expr::Func(_, _) => AggState::Max(None),
            _ => unreachable!(),
        })
        .collect()
}
pub(crate) fn update(st: &mut [AggState], q: &Query, t: &Table, i: usize) {
    for (state, item) in st
        .iter_mut()
        .zip(q.select.iter().filter(|s| is_agg(&s.expr)))
    {
        let Expr::Func(_, arg) = &item.expr else {
            continue;
        };
        let v = eval(arg, t, i);
        match state {
            AggState::Count(c) => {
                if matches!(**arg, Expr::Star) || !matches!(v, Scalar::Null) {
                    *c += 1
                }
            }
            AggState::Sum(sum) => sum.add(&v),
            AggState::Avg { sum, count } => {
                if let Some(n) = v.number() {
                    *sum += n;
                    *count += 1
                }
            }
            AggState::Min(x) => {
                if !matches!(v, Scalar::Null)
                    && (x.is_none()
                        || cmp(&v, x.as_ref().expect("present")) == Some(Ordering::Less))
                {
                    *x = Some(v)
                }
            }
            AggState::Max(x) => {
                if !matches!(v, Scalar::Null)
                    && (x.is_none()
                        || cmp(&v, x.as_ref().expect("present")) == Some(Ordering::Greater))
                {
                    *x = Some(v)
                }
            }
        }
    }
}
pub(crate) fn merge(a: &mut [AggState], b: &[AggState]) {
    for (x, y) in a.iter_mut().zip(b) {
        match (x, y) {
            (AggState::Count(a), AggState::Count(b)) => *a += b,
            (AggState::Sum(a), AggState::Sum(b)) => a.merge(b),
            (AggState::Avg { sum: a, count: ac }, AggState::Avg { sum: b, count: bc }) => {
                *a += b;
                *ac += bc
            }
            (AggState::Min(a), AggState::Min(Some(v)))
                if a.is_none() || cmp(v, a.as_ref().expect("present")) == Some(Ordering::Less) =>
            {
                *a = Some(v.clone())
            }
            (AggState::Max(a), AggState::Max(Some(v)))
                if a.is_none()
                    || cmp(v, a.as_ref().expect("present")) == Some(Ordering::Greater) =>
            {
                *a = Some(v.clone())
            }
            _ => {}
        }
    }
}
pub(crate) fn finish(s: &AggState) -> Scalar {
    match s {
        AggState::Count(v) => Scalar::Int(*v as i64),
        AggState::Sum(sum) => sum.finish(),
        AggState::Avg { sum, count } => {
            if *count == 0 {
                Scalar::Null
            } else {
                Scalar::Float(*sum / (*count as f64))
            }
        }
        AggState::Min(v) | AggState::Max(v) => v.clone().unwrap_or(Scalar::Null),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct GroupKey {
    pub(crate) v: [u64; 3],
    pub(crate) n: u8,
}
pub(crate) fn hash64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D049BB133111EB);
    x ^ (x >> 31)
}
pub(crate) fn key_hash(k: GroupKey) -> u64 {
    let mut h = 0x243F6A8885A308D3;
    for i in 0..k.n as usize {
        h = hash64(h ^ hash64(k.v[i].wrapping_add((i as u64) << 32)))
    }
    h
}
#[derive(Clone)]
pub(crate) struct Entry {
    pub(crate) k: GroupKey,
    pub(crate) s: Vec<AggState>,
}
pub(crate) struct GroupTable {
    pub(crate) slots: Vec<Option<Entry>>,
    pub(crate) len: usize,
}
impl GroupTable {
    pub(crate) fn new() -> Result<Self, String> {
        account_query_memory(
            16usize.saturating_mul(std::mem::size_of::<Option<Entry>>()),
            "hash aggregation table",
        )?;
        Ok(Self {
            slots: vec![None; 16],
            len: 0,
        })
    }
    pub(crate) fn find(&self, k: GroupKey) -> usize {
        let mut i = (key_hash(k) as usize) & (self.slots.len() - 1);
        loop {
            match &self.slots[i] {
                None => return i,
                Some(e) if e.k == k => return i,
                _ => i = (i + 1) & (self.slots.len() - 1),
            }
        }
    }
    pub(crate) fn grow(&mut self) -> Result<(), String> {
        let next_capacity = self.slots.len() * 2;
        account_query_memory(
            next_capacity.saturating_mul(std::mem::size_of::<Option<Entry>>()),
            "hash aggregation table growth",
        )?;
        let old = std::mem::replace(&mut self.slots, vec![None; next_capacity]);
        self.len = 0;
        for e in old.into_iter().flatten() {
            let i = self.find(e.k);
            self.slots[i] = Some(e);
            self.len += 1
        }
        Ok(())
    }
    pub(crate) fn get_or_insert(
        &mut self,
        k: GroupKey,
        template: &[AggState],
    ) -> Result<&mut Vec<AggState>, String> {
        if (self.len + 1) * 10 > self.slots.len() * 7 {
            self.grow()?
        }
        let i = self.find(k);
        if self.slots[i].is_none() {
            account_query_memory(
                std::mem::size_of::<Entry>().saturating_add(
                    template
                        .len()
                        .saturating_mul(std::mem::size_of::<AggState>()),
                ),
                "hash aggregation group",
            )?;
            self.slots[i] = Some(Entry {
                k,
                s: template.to_vec(),
            });
            self.len += 1
        }
        Ok(&mut self.slots[i].as_mut().expect("inserted").s)
    }
    pub(crate) fn into_entries(self) -> Vec<Entry> {
        if !account_query_memory_or_stop(
            self.len.saturating_mul(std::mem::size_of::<Entry>()),
            "aggregation merge buffer",
        ) {
            return Vec::new();
        }
        self.slots.into_iter().flatten().collect()
    }
}
pub(crate) fn partition(
    q: &Query,
    t: &Table,
    start: usize,
    end: usize,
    batch: usize,
) -> Result<GroupTable, String> {
    let template = states(q);
    let mut groups = GroupTable::new()?;
    if q.group_by.is_empty() {
        groups.get_or_insert(GroupKey { v: [0; 3], n: 0 }, &template)?;
    }
    if filter_always_false(q.filter.as_ref()) {
        return Ok(groups);
    }
    account_query_memory(
        batch.max(1).saturating_mul(std::mem::size_of::<usize>()),
        "aggregation selection",
    )?;
    let mut selection = Vec::with_capacity(batch.max(1));
    for bs in (start..end).step_by(batch.max(1)) {
        let be = (bs + batch).min(end);
        selection.clear();
        for i in bs..be {
            if q.filter.as_ref().is_none_or(|f| eval(f, t, i).truthy()) {
                selection.push(i);
            }
        }
        for &i in &selection {
            let mut k = GroupKey {
                v: [0; 3],
                n: q.group_by.len() as u8,
            };
            for (j, c) in q.group_by.iter().enumerate() {
                k.v[j] = t.raw_key(c, i)
            }
            let s = groups.get_or_insert(k, &template)?;
            update(s, q, t, i)
        }
    }
    Ok(groups)
}

pub(crate) struct Job {
    pub(crate) partition_id: usize,
    pub(crate) q: Arc<Query>,
    pub(crate) t: Arc<Table>,
    pub(crate) start: usize,
    pub(crate) end: usize,
    pub(crate) batch: usize,
    pub(crate) memory: Option<Arc<QueryMemory>>,
    pub(crate) reply: mpsc::Sender<(usize, Result<GroupTable, String>)>,
}
pub struct Pool {
    pub(crate) tx: Option<mpsc::Sender<Job>>,
    pub(crate) workers: Vec<thread::JoinHandle<()>>,
    pub(crate) threads: usize,
}
impl Pool {
    pub(crate) fn new(n: usize) -> Self {
        let n = n.max(1);
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        let mut workers = Vec::new();
        for _ in 0..n {
            let rx = rx.clone();
            workers.push(thread::spawn(move || {
                loop {
                    let job = {
                        let Ok(lock) = rx.lock() else { break };
                        lock.recv()
                    };
                    let Ok(j) = job else { break };
                    let previous_memory = current_query_memory();
                    set_query_memory(j.memory.clone());
                    let out = partition(&j.q, &j.t, j.start, j.end, j.batch);
                    set_query_memory(previous_memory);
                    let _ = j.reply.send((j.partition_id, out));
                }
            }));
        }
        Self {
            tx: Some(tx),
            workers,
            threads: n,
        }
    }
    pub(crate) fn aggregate(
        &self,
        q: Arc<Query>,
        t: Arc<Table>,
        batch: usize,
    ) -> Result<GroupTable, String> {
        let parts = (self.threads * 4).min(t.len().div_ceil(batch.max(1)).max(1));
        let (tx, rx) = mpsc::channel();
        for p in 0..parts {
            let n = t.len();
            let j = Job {
                partition_id: p,
                q: q.clone(),
                t: t.clone(),
                start: p * n / parts,
                end: (p + 1) * n / parts,
                batch,
                memory: current_query_memory(),
                reply: tx.clone(),
            };
            self.tx
                .as_ref()
                .expect("pool alive")
                .send(j)
                .expect("worker alive")
        }
        drop(tx);
        let template = states(&q);
        let mut final_t = GroupTable::new()?;
        let mut completed: Vec<_> = rx.into_iter().collect();
        completed.sort_by_key(|(partition_id, _)| *partition_id);
        for (_, part) in completed {
            for e in part?.into_entries() {
                let target = final_t.get_or_insert(e.k, &template)?;
                merge(target, &e.s)
            }
        }
        Ok(final_t)
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        self.tx.take();
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
    }
}
