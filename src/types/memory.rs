use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) struct QueryMemory {
    limit_bytes: usize,
    accounted_bytes: AtomicUsize,
    peak_accounted_bytes: AtomicUsize,
    failed: AtomicBool,
    error: Mutex<Option<String>>,
}

impl QueryMemory {
    pub(crate) fn new(limit_mb: usize) -> Arc<Self> {
        Arc::new(Self {
            limit_bytes: limit_mb.saturating_mul(1024 * 1024),
            accounted_bytes: AtomicUsize::new(0),
            peak_accounted_bytes: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
            error: Mutex::new(None),
        })
    }

    pub(crate) fn account(&self, bytes: usize, operator: &str) -> Result<(), String> {
        if bytes == 0 {
            return Ok(());
        }
        loop {
            let current = self.accounted_bytes.load(Ordering::Relaxed);
            let Some(next) = current.checked_add(bytes) else {
                return self.fail(format!(
                    "RESOURCE_EXHAUSTED query memory accounting overflow in {operator}"
                ));
            };
            if self.limit_bytes > 0 && next > self.limit_bytes {
                let error = format!(
                    "RESOURCE_EXHAUSTED {operator} has {current} accounted bytes and requested {bytes} more; limit is {} bytes",
                    self.limit_bytes
                );
                return self.fail(error);
            }
            if self
                .accounted_bytes
                .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.peak_accounted_bytes.fetch_max(next, Ordering::Relaxed);
                return Ok(());
            }
        }
    }

    pub(crate) fn try_account(&self, bytes: usize) -> bool {
        if bytes == 0 {
            return true;
        }
        loop {
            let current = self.accounted_bytes.load(Ordering::Relaxed);
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if self.limit_bytes > 0 && next > self.limit_bytes {
                return false;
            }
            if self
                .accounted_bytes
                .compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.peak_accounted_bytes.fetch_max(next, Ordering::Relaxed);
                return true;
            }
        }
    }

    pub(crate) fn release(&self, bytes: usize) {
        let mut current = self.accounted_bytes.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_sub(bytes);
            match self.accounted_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }

    fn fail(&self, error: String) -> Result<(), String> {
        if let Ok(mut stored) = self.error.lock()
            && stored.is_none()
        {
            *stored = Some(error.clone());
        }
        self.failed.store(true, Ordering::Relaxed);
        Err(error)
    }

    pub(crate) fn accounted_bytes(&self) -> usize {
        self.accounted_bytes.load(Ordering::Relaxed)
    }

    pub(crate) fn limit_bytes(&self) -> usize {
        self.limit_bytes
    }

    pub(crate) fn peak_accounted_bytes(&self) -> usize {
        self.peak_accounted_bytes.load(Ordering::Relaxed)
    }

    fn error(&self) -> Option<String> {
        self.error.lock().ok().and_then(|error| error.clone())
    }

    fn failed(&self) -> bool {
        self.failed.load(Ordering::Relaxed)
    }
}

thread_local! {
    static QUERY_MEMORY: RefCell<Option<Arc<QueryMemory>>> = const { RefCell::new(None) };
}

pub(crate) fn current_query_memory() -> Option<Arc<QueryMemory>> {
    QUERY_MEMORY.with(|slot| slot.borrow().clone())
}

pub(crate) fn set_query_memory(memory: Option<Arc<QueryMemory>>) {
    QUERY_MEMORY.with(|slot| *slot.borrow_mut() = memory);
}

pub(crate) fn account_query_memory(bytes: usize, operator: &str) -> Result<(), String> {
    QUERY_MEMORY.with(|slot| {
        slot.borrow()
            .as_ref()
            .map_or(Ok(()), |memory| memory.account(bytes, operator))
    })
}

pub(crate) fn account_query_memory_or_stop(bytes: usize, operator: &str) -> bool {
    account_query_memory(bytes, operator).is_ok()
}

pub(crate) fn query_memory_failed() -> bool {
    QUERY_MEMORY.with(|memory| memory.borrow().as_ref().is_some_and(|m| m.failed()))
}

pub(crate) fn with_query_memory<T>(
    limit_mb: usize,
    operation: impl FnOnce() -> Result<T, String>,
) -> (Result<T, String>, Arc<QueryMemory>) {
    let memory = QueryMemory::new(limit_mb);
    let previous = current_query_memory();
    set_query_memory(Some(memory.clone()));
    let mut result = operation();
    set_query_memory(previous);
    if result.is_ok()
        && let Some(error) = memory.error()
    {
        result = Err(error);
    }
    (result, memory)
}
