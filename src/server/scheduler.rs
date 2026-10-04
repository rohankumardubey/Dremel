use super::*;

#[derive(Clone)]
pub(crate) struct AsyncRequestState {
    pub(crate) phase: &'static str,
    pub(crate) queue_ns: u128,
    pub(crate) execution_ns: u128,
    pub(crate) rows: Vec<Vec<Scalar>>,
    pub(crate) error: String,
}

pub(crate) struct AsyncRequest {
    pub(crate) id: String,
    pub(crate) priority: usize,
    pub(crate) group: String,
    pub(crate) memory_mb: usize,
    pub(crate) submitted: Instant,
    pub(crate) query: Query,
    pub(crate) include_rows: bool,
    pub(crate) control: Arc<ExecutionControl>,
    pub(crate) state: Mutex<AsyncRequestState>,
    pub(crate) changed: Condvar,
}

pub(crate) struct SchedulerState {
    pub(crate) queues: [std::collections::VecDeque<Arc<AsyncRequest>>; 3],
    pub(crate) requests: std::collections::HashMap<String, Arc<AsyncRequest>>,
    pub(crate) active: usize,
    pub(crate) active_by_group: std::collections::HashMap<String, usize>,
    pub(crate) reserved_memory_mb: usize,
    pub(crate) reserved_by_group: std::collections::HashMap<String, usize>,
    pub(crate) schedule_cursor: usize,
    pub(crate) shutdown: bool,
}

pub(crate) struct SchedulerShared {
    pub(crate) state: Mutex<SchedulerState>,
    pub(crate) changed: Condvar,
    pub(crate) execute: Arc<QueryExecutor>,
    pub(crate) max_active: usize,
    pub(crate) queue_capacity: usize,
    pub(crate) memory_mb: usize,
}

pub(crate) type QueryExecutor = dyn Fn(&Query) -> Result<Vec<Vec<Scalar>>, String> + Send + Sync;

pub(crate) struct AsyncScheduler {
    pub(crate) shared: Arc<SchedulerShared>,
    pub(crate) dispatcher: Option<thread::JoinHandle<()>>,
}

pub(crate) struct SubmitOptions<'a> {
    pub(crate) priority: usize,
    pub(crate) group: &'a str,
    pub(crate) deadline_ms: u64,
    pub(crate) memory_mb: usize,
    pub(crate) include_rows: bool,
}

pub(crate) fn finish_without_execution(
    request: &Arc<AsyncRequest>,
    phase: &'static str,
    error: &str,
) {
    if let Ok(mut state) = request.state.lock() {
        state.phase = phase;
        state.queue_ns = request.submitted.elapsed().as_nanos();
        state.error = error.into();
        request.changed.notify_all();
    }
}

impl AsyncScheduler {
    pub(crate) fn new(execute: Arc<QueryExecutor>, options: &Options) -> Self {
        let shared = Arc::new(SchedulerShared {
            state: Mutex::new(SchedulerState {
                queues: std::array::from_fn(|_| std::collections::VecDeque::new()),
                requests: std::collections::HashMap::new(),
                active: 0,
                active_by_group: std::collections::HashMap::new(),
                reserved_memory_mb: 0,
                reserved_by_group: std::collections::HashMap::new(),
                schedule_cursor: 0,
                shutdown: false,
            }),
            changed: Condvar::new(),
            execute,
            max_active: options.max_active_queries.max(1),
            queue_capacity: options.admission_queue_capacity.max(1),
            memory_mb: options.scheduler_memory_mb.max(1),
        });
        let dispatcher_shared = shared.clone();
        let dispatcher = thread::spawn(move || {
            // Priority 2 receives four quanta, priority 1 two, priority 0 one.
            const CYCLE: [usize; 7] = [2, 2, 2, 2, 1, 1, 0];
            loop {
                let request = {
                    let mut scheduler = dispatcher_shared.state.lock().expect("scheduler lock");
                    loop {
                        if scheduler.shutdown {
                            return;
                        }
                        let mut selected = None;
                        if scheduler.active < dispatcher_shared.max_active {
                            for _ in 0..CYCLE.len() {
                                let priority = CYCLE[scheduler.schedule_cursor % CYCLE.len()];
                                scheduler.schedule_cursor += 1;
                                let position =
                                    scheduler.queues[priority].iter().position(|request| {
                                        let group_active = scheduler
                                            .active_by_group
                                            .get(&request.group)
                                            .copied()
                                            .unwrap_or(0);
                                        group_active
                                            < dispatcher_shared.max_active.div_ceil(2).max(1)
                                    });
                                if let Some(position) = position {
                                    selected = scheduler.queues[priority].remove(position);
                                    break;
                                }
                            }
                        }
                        if let Some(request) = selected {
                            if request.control.cancelled.load(AtomicOrdering::Relaxed) {
                                scheduler.reserved_memory_mb = scheduler
                                    .reserved_memory_mb
                                    .saturating_sub(request.memory_mb);
                                if let Some(value) =
                                    scheduler.reserved_by_group.get_mut(&request.group)
                                {
                                    *value = value.saturating_sub(request.memory_mb);
                                }
                                finish_without_execution(
                                    &request,
                                    "cancelled",
                                    "client cancellation",
                                );
                                continue;
                            }
                            if request
                                .control
                                .deadline
                                .is_some_and(|deadline| Instant::now() >= deadline)
                            {
                                scheduler.reserved_memory_mb = scheduler
                                    .reserved_memory_mb
                                    .saturating_sub(request.memory_mb);
                                if let Some(value) =
                                    scheduler.reserved_by_group.get_mut(&request.group)
                                {
                                    *value = value.saturating_sub(request.memory_mb);
                                }
                                finish_without_execution(
                                    &request,
                                    "deadline",
                                    "deadline expired in queue",
                                );
                                continue;
                            }
                            scheduler.active += 1;
                            *scheduler
                                .active_by_group
                                .entry(request.group.clone())
                                .or_default() += 1;
                            break request;
                        }
                        scheduler = dispatcher_shared
                            .changed
                            .wait(scheduler)
                            .expect("scheduler wait");
                    }
                };
                let worker_shared = dispatcher_shared.clone();
                thread::spawn(move || {
                    let started = Instant::now();
                    {
                        let mut state = request.state.lock().expect("request state");
                        state.phase = "running";
                        state.queue_ns = request.submitted.elapsed().as_nanos();
                        request.changed.notify_all();
                    }
                    set_execution_control(Some(request.control.clone()));
                    let previous_memory = current_query_memory();
                    set_query_memory(Some(QueryMemory::new(request.memory_mb)));
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        (worker_shared.execute)(&request.query)
                    }));
                    set_query_memory(previous_memory);
                    set_execution_control(None);
                    let elapsed = started.elapsed().as_nanos();
                    {
                        let mut state = request.state.lock().expect("request state");
                        state.execution_ns = elapsed;
                        if request.control.cancelled.load(AtomicOrdering::Relaxed) {
                            state.phase = "cancelled";
                            state.error = "client cancellation".into();
                        } else if request
                            .control
                            .deadline
                            .is_some_and(|deadline| Instant::now() >= deadline)
                        {
                            state.phase = "deadline";
                            state.error = "deadline expired during execution".into();
                        } else {
                            match result {
                                Ok(Ok(rows)) => {
                                    state.phase = "completed";
                                    if request.include_rows {
                                        state.rows = rows;
                                    } else {
                                        state.rows = vec![vec![Scalar::Int(rows.len() as i64)]];
                                    }
                                }
                                Ok(Err(error)) => {
                                    state.phase = "failed";
                                    state.error = error;
                                }
                                Err(_) => {
                                    state.phase = "failed";
                                    state.error = "execution panic".into();
                                }
                            }
                        }
                        request.changed.notify_all();
                    }
                    let mut scheduler = worker_shared.state.lock().expect("scheduler lock");
                    scheduler.active = scheduler.active.saturating_sub(1);
                    if let Some(value) = scheduler.active_by_group.get_mut(&request.group) {
                        *value = value.saturating_sub(1);
                    }
                    scheduler.reserved_memory_mb = scheduler
                        .reserved_memory_mb
                        .saturating_sub(request.memory_mb);
                    if let Some(value) = scheduler.reserved_by_group.get_mut(&request.group) {
                        *value = value.saturating_sub(request.memory_mb);
                    }
                    worker_shared.changed.notify_all();
                });
            }
        });
        Self {
            shared,
            dispatcher: Some(dispatcher),
        }
    }

    pub(crate) fn submit(
        &self,
        id: &str,
        query: Query,
        options: SubmitOptions<'_>,
    ) -> Result<(), String> {
        let SubmitOptions {
            priority,
            group,
            deadline_ms,
            memory_mb,
            include_rows,
        } = options;
        if priority > 2 {
            return Err("priority must be 0, 1, or 2".into());
        }
        let mut scheduler = self.shared.state.lock().map_err(|e| e.to_string())?;
        if scheduler.requests.contains_key(id) {
            return Err("duplicate request id".into());
        }
        let queued: usize = scheduler
            .queues
            .iter()
            .map(std::collections::VecDeque::len)
            .sum();
        if queued >= self.shared.queue_capacity {
            return Err("ADMISSION_REJECTED queue capacity".into());
        }
        let memory_mb = memory_mb.max(1);
        if scheduler.reserved_memory_mb.saturating_add(memory_mb) > self.shared.memory_mb {
            return Err("ADMISSION_REJECTED global memory".into());
        }
        let group_memory_limit = self.shared.memory_mb.div_ceil(2).max(1);
        if scheduler
            .reserved_by_group
            .get(group)
            .copied()
            .unwrap_or(0)
            .saturating_add(memory_mb)
            > group_memory_limit
        {
            return Err("ADMISSION_REJECTED resource-group memory".into());
        }
        let request = Arc::new(AsyncRequest {
            id: id.into(),
            priority,
            group: group.into(),
            memory_mb,
            submitted: Instant::now(),
            query,
            include_rows,
            control: Arc::new(ExecutionControl {
                cancelled: AtomicBool::new(false),
                deadline: (deadline_ms > 0)
                    .then(|| Instant::now() + Duration::from_millis(deadline_ms)),
            }),
            state: Mutex::new(AsyncRequestState {
                phase: "queued",
                queue_ns: 0,
                execution_ns: 0,
                rows: Vec::new(),
                error: String::new(),
            }),
            changed: Condvar::new(),
        });
        scheduler.reserved_memory_mb += memory_mb;
        *scheduler.reserved_by_group.entry(group.into()).or_default() += memory_mb;
        scheduler.requests.insert(id.into(), request.clone());
        scheduler.queues[priority].push_back(request);
        self.shared.changed.notify_all();
        Ok(())
    }

    pub(crate) fn request(&self, id: &str) -> Result<Arc<AsyncRequest>, String> {
        self.shared
            .state
            .lock()
            .map_err(|e| e.to_string())?
            .requests
            .get(id)
            .cloned()
            .ok_or_else(|| "unknown request".into())
    }

    pub(crate) fn cancel(&self, id: &str) -> Result<(), String> {
        let request = self.request(id)?;
        request
            .control
            .cancelled
            .store(true, AtomicOrdering::Relaxed);
        self.shared.changed.notify_all();
        Ok(())
    }

    pub(crate) fn status(&self, id: &str, wait: bool) -> Result<String, String> {
        let request = self.request(id)?;
        let mut state = request.state.lock().map_err(|e| e.to_string())?;
        while wait && matches!(state.phase, "queued" | "running") {
            state = request.changed.wait(state).map_err(|e| e.to_string())?;
        }
        let row_count = if state.phase == "completed" {
            if request.include_rows {
                state.rows.len()
            } else {
                state
                    .rows
                    .first()
                    .and_then(|row| row.first())
                    .and_then(|value| match value {
                        Scalar::Int(value) => Some(*value as usize),
                        _ => None,
                    })
                    .unwrap_or(0)
            }
        } else {
            0
        };
        let json = if state.phase == "completed" && request.include_rows {
            rows_json(&state.rows)
        } else {
            "[]".into()
        };
        Ok(format!(
            "STATUS\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            request.id,
            state.phase,
            request.priority,
            request.group,
            state.queue_ns,
            state.execution_ns,
            row_count,
            json,
            state.error.replace(['\t', '\n'], " ")
        ))
    }
}

impl Drop for AsyncScheduler {
    fn drop(&mut self) {
        if let Ok(mut scheduler) = self.shared.state.lock() {
            scheduler.shutdown = true;
            for request in scheduler.requests.values() {
                request
                    .control
                    .cancelled
                    .store(true, AtomicOrdering::Relaxed);
            }
            self.shared.changed.notify_all();
        }
        if let Some(dispatcher) = self.dispatcher.take() {
            let _ = dispatcher.join();
        }
    }
}
