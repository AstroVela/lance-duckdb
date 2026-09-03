use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};

/// Process-local gate for commits issued through this extension.
///
/// Lance protects each individual manifest commit with optimistic concurrency,
/// but DuckDB catalog commit happens after a transaction-local Lance snapshot
/// is published. The publishing transaction retains this guard until DuckDB's
/// commit succeeds or Lance compensation completes, so another extension write
/// cannot advance the manifest in that interval.
pub(crate) struct WriteGuard {
    gate: Arc<WriteGate>,
    mode: GuardMode,
}

struct WriteGate {
    state: Mutex<GateState>,
    ready: Condvar,
}

struct GateState {
    readers: usize,
    writer: bool,
    waiting_writers: usize,
}

enum GuardMode {
    Shared,
    Exclusive,
}

impl WriteGate {
    fn new() -> Self {
        Self {
            state: Mutex::new(GateState {
                readers: 0,
                writer: false,
                waiting_writers: 0,
            }),
            ready: Condvar::new(),
        }
    }

    fn lock_state(&self) -> MutexGuard<'_, GateState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn acquire_shared(self: &Arc<Self>) -> WriteGuard {
        let mut state = self.lock_state();
        while state.writer || state.waiting_writers > 0 {
            state = self
                .ready
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        state.readers += 1;
        WriteGuard {
            gate: self.clone(),
            mode: GuardMode::Shared,
        }
    }

    fn acquire_exclusive(self: &Arc<Self>) -> WriteGuard {
        let mut state = self.lock_state();
        state.waiting_writers += 1;
        while state.writer || state.readers > 0 {
            state = self
                .ready
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        state.waiting_writers -= 1;
        state.writer = true;
        WriteGuard {
            gate: self.clone(),
            mode: GuardMode::Exclusive,
        }
    }
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        let mut state = self.gate.lock_state();
        match self.mode {
            GuardMode::Shared => state.readers -= 1,
            GuardMode::Exclusive => state.writer = false,
        }
        self.gate.ready.notify_all();
    }
}

fn global_gate() -> &'static Arc<WriteGate> {
    static GATE: OnceLock<Arc<WriteGate>> = OnceLock::new();
    GATE.get_or_init(|| Arc::new(WriteGate::new()))
}

/// Allow ordinary Lance writes to run concurrently while ensuring they cannot
/// enter a transaction-local publication's compensation window.
pub(crate) fn acquire_shared_write_guard() -> WriteGuard {
    global_gate().acquire_shared()
}

/// Exclude every other extension write until DuckDB commit or compensation has
/// finished.
pub(crate) fn acquire_exclusive_write_guard() -> WriteGuard {
    global_gate().acquire_exclusive()
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn an_exclusive_writer_blocks_shared_writers() {
        let gate = Arc::new(WriteGate::new());
        let first = gate.acquire_exclusive();
        let (started_tx, started_rx) = mpsc::channel();
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let worker_gate = gate.clone();
        let worker = thread::spawn(move || {
            started_tx.send(()).unwrap();
            let _second = worker_gate.acquire_shared();
            acquired_tx.send(()).unwrap();
        });

        started_rx.recv().unwrap();
        assert!(matches!(
            acquired_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        drop(first);
        acquired_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        worker.join().unwrap();
    }
}
