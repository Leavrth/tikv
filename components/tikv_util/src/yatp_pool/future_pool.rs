// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This mod implemented a wrapped future pool that supports `on_tick()` which
//! is invoked no less than the specific interval.

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use fail::fail_point;
use futures::channel::oneshot::{self, Canceled};
use futures_util::future::FutureExt;
use prometheus::{IntCounter, IntGauge};
use tracker::TlsTrackedFuture;
use yatp::{queue::Extras, task::future};

use crate::resource_control::{TaskPriority, priority_from_task_meta};

pub type ThreadPool = yatp::ThreadPool<future::TaskCell>;

use super::metrics;

#[derive(Clone)]
struct Env {
    metrics_running_task_count_by_priority: [IntGauge; TaskPriority::PRIORITY_COUNT],
    metrics_handled_task_count: IntCounter,
}

#[derive(Clone)]
// FuturePool wraps a yatp thread pool providing task count metrics and gate
// maximum running tasks.
pub struct FuturePool {
    inner: Arc<PoolInner>,
}

impl std::fmt::Debug for FuturePool {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "FuturePool")
    }
}

impl crate::AssertSend for FuturePool {}
impl crate::AssertSync for FuturePool {}

impl FuturePool {
    pub fn from_pool(pool: ThreadPool, name: &str, pool_size: usize, max_tasks: usize) -> Self {
        let env = Env {
            metrics_running_task_count_by_priority: TaskPriority::priorities().map(|p| {
                metrics::FUTUREPOOL_RUNNING_TASK_VEC.with_label_values(&[name, p.as_str()])
            }),
            metrics_handled_task_count: metrics::FUTUREPOOL_HANDLED_TASK_VEC
                .with_label_values(&[name]),
        };
        FuturePool {
            inner: Arc::new(PoolInner {
                pool,
                env,
                pool_size: AtomicUsize::new(pool_size),
                max_tasks: AtomicUsize::new(max_tasks),
            }),
        }
    }

    /// Gets inner thread pool size.
    #[inline]
    pub fn get_pool_size(&self) -> usize {
        self.inner.pool_size.load(Ordering::Relaxed)
    }

    pub fn scale_pool_size(&self, thread_count: usize) {
        self.inner.scale_pool_size(thread_count)
    }

    #[inline]
    pub fn set_max_tasks_per_worker(&self, tasks_per_thread: usize) {
        self.inner.set_max_tasks_per_worker(tasks_per_thread);
    }

    #[inline]
    pub fn get_max_tasks_count(&self) -> usize {
        self.inner.max_tasks.load(Ordering::Relaxed)
    }

    /// Gets current running task count.
    #[inline]
    pub fn get_running_task_count(&self) -> usize {
        // As long as different future pool has different name prefix, we can safely use
        // the value in metrics.
        self.inner.get_running_task_count()
    }

    /// Spawns a future in the pool.
    pub fn spawn<F>(&self, future: F) -> Result<(), Full>
    where
        F: Future + Send + 'static,
    {
        self.inner.spawn(TlsTrackedFuture::new(future), None)
    }

    pub fn spawn_with_extras<F>(&self, future: F, extras: Extras) -> Result<(), Full>
    where
        F: Future + Send + 'static,
    {
        self.inner
            .spawn(TlsTrackedFuture::new(future), Some(extras))
    }

    /// Spawns a future in the pool and returns a handle to the result of the
    /// future.
    ///
    /// The future will not be executed if the handle is not polled.
    pub fn spawn_handle<F>(
        &self,
        future: F,
    ) -> Result<impl Future<Output = Result<F::Output, Canceled>>, Full>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        self.inner.spawn_handle(TlsTrackedFuture::new(future))
    }

    /// Return the min thread count and the max thread count that this pool can
    /// scale to.
    pub fn thread_count_limit(&self) -> (usize, usize) {
        self.inner.pool.thread_count_limit()
    }

    /// Cancel all pending futures and join all threads.
    pub fn shutdown(&self) {
        self.inner.pool.shutdown();
    }

    //  Get a remote queue for spawning tasks without owning the thread pool.
    pub fn remote(&self) -> &yatp::Remote<future::TaskCell> {
        self.inner.pool.remote()
    }
}

struct PoolInner {
    pool: ThreadPool,
    env: Env,
    // for accessing pool_size config since yatp doesn't offer such getter.
    pool_size: AtomicUsize,
    max_tasks: AtomicUsize,
}

impl PoolInner {
    #[inline]
    fn scale_pool_size(&self, thread_count: usize) {
        self.pool.scale_workers(thread_count);
        let mut max_tasks = self.max_tasks.load(Ordering::Acquire);
        if max_tasks != usize::MAX {
            max_tasks = max_tasks
                .saturating_div(self.pool_size.load(Ordering::Acquire))
                .saturating_mul(thread_count);
            self.max_tasks.store(max_tasks, Ordering::Release);
        }
        self.pool_size.store(thread_count, Ordering::Release);
    }

    fn set_max_tasks_per_worker(&self, max_tasks_per_thread: usize) {
        let max_tasks = self
            .pool_size
            .load(Ordering::Acquire)
            .saturating_mul(max_tasks_per_thread);
        self.max_tasks.store(max_tasks, Ordering::Release);
    }

    fn get_running_task_count(&self) -> usize {
        // As long as different future pool has different name prefix, we can safely use
        // the value in metrics.
        self.env
            .metrics_running_task_count_by_priority
            .iter()
            .map(|r| r.get())
            .sum::<i64>() as usize
    }

    fn gate_spawn(&self, current_tasks: usize) -> Result<(), Full> {
        fail_point!("future_pool_spawn_full", |_| Err(Full {
            current_tasks: 100,
            max_tasks: 100,
        }));

        let max_tasks = self.max_tasks.load(Ordering::Acquire);
        if max_tasks == usize::MAX {
            return Ok(());
        }

        if current_tasks >= max_tasks {
            Err(Full {
                current_tasks,
                max_tasks,
            })
        } else {
            Ok(())
        }
    }

    fn spawn<F>(&self, future: F, extras: Option<Extras>) -> Result<(), Full>
    where
        F: Future + Send + 'static,
    {
        let metrics_handled_task_count = self.env.metrics_handled_task_count.clone();
        let task_priority = extras
            .as_ref()
            .map(|m| priority_from_task_meta(m.metadata()))
            .unwrap_or(TaskPriority::Medium);
        let metrics_running_task_count =
            self.env.metrics_running_task_count_by_priority[task_priority as usize].clone();

        self.gate_spawn(metrics_running_task_count.get() as usize)?;

        metrics_running_task_count.inc();

        // NB: Prefer FutureExt::map to async block, because an async block
        // doubles memory usage.
        // See https://github.com/rust-lang/rust/issues/59087
        let f = future.map(move |_| {
            metrics_handled_task_count.inc();
            metrics_running_task_count.dec();
        });

        if let Some(extras) = extras {
            self.pool.spawn(future::TaskCell::new(f, extras));
        } else {
            self.pool.spawn(f);
        }
        Ok(())
    }

    fn spawn_handle<F>(
        &self,
        future: F,
    ) -> Result<impl Future<Output = Result<F::Output, Canceled>>, Full>
    where
        F: Future + Send + 'static,
        F::Output: Send,
    {
        let metrics_handled_task_count = self.env.metrics_handled_task_count.clone();
        let metrics_running_task_count =
            self.env.metrics_running_task_count_by_priority[TaskPriority::Medium as usize].clone();

        self.gate_spawn(metrics_running_task_count.get() as usize)?;

        let (tx, rx) = oneshot::channel();
        metrics_running_task_count.inc();
        // NB: Prefer FutureExt::map to async block, because an async block
        // doubles memory usage.
        // See https://github.com/rust-lang/rust/issues/59087
        self.pool.spawn(future.map(move |res| {
            metrics_handled_task_count.inc();
            metrics_running_task_count.dec();
            let _ = tx.send(res);
        }));
        Ok(rx)
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub struct Full {
    pub current_tasks: usize,
    pub max_tasks: usize,
}

impl std::fmt::Display for Full {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "future pool is full")
    }
}

impl std::error::Error for Full {
    fn description(&self) -> &str {
        "future pool is full"
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
        time::Duration,
    };

    use futures::executor::block_on;

    use super::{
        super::{DefaultTicker, PoolTicker, TICK_INTERVAL, YatpPoolBuilder as Builder},
        *,
    };

    fn spawn_future_and_wait(pool: &FuturePool, duration: Duration) {
        block_on(
            pool.spawn_handle(async move {
                thread::sleep(duration);
            })
            .unwrap(),
        )
        .unwrap();
    }

    fn spawn_future_without_wait(pool: &FuturePool, duration: Duration) {
        pool.spawn(async move {
            thread::sleep(duration);
        })
        .unwrap();
    }

    #[derive(Clone)]
    pub struct SequenceTicker {
        tick: Arc<dyn Fn() + Send + Sync>,
    }

    impl SequenceTicker {
        pub fn new<F>(tick: F) -> SequenceTicker
        where
            F: Fn() + Send + Sync + 'static,
        {
            SequenceTicker {
                tick: Arc::new(tick),
            }
        }
    }

    impl PoolTicker for SequenceTicker {
        fn on_tick(&mut self) {
            (self.tick)();
        }
    }

    #[test]
    fn test_tick() {
        let tick_sequence = Arc::new(AtomicUsize::new(0));

        let (tx, rx) = mpsc::sync_channel(1000);
        let rx = Arc::new(Mutex::new(rx));
        let ticker = SequenceTicker::new(move || {
            let seq = tick_sequence.fetch_add(1, Ordering::SeqCst);
            tx.send(seq).unwrap();
        });

        let pool = Builder::new(ticker)
            .thread_count(1, 1, 1)
            .build_future_pool();
        let try_recv_tick = || {
            let rx = rx.clone();
            block_on(
                pool.spawn_handle(async move { rx.lock().unwrap().try_recv() })
                    .unwrap(),
            )
            .unwrap()
        };

        try_recv_tick().unwrap_err();

        // Tick is emitted because long enough time has elapsed since pool is created
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        try_recv_tick().unwrap_err();

        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);

        // So far we have only elapsed TICK_INTERVAL * 0.2, so no ticks so far.
        try_recv_tick().unwrap_err();

        // Even if long enough time has elapsed, tick is not emitted until next task
        // arrives
        thread::sleep(TICK_INTERVAL * 2);
        try_recv_tick().unwrap_err();

        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        assert_eq!(try_recv_tick().unwrap(), 0);
        try_recv_tick().unwrap_err();

        // Tick is not emitted if there is no task
        thread::sleep(TICK_INTERVAL * 2);
        try_recv_tick().unwrap_err();

        // Tick is emitted since long enough time has passed
        spawn_future_and_wait(&pool, TICK_INTERVAL / 20);
        assert_eq!(try_recv_tick().unwrap(), 1);
        try_recv_tick().unwrap_err();

        // Tick is emitted immediately after a long task
        spawn_future_and_wait(&pool, TICK_INTERVAL * 2);
        assert_eq!(try_recv_tick().unwrap(), 2);
        try_recv_tick().unwrap_err();
    }

    #[test]
    fn test_tick_multi_thread() {
        let tick_sequence = Arc::new(AtomicUsize::new(0));

        let (tx, rx) = mpsc::sync_channel(1000);
        let ticker = SequenceTicker::new(move || {
            let seq = tick_sequence.fetch_add(1, Ordering::SeqCst);
            tx.send(seq).unwrap();
        });

        let pool = Builder::new(ticker)
            .thread_count(2, 2, 2)
            .build_future_pool();

        rx.try_recv().unwrap_err();

        // Spawn two tasks, each will be processed in one worker thread.
        spawn_future_without_wait(&pool, TICK_INTERVAL / 2);
        spawn_future_without_wait(&pool, TICK_INTERVAL / 2);

        rx.try_recv().unwrap_err();

        // Wait long enough time to trigger a tick.
        thread::sleep(TICK_INTERVAL * 2);

        rx.try_recv().unwrap_err();

        // These two tasks should both trigger a tick.
        spawn_future_without_wait(&pool, TICK_INTERVAL);
        spawn_future_without_wait(&pool, TICK_INTERVAL / 2);

        // Wait until these tasks are finished.
        thread::sleep(TICK_INTERVAL * 2);

        assert_eq!(rx.try_recv().unwrap(), 0);
        assert_eq!(rx.try_recv().unwrap(), 1);
        rx.try_recv().unwrap_err();
    }

    #[test]
    fn test_handle_result() {
        let pool = Builder::new(DefaultTicker {})
            .thread_count(1, 1, 1)
            .build_future_pool();

        let handle = pool.spawn_handle(async { 42 });

        assert_eq!(block_on(handle.unwrap()).unwrap(), 42);
    }

    #[test]
    fn test_running_task_count() {
        let pool = Builder::new(DefaultTicker {})
            .name_prefix("future_pool_for_running_task_test") // The name is important
            .thread_count(2, 2, 2)
            .build_future_pool();

        assert_eq!(pool.get_running_task_count(), 0);

        spawn_future_without_wait(&pool, Duration::from_millis(500)); // f1
        assert_eq!(pool.get_running_task_count(), 1);

        spawn_future_without_wait(&pool, Duration::from_millis(1000)); // f2
        assert_eq!(pool.get_running_task_count(), 2);

        spawn_future_without_wait(&pool, Duration::from_millis(1500));
        assert_eq!(pool.get_running_task_count(), 3);

        thread::sleep(Duration::from_millis(700)); // f1 completed, f2 elapsed 700
        assert_eq!(pool.get_running_task_count(), 2);

        spawn_future_without_wait(&pool, Duration::from_millis(1500));
        assert_eq!(pool.get_running_task_count(), 3);

        thread::sleep(Duration::from_millis(2700));
        assert_eq!(pool.get_running_task_count(), 0);
    }

    fn spawn_long_time_future(
        pool: &FuturePool,
        id: u64,
        future_duration_ms: u64,
    ) -> Result<impl Future<Output = Result<u64, Canceled>>, Full> {
        pool.spawn_handle(async move {
            thread::sleep(Duration::from_millis(future_duration_ms));
            id
        })
    }

    fn wait_on_new_thread<F>(sender: mpsc::Sender<F::Output>, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        thread::spawn(move || {
            let r = block_on(future);
            sender.send(r).unwrap();
        });
    }

    #[test]
    fn test_full() {
        let (tx, rx) = mpsc::channel();

        let read_pool = Builder::new(DefaultTicker {})
            .name_prefix("future_pool_test_full")
            .thread_count(2, 2, 2)
            .max_tasks(4)
            .build_future_pool();

        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 0, 5).unwrap(),
        );
        // not full
        assert_eq!(rx.recv().unwrap(), Ok(0));

        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 1, 100).unwrap(),
        );
        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 2, 200).unwrap(),
        );
        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 3, 300).unwrap(),
        );
        wait_on_new_thread(
            tx.clone(),
            spawn_long_time_future(&read_pool, 4, 400).unwrap(),
        );
        // no available results (running = 4)
        rx.recv_timeout(Duration::from_millis(50)).unwrap_err();

        // full
        assert!(spawn_long_time_future(&read_pool, 5, 100).is_err());

        // full
        assert!(spawn_long_time_future(&read_pool, 6, 100).is_err());

        // wait a future completes (running = 3)
        assert_eq!(rx.recv().unwrap(), Ok(1));

        // add new (running = 4)
        wait_on_new_thread(tx, spawn_long_time_future(&read_pool, 7, 5).unwrap());

        // full
        assert!(spawn_long_time_future(&read_pool, 8, 100).is_err());

        rx.recv().unwrap().unwrap();
        rx.recv().unwrap().unwrap();
        rx.recv().unwrap().unwrap();
        rx.recv().unwrap().unwrap();

        // no more results
        rx.recv_timeout(Duration::from_millis(500)).unwrap_err();
    }

    #[test]
    fn test_scale_pool_size() {
        let pool = Builder::new(DefaultTicker {})
            .thread_count(1, 4, 8)
            .build_future_pool();

        assert_eq!(pool.get_pool_size(), 4);
        let cloned = pool.clone();

        pool.scale_pool_size(8);
        assert_eq!(pool.get_pool_size(), 8);
        assert_eq!(cloned.get_pool_size(), 8);

        pool.scale_pool_size(1);
        assert_eq!(pool.get_pool_size(), 1);
        assert_eq!(cloned.get_pool_size(), 1);
    }
}
