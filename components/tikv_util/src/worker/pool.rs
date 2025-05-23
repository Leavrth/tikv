// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use futures::{
    channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded},
    compat::{Future01CompatExt, Stream01CompatExt},
    executor::block_on,
    future::FutureExt,
    stream::StreamExt,
};
use prometheus::{IntCounter, IntGauge};
use yatp::Remote;

use super::metrics::*;
use crate::{
    future::{block_on_timeout, poll_future_notify},
    timer::GLOBAL_TIMER_HANDLE,
    yatp_pool::{DefaultTicker, FuturePool, YatpPoolBuilder},
};

#[derive(PartialEq)]
pub enum ScheduleError<T> {
    Stopped(T),
    Full(T),
}

impl<T> ScheduleError<T> {
    pub fn into_inner(self) -> T {
        match self {
            ScheduleError::Stopped(t) | ScheduleError::Full(t) => t,
        }
    }
}

impl<T> Error for ScheduleError<T> {}

impl<T> Display for ScheduleError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let msg = match *self {
            ScheduleError::Stopped(_) => "channel has been closed",
            ScheduleError::Full(_) => "channel is full",
        };
        write!(f, "{}", msg)
    }
}

impl<T> Debug for ScheduleError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

pub trait Runnable: Send {
    type Task: Display + Send + 'static;

    /// Runs a task.
    fn run(&mut self, _: Self::Task) {
        unimplemented!()
    }
    fn on_tick(&mut self) {}
    fn shutdown(&mut self) {}
}

pub trait RunnableWithTimer: Runnable {
    fn on_timeout(&mut self);
    fn get_interval(&self) -> Duration;
}

struct RunnableWrapper<R: Runnable + 'static> {
    inner: R,
}

impl<R: Runnable + 'static> Drop for RunnableWrapper<R> {
    fn drop(&mut self) {
        self.inner.shutdown();
    }
}

enum Msg<T: Display + Send> {
    Task(T),
    Timeout,
}

// A wrapper of Runnable that implements RunnableWithTimer with no timeout.
struct NoTimeoutRunnableWrapper<T: Runnable>(T);

impl<T: Runnable> Runnable for NoTimeoutRunnableWrapper<T> {
    type Task = T::Task;
    fn run(&mut self, task: Self::Task) {
        self.0.run(task)
    }
    fn on_tick(&mut self) {
        self.0.on_tick()
    }
    fn shutdown(&mut self) {
        self.0.shutdown()
    }
}

impl<T: Runnable> RunnableWithTimer for NoTimeoutRunnableWrapper<T> {
    fn on_timeout(&mut self) {}
    fn get_interval(&self) -> Duration {
        Duration::ZERO
    }
}

/// Scheduler provides interface to schedule task to underlying workers.
pub struct Scheduler<T: Display + Send> {
    counter: Arc<AtomicUsize>,
    sender: UnboundedSender<Msg<T>>,
    pending_capacity: usize,
    metrics_pending_task_count: IntGauge,
}

impl<T: Display + Send> Scheduler<T> {
    fn new(
        sender: UnboundedSender<Msg<T>>,
        counter: Arc<AtomicUsize>,
        pending_capacity: usize,
        metrics_pending_task_count: IntGauge,
    ) -> Scheduler<T> {
        Scheduler {
            counter,
            sender,
            pending_capacity,
            metrics_pending_task_count,
        }
    }

    /// Schedules a task to run.
    ///
    /// If the worker is stopped or number pending tasks exceeds capacity, an
    /// error will return.
    pub fn schedule(&self, task: T) -> Result<(), ScheduleError<T>> {
        debug!("scheduling task {}", task);
        if self.counter.load(Ordering::Acquire) >= self.pending_capacity {
            return Err(ScheduleError::Full(task));
        }
        self.schedule_force(task)
    }

    /// Schedules a task to run.
    ///
    /// Different from the `schedule` function, the task will still be scheduled
    /// if pending task number exceeds capacity.
    pub fn schedule_force(&self, task: T) -> Result<(), ScheduleError<T>> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        self.metrics_pending_task_count.inc();
        if let Err(e) = self.sender.unbounded_send(Msg::Task(task)) {
            if let Msg::Task(t) = e.into_inner() {
                self.counter.fetch_sub(1, Ordering::SeqCst);
                self.metrics_pending_task_count.dec();
                return Err(ScheduleError::Stopped(t));
            }
        }
        Ok(())
    }

    /// Checks if underlying worker can't handle task immediately.
    pub fn is_busy(&self) -> bool {
        self.counter.load(Ordering::Acquire) > 0
    }

    pub fn stop(&self) {
        self.sender.close_channel();
    }

    pub fn pending_tasks(&self) -> usize {
        self.counter.load(Ordering::Acquire)
    }
}

impl<T: Display + Send> Clone for Scheduler<T> {
    fn clone(&self) -> Scheduler<T> {
        Scheduler {
            counter: Arc::clone(&self.counter),
            sender: self.sender.clone(),
            pending_capacity: self.pending_capacity,
            metrics_pending_task_count: self.metrics_pending_task_count.clone(),
        }
    }
}

pub struct LazyWorker<T: Display + Send + 'static> {
    scheduler: Scheduler<T>,
    worker: Worker,
    receiver: Option<UnboundedReceiver<Msg<T>>>,
    metrics_pending_task_count: IntGauge,
    metrics_handled_task_count: IntCounter,
}

impl<T: Display + Send + 'static> LazyWorker<T> {
    pub fn new<S: Into<String>>(name: S) -> LazyWorker<T> {
        let name = name.into();
        let worker = Worker::new(name.clone());
        worker.lazy_build(name)
    }

    pub fn start<R: 'static + Runnable<Task = T>>(&mut self, runner: R) -> bool {
        let no_timeout_runner = NoTimeoutRunnableWrapper(runner);
        self.start_with_timer(no_timeout_runner)
    }

    pub fn start_with_timer<R: 'static + RunnableWithTimer<Task = T>>(
        &mut self,
        runner: R,
    ) -> bool {
        if let Some(receiver) = self.receiver.take() {
            self.worker.start_with_timer_impl(
                runner,
                self.scheduler.sender.clone(),
                receiver,
                self.metrics_pending_task_count.clone(),
                self.metrics_handled_task_count.clone(),
            );
            return true;
        }
        false
    }

    pub fn scheduler(&self) -> Scheduler<T> {
        self.scheduler.clone()
    }

    pub fn stop(&mut self) {
        self.scheduler.stop();
    }

    pub fn stop_worker(mut self) {
        self.stop();
        self.worker.stop()
    }

    pub fn remote(&self) -> Remote<yatp::task::future::TaskCell> {
        self.worker.remote()
    }

    pub fn pool_size(&self) -> usize {
        self.worker.pool_size()
    }

    pub fn pool(&self) -> FuturePool {
        self.worker.pool()
    }
}

pub struct ReceiverWrapper<T: Display + Send> {
    inner: UnboundedReceiver<Msg<T>>,
}

impl<T: Display + Send> ReceiverWrapper<T> {
    pub fn recv(&mut self) -> Option<T> {
        let msg = block_on(self.inner.next());
        match msg {
            Some(Msg::Task(t)) => Some(t),
            _ => None,
        }
    }

    pub fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<T>, std::sync::mpsc::RecvTimeoutError> {
        let msg = block_on_timeout(self.inner.next(), timeout)
            .map_err(|_| std::sync::mpsc::RecvTimeoutError::Timeout)?;
        if let Some(Msg::Task(t)) = msg {
            return Ok(Some(t));
        }
        Ok(None)
    }
}

/// Creates a scheduler that can't schedule any task.
///
/// Useful for test purpose.
pub fn dummy_scheduler<T: Display + Send>() -> (Scheduler<T>, ReceiverWrapper<T>) {
    let (tx, rx) = unbounded();
    (
        Scheduler::new(
            tx,
            Arc::new(AtomicUsize::new(0)),
            1000,
            WORKER_PENDING_TASK_VEC.with_label_values(&["dummy"]),
        ),
        ReceiverWrapper { inner: rx },
    )
}

#[derive(Copy, Clone)]
pub struct Builder<S: Into<String>> {
    name: S,
    core_thread_count: usize,
    min_thread_count: Option<usize>,
    max_thread_count: Option<usize>,
    pending_capacity: usize,
}

impl<S: Into<String>> Builder<S> {
    pub fn new(name: S) -> Self {
        Builder {
            name,
            core_thread_count: 1,
            min_thread_count: None,
            max_thread_count: None,
            pending_capacity: usize::MAX,
        }
    }

    /// Pending tasks won't exceed `pending_capacity`.
    #[must_use]
    pub fn pending_capacity(mut self, pending_capacity: usize) -> Self {
        self.pending_capacity = pending_capacity;
        self
    }

    #[must_use]
    pub fn thread_count(mut self, thread_count: usize) -> Self {
        self.core_thread_count = thread_count;
        self
    }

    #[must_use]
    pub fn thread_count_limits(mut self, min_thread_count: usize, max_thread_count: usize) -> Self {
        self.min_thread_count = Some(min_thread_count);
        self.max_thread_count = Some(max_thread_count);
        self
    }

    pub fn create(self) -> Worker {
        let pool = YatpPoolBuilder::new(DefaultTicker::default())
            .name_prefix(self.name)
            .thread_count(
                self.min_thread_count.unwrap_or(self.core_thread_count),
                self.core_thread_count,
                self.max_thread_count.unwrap_or(self.core_thread_count),
            )
            .build_future_pool();
        Worker {
            stop: Arc::new(AtomicBool::new(false)),
            pool,
            counter: Arc::new(AtomicUsize::new(0)),
            pending_capacity: self.pending_capacity,
            thread_count: self.core_thread_count,
        }
    }
}

/// A worker that can schedule time consuming tasks.
#[derive(Clone)]
pub struct Worker {
    pool: FuturePool,
    pending_capacity: usize,
    counter: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread_count: usize,
}

impl Worker {
    pub fn new<S: Into<String>>(name: S) -> Worker {
        Builder::new(name).create()
    }

    pub fn start<R: Runnable + 'static, S: Into<String>>(
        &self,
        name: S,
        runner: R,
    ) -> Scheduler<R::Task> {
        let no_timeout_runner = NoTimeoutRunnableWrapper(runner);
        self.start_with_timer(name, no_timeout_runner)
    }

    pub fn start_with_timer<R: RunnableWithTimer + 'static, S: Into<String>>(
        &self,
        name: S,
        runner: R,
    ) -> Scheduler<R::Task> {
        let (tx, rx) = unbounded();
        let name = name.into();
        let metrics_pending_task_count = WORKER_PENDING_TASK_VEC.with_label_values(&[&name]);
        let metrics_handled_task_count = WORKER_HANDLED_TASK_VEC.with_label_values(&[&name]);
        self.start_with_timer_impl(
            runner,
            tx.clone(),
            rx,
            metrics_pending_task_count.clone(),
            metrics_handled_task_count,
        );
        Scheduler::new(
            tx,
            self.counter.clone(),
            self.pending_capacity,
            metrics_pending_task_count,
        )
    }

    pub fn spawn_interval_task<F>(&self, interval: Duration, mut func: F)
    where
        F: FnMut() + Send + 'static,
    {
        let mut interval = GLOBAL_TIMER_HANDLE
            .interval(std::time::Instant::now(), interval)
            .compat();
        let stop = self.stop.clone();
        let _ = self.pool.spawn(async move {
            while !stop.load(Ordering::Relaxed)
                && let Some(Ok(_)) = interval.next().await
            {
                func();
            }
        });
    }

    pub fn spawn_interval_async_task<F, Fut>(&self, interval: Duration, mut func: F)
    where
        Fut: Future<Output = ()> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
    {
        let mut interval = GLOBAL_TIMER_HANDLE
            .interval(std::time::Instant::now(), interval)
            .compat();
        let stop = self.stop.clone();
        let _ = self.pool.spawn(async move {
            while !stop.load(Ordering::Relaxed)
                && let Some(Ok(_)) = interval.next().await
            {
                let fut = func();
                fut.await;
            }
        });
    }

    pub fn spawn_async_task<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let _ = self.pool.spawn(f);
    }

    fn delay_notify<T: Display + Send + 'static>(
        tx: Option<UnboundedSender<Msg<T>>>,
        timeout: Duration,
    ) {
        let Some(tx) = tx else {
            return;
        };
        let now = Instant::now();
        let f = GLOBAL_TIMER_HANDLE
            .delay(now + timeout)
            .compat()
            .map(move |_| {
                let _ = tx.unbounded_send(Msg::<T>::Timeout);
            });
        poll_future_notify(f);
    }

    pub fn lazy_build<T: Display + Send + 'static, S: Into<String>>(
        &self,
        name: S,
    ) -> LazyWorker<T> {
        let (tx, rx) = unbounded();
        let name = name.into();
        let metrics_pending_task_count = WORKER_PENDING_TASK_VEC.with_label_values(&[&name]);
        let metrics_handled_task_count = WORKER_HANDLED_TASK_VEC.with_label_values(&[&name]);
        LazyWorker {
            receiver: Some(rx),
            worker: self.clone(),
            scheduler: Scheduler::new(
                tx,
                self.counter.clone(),
                self.pending_capacity,
                metrics_pending_task_count.clone(),
            ),
            metrics_pending_task_count,
            metrics_handled_task_count,
        }
    }

    /// Stops the worker thread.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
        self.pool.shutdown();
    }

    /// Checks if underlying worker can't handle task immediately.
    pub fn is_busy(&self) -> bool {
        self.stop.load(Ordering::Acquire)
            || self.counter.load(Ordering::Acquire) >= self.thread_count
    }

    pub fn remote(&self) -> Remote<yatp::task::future::TaskCell> {
        self.pool.remote().clone()
    }

    pub fn pool_size(&self) -> usize {
        self.pool.get_pool_size()
    }

    pub fn pool(&self) -> FuturePool {
        self.pool.clone()
    }

    fn start_with_timer_impl<R>(
        &self,
        runner: R,
        tx: UnboundedSender<Msg<R::Task>>,
        mut receiver: UnboundedReceiver<Msg<R::Task>>,
        metrics_pending_task_count: IntGauge,
        metrics_handled_task_count: IntCounter,
    ) where
        R: RunnableWithTimer + 'static,
    {
        let counter = self.counter.clone();
        let timeout = runner.get_interval();
        let tx = if !timeout.is_zero() { Some(tx) } else { None };
        Self::delay_notify(tx.clone(), timeout);
        let _ = self.pool.spawn(async move {
            let mut handle = RunnableWrapper { inner: runner };
            while let Some(msg) = receiver.next().await {
                match msg {
                    Msg::Task(task) => {
                        handle.inner.run(task);
                        counter.fetch_sub(1, Ordering::SeqCst);
                        metrics_pending_task_count.dec();
                        metrics_handled_task_count.inc();
                    }
                    Msg::Timeout => {
                        handle.inner.on_timeout();
                        let timeout = handle.inner.get_interval();
                        Self::delay_notify(tx.clone(), timeout);
                    }
                }
            }
        });
    }
}

mod tests {

    use std::{
        sync::{
            Arc, Mutex,
            atomic::{self, AtomicU64},
        },
        time::Duration,
    };

    use super::*;

    struct StepRunner {
        count: Arc<AtomicU64>,
        timeout_duration: Duration,
        tasks: Arc<Mutex<Vec<u64>>>,
    }

    impl Runnable for StepRunner {
        type Task = u64;

        fn run(&mut self, step: u64) {
            let mut tasks = self.tasks.lock().unwrap();
            tasks.push(step);
        }

        fn shutdown(&mut self) {
            self.count.fetch_add(1, atomic::Ordering::SeqCst);
        }
    }

    impl RunnableWithTimer for StepRunner {
        fn on_timeout(&mut self) {
            let tasks = self.tasks.lock().unwrap();
            for t in tasks.iter() {
                self.count.fetch_add(*t, atomic::Ordering::SeqCst);
            }
        }

        fn get_interval(&self) -> Duration {
            self.timeout_duration
        }
    }

    #[test]
    fn test_lazy_worker_with_timer() {
        let mut worker = LazyWorker::new("test_lazy_worker_with_timer");
        let scheduler = worker.scheduler();
        let count = Arc::new(AtomicU64::new(0));
        let tasks = Arc::new(Mutex::new(vec![]));
        worker.start_with_timer(StepRunner {
            count: count.clone(),
            timeout_duration: Duration::from_millis(200),
            tasks: tasks.clone(),
        });

        scheduler.schedule(1).unwrap();
        scheduler.schedule(2).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(2, tasks.lock().unwrap().len());
        assert_eq!(0, count.load(atomic::Ordering::SeqCst));
        std::thread::sleep(Duration::from_millis(200));
        // The worker already trigger `on_timeout`.
        assert_eq!(3, count.load(atomic::Ordering::SeqCst));
        scheduler.schedule(5).unwrap();
        std::thread::sleep(Duration::from_millis(10));
        assert_eq!(3, tasks.lock().unwrap().len());
        assert_eq!(3, count.load(atomic::Ordering::SeqCst));
        std::thread::sleep(Duration::from_millis(200));
        // The worker already trigger `on_timeout`.
        assert_eq!(11, count.load(atomic::Ordering::SeqCst));
        worker.stop();
        // The worker need some time to trigger shutdown.
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(12, count.load(atomic::Ordering::SeqCst));

        // Handled task must be 3.
        assert_eq!(3, worker.metrics_handled_task_count.get());
    }
}
