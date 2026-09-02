use std::future::Future;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::Receiver;

use crate::config::Config;
use crate::reactor::Reactor;
use crate::task::Task;

mod context;
mod join;
mod task_sender;

use context::Context as RuntimeContext;
pub(crate) use context::reactor;
pub use context::spawn;
pub use join::JoinHandle;
pub(crate) use task_sender::TaskSender;

pub struct Runtime {
    executor: Executor,
}

impl Runtime {
    pub fn new(config: Config) -> Self {
        let (task_sender, queue) = TaskSender::new();
        let reactor = Reactor::new(&config).expect("failed to initialize reactor");
        RuntimeContext::init(task_sender, reactor);
        Self {
            executor: Executor::new(queue, config.worker_threads),
        }
    }

    pub fn spawn<F, T>(&self, future: F) -> JoinHandle<T>
    where
        F: Future<Output = T> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        context::spawn(future)
    }

    pub fn run(self) {
        self.executor.run()
    }

    pub fn block_on<F, T>(self, future: F) -> T
    where
        F: Future<Output = T> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        let (tx, rx) = std::sync::mpsc::channel();
        self.spawn(async move {
            let value = future.await;
            let _ = tx.send(value);
        });
        self.executor.spawn_background();
        rx.recv().unwrap()
    }
}

impl Default for Runtime {
    fn default() -> Self {
        Self::new(Config::default())
    }
}

struct Executor {
    queue: Receiver<Arc<Task>>,
    worker_threads: usize,
}

impl Executor {
    fn new(queue: Receiver<Arc<Task>>, worker_threads: usize) -> Self {
        Self {
            queue,
            worker_threads: worker_threads.max(1),
        }
    }

    fn run(self) {
        let handles: Vec<_> = (1..self.worker_threads)
            .map(|_| {
                let queue = self.queue.clone();
                thread::spawn(move || Self::worker(queue))
            })
            .collect();

        Self::worker(self.queue);

        for handle in handles {
            let _ = handle.join();
        }
    }

    fn spawn_background(&self) {
        for _ in 0..self.worker_threads {
            let queue = self.queue.clone();
            thread::spawn(move || Self::worker(queue));
        }
    }

    fn worker(queue: Receiver<Arc<Task>>) {
        while let Ok(task) = queue.recv() {
            task.run();
        }
    }
}
