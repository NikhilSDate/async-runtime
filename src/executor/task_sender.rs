use std::future::Future;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, unbounded};

use crate::task::Task;

#[derive(Clone)]
pub struct TaskSender {
    sender: Sender<Arc<Task>>,
}

impl TaskSender {
    pub fn new() -> (Self, Receiver<Arc<Task>>) {
        let (sender, receiver) = unbounded();
        (Self { sender }, receiver)
    }

    pub fn push(&self, task: Arc<Task>) {
        self.sender.send(task).unwrap();
    }

    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + Sync + 'static,
    {
        self.push(Arc::new(Task::new(Box::pin(future), self.clone())));
    }
}
