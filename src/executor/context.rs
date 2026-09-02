use std::future::Future;
use std::sync::{Arc, OnceLock};

use crate::executor::TaskSender;
use crate::executor::join::{self, JoinHandle};
use crate::reactor::Reactor;

pub(crate) struct Context {
    task_sender: TaskSender,
    reactor: Arc<Reactor>,
}

static CONTEXT: OnceLock<Context> = OnceLock::new();

impl Context {
    pub(crate) fn init(task_sender: TaskSender, reactor: Arc<Reactor>) {
        CONTEXT
            .set(Context {
                task_sender,
                reactor,
            })
            .unwrap_or_else(|_| panic!("runtime context already initialized"));
    }

    pub(crate) fn current() -> &'static Context {
        CONTEXT
            .get()
            .expect("runtime context not initialized; is a Runtime running?")
    }

    pub(crate) fn task_sender(&self) -> &TaskSender {
        &self.task_sender
    }

    pub(crate) fn reactor(&self) -> Arc<Reactor> {
        self.reactor.clone()
    }
}

pub fn spawn<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    let (wrapped, handle) = join::spawn_with_handle(future);
    Context::current().task_sender().spawn(wrapped);
    handle
}

pub(crate) fn reactor() -> Arc<Reactor> {
    Context::current().reactor().clone()
}
