use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

// The status of a spawned task as observed by its JoinHandle.
enum TaskStatus<T> {
    Pending,
    Waiting(Waker),
    Done(T),
}

struct Shared<T> {
    status: Mutex<TaskStatus<T>>,
}

impl<T> Shared<T> {
    fn complete(&self, value: T) {
        let waker = match mem::replace(&mut *self.status.lock().unwrap(), TaskStatus::Done(value)) {
            TaskStatus::Waiting(waker) => Some(waker),
            TaskStatus::Pending | TaskStatus::Done(_) => None,
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

pub struct JoinHandle<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut status = self.shared.status.lock().unwrap();
        match mem::replace(&mut *status, TaskStatus::Pending) {
            TaskStatus::Done(value) => Poll::Ready(value),
            TaskStatus::Pending | TaskStatus::Waiting(_) => {
                *status = TaskStatus::Waiting(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

pub(crate) fn spawn_with_handle<F, T>(
    future: F,
) -> (
    impl Future<Output = ()> + Send + Sync + 'static,
    JoinHandle<T>,
)
where
    F: Future<Output = T> + Send + Sync + 'static,
    T: Send + Sync + 'static,
{
    let shared = Arc::new(Shared {
        status: Mutex::new(TaskStatus::Pending),
    });
    let handle = JoinHandle {
        shared: shared.clone(),
    };
    let wrapped = async move {
        let value = future.await;
        shared.complete(value);
    };
    (wrapped, handle)
}
