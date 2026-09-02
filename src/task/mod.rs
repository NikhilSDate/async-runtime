use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use crate::executor::TaskSender;

struct TaskState {
    future: Pin<Box<dyn Future<Output = ()> + Sync + Send + 'static>>,
    // Set once the future returns Ready (or panics).
    completed: bool,
}

// Task wraps over a future and allows a Waker to be constructed from it
// it also holds a sender which allows it to be put back on the executor's queue when ready
pub struct Task {
    state: Mutex<TaskState>,
    sender: TaskSender,
}

impl Task {
    pub(crate) fn new(
        future: Pin<Box<dyn Future<Output = ()> + Sync + Send + 'static>>,
        sender: TaskSender,
    ) -> Self {
        Self {
            state: Mutex::new(TaskState {
                future,
                completed: false,
            }),
            sender,
        }
    }

    // Polls the task once, unless it has already completed.
    pub(crate) fn run(self: &Arc<Self>) {
        let waker = Waker::from(self.clone());
        let mut cx = Context::from_waker(&waker);

        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.completed {
            return;
        }

        let poll_result = catch_unwind(AssertUnwindSafe(|| state.future.as_mut().poll(&mut cx)));

        match poll_result {
            Ok(Poll::Ready(())) => state.completed = true,
            Ok(Poll::Pending) => {}
            Err(_) => state.completed = true,
        }
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        let sender = self.sender.clone();
        sender.push(self);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.sender.push(self.clone());
    }
}
