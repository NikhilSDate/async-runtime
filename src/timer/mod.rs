use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

struct SharedState {
    completed: bool,
    waker: Option<Waker>,
}

pub struct Sleep {
    duration: Duration,
    shared: Arc<Mutex<SharedState>>,
    started: bool,
}

impl Sleep {
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            shared: Arc::new(Mutex::new(SharedState {
                completed: false,
                waker: None,
            })),
            started: false,
        }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if !this.started {
            this.started = true;
            let thread_shared = this.shared.clone();
            let duration = this.duration;
            thread::spawn(move || {
                thread::sleep(duration);
                let waker = {
                    let mut state = thread_shared.lock().unwrap_or_else(|e| e.into_inner());
                    state.completed = true;
                    state.waker.take()
                };
                if let Some(waker) = waker {
                    waker.wake();
                }
            });
        }

        let mut state = this.shared.lock().unwrap_or_else(|e| e.into_inner());
        if state.completed {
            Poll::Ready(())
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
