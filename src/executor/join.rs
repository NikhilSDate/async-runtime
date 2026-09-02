use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

enum State<T> {
    Pending,
    Waiting(Waker),
    Done(T),
}

struct Shared<T> {
    state: Mutex<State<T>>,
}

pub struct JoinHandle<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let mut state = self.shared.state.lock().unwrap();
        match mem::replace(&mut *state, State::Pending) {
            State::Done(value) => Poll::Ready(value),
            State::Pending | State::Waiting(_) => {
                *state = State::Waiting(cx.waker().clone());
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
        state: Mutex::new(State::Pending),
    });
    let handle = JoinHandle {
        shared: shared.clone(),
    };
    let wrapped = async move {
        let value = future.await;
        let waker = match mem::replace(&mut *shared.state.lock().unwrap(), State::Done(value)) {
            State::Waiting(waker) => Some(waker),
            State::Pending | State::Done(_) => None,
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    };
    (wrapped, handle)
}
