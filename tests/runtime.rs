use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Once};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use runtime::config::Config;
use runtime::executor::Runtime;
use runtime::net::udp::UdpSocket;
use runtime::timer::Sleep;

const TIMEOUT: Duration = Duration::from_secs(5);

static INIT: Once = Once::new();

fn ensure_runtime() {
    INIT.call_once(|| {
        let default_workers = Config::default().worker_threads;
        let config = Config {
            events_capacity: 1024,
            worker_threads: default_workers.max(4),
        };
        let rt = Runtime::new(config);
        thread::spawn(move || rt.run());
    });
}

struct ThreadWaker(thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(v) => return v,
            Poll::Pending => thread::park(),
        }
    }
}

static NEXT_PORT: AtomicUsize = AtomicUsize::new(46000);

fn fresh_addr() -> std::net::SocketAddr {
    let port = NEXT_PORT.fetch_add(1, Ordering::Relaxed) as u16;
    format!("127.0.0.1:{port}").parse().unwrap()
}

fn any_addr() -> std::net::SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

#[test]
fn spawned_future_result_observable_via_channel() {
    ensure_runtime();
    let (tx, rx) = mpsc::channel();
    runtime::spawn(async move {
        tx.send(2 + 2).unwrap();
    });
    assert_eq!(rx.recv_timeout(TIMEOUT).unwrap(), 4);
}

#[test]
fn join_handle_ready_before_ever_polled() {
    ensure_runtime();
    let handle = runtime::spawn(async { 6 * 7 });
    thread::sleep(Duration::from_millis(200));
    assert_eq!(block_on(handle), 42);
}

#[test]
fn join_handle_pending_then_woken_on_completion() {
    ensure_runtime();
    let handle = runtime::spawn(async {
        Sleep::new(Duration::from_millis(150)).await;
        "done"
    });
    assert_eq!(block_on(handle), "done");
}

#[test]
fn panicking_task_does_not_take_down_other_tasks() {
    ensure_runtime();
    let (tx, rx) = mpsc::channel();

    let before = runtime::spawn(async { 10 });

    runtime::spawn(async {
        panic!("deliberate panic to exercise task-isolation");
    });

    let tx2 = tx.clone();
    runtime::spawn(async move {
        tx2.send("survived").unwrap();
    });

    assert_eq!(block_on(before), 10);
    assert_eq!(rx.recv_timeout(TIMEOUT).unwrap(), "survived");
}

#[test]
fn sleep_delays_at_least_requested_duration() {
    ensure_runtime();
    let requested = Duration::from_millis(200);
    let (tx, rx) = mpsc::channel();
    runtime::spawn(async move {
        let start = Instant::now();
        Sleep::new(requested).await;
        tx.send(start.elapsed()).unwrap();
    });
    let elapsed = rx.recv_timeout(TIMEOUT).unwrap();
    assert!(
        elapsed >= requested,
        "elapsed {elapsed:?} < requested {requested:?}"
    );
    assert!(
        elapsed < requested * 5,
        "elapsed {elapsed:?} far exceeds requested {requested:?}"
    );
}

#[test]
fn worker_threads_execute_tasks_concurrently() {
    ensure_runtime();
    const N: usize = 4;
    let running = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::channel();

    for _ in 0..N {
        let running = running.clone();
        let max_seen = max_seen.clone();
        let tx = tx.clone();
        runtime::spawn(async move {
            let cur = running.fetch_add(1, Ordering::SeqCst) + 1;
            max_seen.fetch_max(cur, Ordering::SeqCst);
            thread::sleep(Duration::from_millis(200));
            running.fetch_sub(1, Ordering::SeqCst);
            tx.send(()).unwrap();
        });
    }

    for _ in 0..N {
        rx.recv_timeout(TIMEOUT).unwrap();
    }

    let observed = max_seen.load(Ordering::SeqCst);
    assert!(
        observed > 1,
        "expected multiple tasks running concurrently, only observed {observed}"
    );
}

#[test]
fn udp_echo_round_trip_multiple_datagrams() {
    ensure_runtime();
    let server_addr = fresh_addr();
    let mut server = UdpSocket::bind(server_addr).unwrap();
    let mut client = UdpSocket::bind(any_addr()).unwrap();
    let (tx, rx) = mpsc::channel();

    runtime::spawn(async move {
        let mut buf = [0u8; 64];
        for _ in 0..5 {
            let (n, peer) = server.recv_from(&mut buf).await.unwrap();
            server.send_to(&buf[..n], peer).await.unwrap();
        }
    });

    runtime::spawn(async move {
        let mut buf = [0u8; 64];
        for i in 0..5u8 {
            let msg = [b'A' + i; 3];
            client.send_to(&msg, server_addr).await.unwrap();
            let (n, _from) = client.recv_from(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], &msg[..]);
        }
        tx.send(()).unwrap();
    });

    rx.recv_timeout(TIMEOUT)
        .expect("udp round trip did not complete");
}

#[test]
fn udp_recv_from_blocks_until_data_arrives() {
    ensure_runtime();
    let addr = fresh_addr();
    let mut socket = UdpSocket::bind(addr).unwrap();
    let mut buf = [0u8; 16];

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        sock.send_to(b"late", addr).unwrap();
    });

    let start = Instant::now();
    let (n, _from) = block_on(socket.recv_from(&mut buf)).unwrap();
    assert!(start.elapsed() >= Duration::from_millis(150));
    assert_eq!(&buf[..n], b"late");
}

#[test]
fn udp_drop_after_synchronous_send_does_not_panic() {
    ensure_runtime();
    let mut socket = UdpSocket::bind(any_addr()).unwrap();
    let target = fresh_addr();
    let result = block_on(socket.send_to(b"hello", target));
    assert!(result.is_ok());
    drop(socket);
}

#[test]
fn udp_drop_after_blocking_recv_completes_does_not_panic() {
    ensure_runtime();
    let addr = fresh_addr();
    let mut socket = UdpSocket::bind(addr).unwrap();
    let mut buf = [0u8; 16];

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        let sock = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        sock.send_to(b"hi", addr).unwrap();
    });

    let (n, _from) = block_on(socket.recv_from(&mut buf)).unwrap();
    assert_eq!(&buf[..n], b"hi");
    drop(socket);
}

#[test]
#[should_panic(expected = "runtime context already initialized")]
fn second_runtime_new_panics() {
    ensure_runtime();
    let _ = Runtime::new(Config::default());
}
