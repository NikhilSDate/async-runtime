use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use runtime::config::Config;
use runtime::executor::Runtime;
use runtime::timer::Sleep;

const TIMEOUT: Duration = Duration::from_secs(5);

#[test]
fn block_on_returns_promptly_after_future_resolves() {
    let runtime = Runtime::new(Config::default());
    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let start = Instant::now();
        let value = runtime.block_on(async {
            Sleep::new(Duration::from_millis(200)).await;
            "done"
        });
        tx.send((value, start.elapsed())).unwrap();
    });

    let (value, elapsed) = rx
        .recv_timeout(TIMEOUT)
        .expect("block_on did not return within the external timeout");

    assert_eq!(value, "done");
    assert!(
        elapsed < Duration::from_secs(2),
        "block_on took {elapsed:?} to return after its future resolved"
    );
}
