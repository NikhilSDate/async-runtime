use std::net::SocketAddr;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use runtime::config::Config;
use runtime::executor::Runtime;
use runtime::net::udp::UdpSocket;
use runtime::spawn;

const TIMEOUT: Duration = Duration::from_secs(5);

async fn echo_once(addr: SocketAddr) -> usize {
    let mut socket = UdpSocket::bind(addr).unwrap();
    let mut buf = [0u8; 64];
    let (n, peer) = socket.recv_from(&mut buf).await.unwrap();
    socket.send_to(&buf[..n], peer).await.unwrap();
    n
}

#[test]
fn block_on_awaits_two_independent_udp_tasks_to_completion() {
    let first_addr: SocketAddr = "127.0.0.1:58000".parse().unwrap();
    let second_addr: SocketAddr = "127.0.0.1:58001".parse().unwrap();

    let runtime = Runtime::new(Config::default());

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        let client = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        client.send_to(b"first", first_addr).unwrap();
        client.send_to(b"second-msg", second_addr).unwrap();
    });

    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let result = runtime.block_on(async move {
            let first = spawn(echo_once(first_addr));
            let second = spawn(echo_once(second_addr));
            (first.await, second.await)
        });
        tx.send(result).unwrap();
    });

    let (first_len, second_len) = rx
        .recv_timeout(TIMEOUT)
        .expect("block_on with two spawned tasks did not complete in time");

    assert_eq!(first_len, 5);
    assert_eq!(second_len, 10);
}
