use std::net::SocketAddr;
use std::println;

use runtime::config::Config;
use runtime::executor::Runtime;
use runtime::net::udp::UdpSocket;
use runtime::spawn;

async fn echo(addr: SocketAddr) {
    let mut socket = UdpSocket::bind(addr).unwrap();
    println!("echoing on {addr}");

    let mut buf = [0u8; 1024];
    loop {
        let (n, peer) = socket.recv_from(&mut buf).await.unwrap();
        println!("Received data from {} to {}", peer.to_string(), addr.to_string());
        socket.send_to(&buf[..n], peer).await.unwrap();
    }
}

fn main() {
    let runtime = Runtime::new(Config::default());

    runtime.block_on(async {
        let first = spawn(echo("127.0.0.1:9000".parse().unwrap()));
        let second = spawn(echo("127.0.0.1:9001".parse().unwrap()));
        first.await;
        second.await;
    });
}
