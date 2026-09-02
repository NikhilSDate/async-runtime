use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use mio::{Interest, Token};

use crate::executor::reactor;

pub struct UdpSocket {
    io: mio::net::UdpSocket,
    token: Token,
}

impl UdpSocket {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            io: mio::net::UdpSocket::bind(addr)?,
            token: reactor().token(),
        })
    }

    pub fn send_to<'a>(&'a mut self, buf: &'a [u8], target: SocketAddr) -> SendTo<'a> {
        SendTo {
            socket: self,
            buf,
            target,
        }
    }

    pub fn recv_from<'a>(&'a mut self, buf: &'a mut [u8]) -> RecvFrom<'a> {
        RecvFrom { socket: self, buf }
    }
}

impl Drop for UdpSocket {
    fn drop(&mut self) {
        reactor().deregister(&mut self.io, &self.token).unwrap();
    }
}

fn poll_io<T>(
    socket: &mut UdpSocket,
    cx: &mut Context<'_>,
    interest: Interest,
    mut op: impl FnMut(&mio::net::UdpSocket) -> io::Result<T>,
) -> Poll<io::Result<T>> {
    loop {
        match op(&socket.io) {
            Ok(v) => return Poll::Ready(Ok(v)),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                if let Err(e) =
                    reactor().register(&mut socket.io, socket.token, interest, cx.waker().clone())
                {
                    return Poll::Ready(Err(e.into()));
                }
                return Poll::Pending;
            }
            Err(e) => return Poll::Ready(Err(e)),
        }
    }
}

pub struct SendTo<'a> {
    socket: &'a mut UdpSocket,
    buf: &'a [u8],
    target: SocketAddr,
}

impl Future for SendTo<'_> {
    type Output = io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        poll_io(this.socket, cx, Interest::WRITABLE, |io| {
            io.send_to(this.buf, this.target)
        })
    }
}

pub struct RecvFrom<'a> {
    socket: &'a mut UdpSocket,
    buf: &'a mut [u8],
}

impl Future for RecvFrom<'_> {
    type Output = io::Result<(usize, SocketAddr)>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        poll_io(this.socket, cx, Interest::READABLE, |io| {
            io.recv_from(this.buf)
        })
    }
}
