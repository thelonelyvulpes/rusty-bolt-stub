use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::Poll;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use crate::net_actor::Connection;

pub struct WebSocketStream {
    conn: TcpStream,
}

impl WebSocketStream {
    pub fn new(conn: TcpStream) -> Self {
        Self { conn }
    }
}

impl AsyncRead for WebSocketStream {
    fn poll_read(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        todo!()
    }
}

impl AsyncWrite for WebSocketStream {
    fn poll_write(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &[u8]) -> Poll<Result<usize, io::Error>> {
        todo!()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), io::Error>> {
        todo!()
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Result<(), io::Error>> {
        todo!()
    }
}

impl Connection for WebSocketStream {
    fn addresses(&self) -> io::Result<(SocketAddr, SocketAddr)> {
        todo!()
    }
}