use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use bytes::BufMut;
use cfg_if::cfg_if;

pub use buffered::{BufferedSocket, WriteBuffer};

use crate::io::ReadBuf;

mod buffered;

pub trait Socket: Send + Sync + Unpin + 'static {
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> io::Result<usize>;

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize>;

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    fn poll_flush(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // `flush()` is a no-op for TCP/UDS
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>>;

    fn read<'a, B: ReadBuf>(&'a mut self, buf: &'a mut B) -> Read<'a, Self, B>
    where
        Self: Sized,
    {
        Read { socket: self, buf }
    }

    fn write<'a>(&'a mut self, buf: &'a [u8]) -> Write<'a, Self>
    where
        Self: Sized,
    {
        Write { socket: self, buf }
    }

    fn flush(&mut self) -> Flush<'_, Self>
    where
        Self: Sized,
    {
        Flush { socket: self }
    }

    fn shutdown(&mut self) -> Shutdown<'_, Self>
    where
        Self: Sized,
    {
        Shutdown { socket: self }
    }
}

pub struct Read<'a, S: ?Sized, B> {
    socket: &'a mut S,
    buf: &'a mut B,
}

impl<'a, S: ?Sized, B> Future for Read<'a, S, B>
where
    S: Socket,
    B: ReadBuf,
{
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        while this.buf.has_remaining_mut() {
            match this.socket.try_read(&mut *this.buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    ready!(this.socket.poll_read_ready(cx))?;
                }
                ready => return Poll::Ready(ready),
            }
        }

        Poll::Ready(Ok(0))
    }
}

pub struct Write<'a, S: ?Sized> {
    socket: &'a mut S,
    buf: &'a [u8],
}

impl<'a, S: ?Sized> Future for Write<'a, S>
where
    S: Socket,
{
    type Output = io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;

        while !this.buf.is_empty() {
            match this.socket.try_write(this.buf) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    ready!(this.socket.poll_write_ready(cx))?;
                }
                ready => return Poll::Ready(ready),
            }
        }

        Poll::Ready(Ok(0))
    }
}

pub struct Flush<'a, S: ?Sized> {
    socket: &'a mut S,
}

impl<'a, S: Socket + ?Sized> Future for Flush<'a, S> {
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.socket.poll_flush(cx)
    }
}

pub struct Shutdown<'a, S: ?Sized> {
    socket: &'a mut S,
}

impl<'a, S: ?Sized> Future for Shutdown<'a, S>
where
    S: Socket,
{
    type Output = io::Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.socket.poll_shutdown(cx)
    }
}

pub trait WithSocket {
    type Output;

    fn with_socket<S: Socket>(
        self,
        socket: S,
    ) -> impl std::future::Future<Output = Self::Output> + Send;
}

pub struct SocketIntoBox;

impl WithSocket for SocketIntoBox {
    type Output = Box<dyn Socket>;

    async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
        Box::new(socket)
    }
}

impl<S: Socket + ?Sized> Socket for Box<S> {
    fn try_read(&mut self, buf: &mut dyn ReadBuf) -> io::Result<usize> {
        (**self).try_read(buf)
    }

    fn try_write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (**self).try_write(buf)
    }

    fn poll_read_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (**self).poll_read_ready(cx)
    }

    fn poll_write_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (**self).poll_write_ready(cx)
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (**self).poll_flush(cx)
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        (**self).poll_shutdown(cx)
    }
}

pub async fn connect_tcp<Ws: WithSocket>(
    host: &str,
    port: u16,
    with_socket: Ws,
) -> crate::Result<Ws::Output> {
    connect_tcp_inner(host, port, None, with_socket).await
}

/// Connect with client-side TCP keepalive configured before the socket is
/// handed to `with_socket`, including any TLS upgrade it performs.
///
/// These settings detect an unresponsive peer; they do not limit a query's
/// running time. An unsupported setting fails the connection attempt.
pub async fn connect_tcp_with_keepalive<Ws: WithSocket>(
    host: &str,
    port: u16,
    idle: Duration,
    interval: Duration,
    retries: u32,
    with_socket: Ws,
) -> crate::Result<Ws::Output> {
    connect_tcp_inner(host, port, Some((idle, interval, retries)), with_socket).await
}

async fn connect_tcp_inner<Ws: WithSocket>(
    host: &str,
    port: u16,
    keepalive: Option<(Duration, Duration, u32)>,
    with_socket: Ws,
) -> crate::Result<Ws::Output> {
    // IPv6 addresses in URLs will be wrapped in brackets and the `url` crate doesn't trim those.
    let host = host.trim_matches(&['[', ']'][..]);

    #[cfg(feature = "_rt-tokio")]
    if crate::rt::rt_tokio::available() {
        use tokio::net::TcpStream;

        let stream = TcpStream::connect((host, port)).await?;
        stream.set_nodelay(true)?;
        if let Some((idle, interval, retries)) = keepalive {
            socket2::SockRef::from(&stream)
                .set_tcp_keepalive(&tcp_keepalive(idle, interval, retries)?)?;
        }

        return Ok(with_socket.with_socket(stream).await);
    }

    #[cfg(feature = "_rt-async-std")]
    {
        use async_io::Async;
        use async_std::net::ToSocketAddrs;
        use std::net::TcpStream;

        let mut last_err = None;

        // Loop through all the Socket Addresses that the hostname resolves to
        for socket_addr in (host, port).to_socket_addrs().await? {
            let stream = Async::<TcpStream>::connect(socket_addr)
                .await
                .and_then(|s| {
                    s.get_ref().set_nodelay(true)?;
                    if let Some((idle, interval, retries)) = keepalive {
                        socket2::SockRef::from(s.get_ref())
                            .set_tcp_keepalive(&tcp_keepalive(idle, interval, retries)?)?;
                    }
                    Ok(s)
                });
            match stream {
                Ok(stream) => return Ok(with_socket.with_socket(stream).await),
                Err(e) => last_err = Some(e),
            }
        }

        // If we reach this point, it means we failed to connect to any of the addresses.
        // Return the last error we encountered, or a custom error if the hostname didn't resolve to any address.
        match last_err {
            Some(err) => Err(err.into()),
            None => Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "Hostname did not resolve to any addresses",
            )
            .into()),
        }
    }

    #[cfg(not(feature = "_rt-async-std"))]
    {
        crate::rt::missing_rt((host, port, keepalive, with_socket))
    }
}

#[cfg(any(feature = "_rt-tokio", feature = "_rt-async-std"))]
fn tcp_keepalive(
    idle: Duration,
    interval: Duration,
    retries: u32,
) -> io::Result<socket2::TcpKeepalive> {
    cfg_if! {
        if #[cfg(any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "ios",
            target_os = "visionos",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "windows",
            target_os = "cygwin",
            all(target_os = "wasi", not(target_env = "p1")),
        ))] {
            Ok(socket2::TcpKeepalive::new()
                .with_time(idle)
                .with_interval(interval)
                .with_retries(retries))
        } else {
            let _ = (idle, interval, retries);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "TCP keepalive probe intervals and retries are not supported on this platform",
            ))
        }
    }
}

#[cfg(all(test, feature = "_rt-tokio", target_os = "linux"))]
mod tests {
    use super::{connect_tcp, connect_tcp_with_keepalive, Socket, WithSocket};
    use std::time::Duration;

    struct InspectTcp;

    impl WithSocket for InspectTcp {
        type Output = (bool, Duration, Duration, u32);

        async fn with_socket<S: Socket>(self, socket: S) -> Self::Output {
            let stream = (Box::new(socket) as Box<dyn std::any::Any + Send>)
                .downcast::<tokio::net::TcpStream>()
                .ok()
                .expect("the Tokio connector hands its TCP socket to the TLS upgrade");
            let socket = socket2::SockRef::from(stream.as_ref());
            (
                socket.keepalive().unwrap(),
                socket.tcp_keepalive_time().unwrap(),
                socket.tcp_keepalive_interval().unwrap(),
                socket.tcp_keepalive_retries().unwrap(),
            )
        }
    }

    #[test]
    fn tcp_keepalive_is_optional_and_set_before_the_socket_upgrade() {
        tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .build()
            .unwrap()
            .block_on(async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let port = listener.local_addr().unwrap().port();

                let default = connect_tcp("127.0.0.1", port, InspectTcp).await.unwrap();
                assert!(!default.0);

                let idle = Duration::from_secs(30);
                let interval = Duration::from_secs(10);
                let configured =
                    connect_tcp_with_keepalive("127.0.0.1", port, idle, interval, 3, InspectTcp)
                        .await
                        .unwrap();
                assert_eq!(configured, (true, idle, interval, 3));
            });
    }
}

/// Connect a Unix Domain Socket at the given path.
///
/// Returns an error if Unix Domain Sockets are not supported on this platform.
pub async fn connect_uds<P: AsRef<Path>, Ws: WithSocket>(
    path: P,
    with_socket: Ws,
) -> crate::Result<Ws::Output> {
    #[cfg(unix)]
    {
        #[cfg(feature = "_rt-tokio")]
        if crate::rt::rt_tokio::available() {
            use tokio::net::UnixStream;

            let stream = UnixStream::connect(path).await?;

            return Ok(with_socket.with_socket(stream).await);
        }

        #[cfg(feature = "_rt-async-std")]
        {
            use async_io::Async;
            use std::os::unix::net::UnixStream;

            let stream = Async::<UnixStream>::connect(path).await?;

            Ok(with_socket.with_socket(stream).await)
        }

        #[cfg(not(feature = "_rt-async-std"))]
        {
            crate::rt::missing_rt((path, with_socket))
        }
    }

    #[cfg(not(unix))]
    {
        drop((path, with_socket));

        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Unix domain sockets are not supported on this platform",
        )
        .into())
    }
}
