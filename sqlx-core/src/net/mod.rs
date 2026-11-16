mod socket;
pub mod tls;

pub use socket::{
    connect_tcp, connect_tcp_with_options, connect_uds, BufferedSocket, Socket, SocketIntoBox,
    WithSocket, WriteBuffer,
};
