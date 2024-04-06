#![warn(
    clippy::all
    // clippy::correctness,
    // clippy::restriction,
    // clippy::pedantic,
    // clippy::nursery,
    // clippy::cargo
)]

use anyhow::Context;
use tokio::{
    io::AsyncReadExt,
    net::{tcp::ReadHalf, TcpListener},
};
use tracing::{debug, info};

use crate::log::setup_log;

pub(crate) mod log;

pub const LINE_ENDING: &str = "\r\n";
pub const NEW_LINE: u8 = b'\n';

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_log()?;
    debug!("🚀🚀🚀 Logs from your program will appear here! 🚀🚀🚀 ");
    let port = 6_379_u16;
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .context(fdbg!("Unable to bind to port:{port}"))?;
    info!("Server started on 127.0.0.1:{port}");

    loop {
        let (mut stream, _) = listener.accept().await?;
        let (mut reader, _writer) = stream.split();
        perform_request(&mut reader).await?; // Call perform_request with the reader as a parameter
    }
}

async fn perform_request<'a>(reader: &'a mut ReadHalf<'_>) -> anyhow::Result<()> {
    let mut buf = [0; 1];
    reader
        .read_exact(&mut buf)
        .await
        .context(fdbg!("Unable to read string from client"))?;
    debug!("Buf: {}", buf[0] as char);
    Ok(())
}
