#![warn(
    clippy::all
    // clippy::correctness,
    // clippy::restriction,
    // clippy::pedantic,
    // clippy::nursery,
    // clippy::cargo
)]

use anyhow::{bail, Context};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    net::{tcp::ReadHalf, TcpListener},
};
use tracing::{debug, error, info};

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
        let (reader, _writer) = stream.split();
        let mut reader = BufReader::new(reader);
        parse_request(&mut reader).await?; // Call perform_request with the reader as a parameter
    }
}

async fn parse_request<'a>(reader: &'a mut BufReader<ReadHalf<'_>>) -> anyhow::Result<()> {
    let mut buf = [0; 1];
    reader
        .read_exact(&mut buf)
        .await
        .context(fdbg!("Unable to read string from client"))?;
    debug!(": {}", buf[0] as char);
    match buf[0] {
        b'*' => {
            let count = read_count(reader).await?;
            debug!("Length of an array: {:?}", count);
        }
        _ => bail!("Unable to determine data type: {}", buf[0] as char),
    };
    Ok(())
}
async fn read_count<'a>(reader: &'a mut BufReader<ReadHalf<'_>>) -> anyhow::Result<usize> {
    let mut buf = vec![];
    let read_count = reader
        .read_until(NEW_LINE, &mut buf)
        .await
        .context(fdbg!("Unable to read length of data"))?;
    let number_part = &buf[..(read_count - LINE_ENDING.len())];
    let length = std::str::from_utf8(number_part)
        .context(fdbg!("Unable to convert length to string"))?
        .parse::<usize>()
        .context(fdbg!("Unable to parse length to usize"))?;
    Ok(length)
}
