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
    io::BufReader,
    net::{TcpListener, TcpStream},
};
use tracing::{debug, info};

use crate::{log::setup_log, resp_type::parser::parse_request};

pub(crate) mod cmd;
pub(crate) mod cmd_processor;
pub(crate) mod log;
pub(crate) mod resp_type;

pub const LINE_ENDING: &[u8; 2] = b"\r\n";
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
        let (stream, addr) = listener.accept().await?;
        debug!("Got a request from: {:?}", addr);
        tokio::spawn(async move {
            handle_connection(stream)
                .await
                .expect("Connection was disconnected with an error")
        });
    }
}

async fn handle_connection(mut stream: TcpStream) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    loop {
        let end_stream = parse_request(&mut reader)
            .await?
            .to_client_cmd()?
            .process_client_cmd(&mut writer)
            .await
            .context(fdbg!("Unable to write to client stream"))?;
        if end_stream {
            break;
        }
    }
    Ok(())
}
