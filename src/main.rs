#![warn(
    clippy::all
    // clippy::correctness,
    // clippy::restriction,
    // clippy::pedantic,
    // clippy::nursery,
    // clippy::cargo
)]

use anyhow::Context;
use cmd_parser::client_cmd::ClientCmd;
use kvstore::{KvChan, KvStoreCmd};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tracing::debug;

use crate::{kvstore::prepare_kvstore_channel, log::setup_log, resp_type::parser::parse_request};

pub(crate) mod cmd_parser;
pub(crate) mod cmd_processor;
pub(crate) mod kvstore;
pub(crate) mod log;
pub(crate) mod resp_type;

pub const LINE_ENDING: &[u8; 2] = b"\r\n";
pub const NEW_LINE: u8 = b'\n';

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_log()?;
    debug!("🚀🚀🚀 Logs from your program will appear here! 🚀🚀🚀");
    let port = 6_379_u16;
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    let kv_chan = prepare_kvstore_channel().await;
    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("Got a request from: {:?}", addr);
        let kv_chan = kv_chan.clone();
        tokio::spawn(async move {
            handle_connection(stream, kv_chan)
                .await
                .expect("Connection was disconnected with an error")
        });
    }
}

async fn handle_connection(mut stream: TcpStream, kv_chan: KvChan) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    loop {
        let resp_type = parse_request(&mut reader).await?;
        let client_cmd = ClientCmd::from_resp_type(&resp_type)?;
        client_cmd
            .process_client_cmd(&mut writer, &kv_chan)
            .await
            .context(fdbg!("Unable to write to client stream"))?;
        if let ClientCmd::EOF = client_cmd {
            break;
        }
        writer.flush().await?;
    }
    debug!("Connection closed successfully!");
    Ok(())
}
