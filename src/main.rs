#![warn(
    clippy::all
    // clippy::correctness,
    // clippy::restriction,
    // clippy::pedantic,
    // clippy::nursery,
    // clippy::cargo
)]

use std::collections::HashMap;

use anyhow::Context;
use cmd::client_cmd::ClientCmd;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{
        mpsc::{self, channel},
        oneshot,
    },
};
use tracing::{debug, info, span, Level};

use crate::{log::setup_log, resp_type::parser::parse_request};

pub(crate) mod cmd;
pub(crate) mod cmd_processor;
pub(crate) mod log;
pub(crate) mod resp_type;

pub const LINE_ENDING: &[u8; 2] = b"\r\n";
pub const NEW_LINE: u8 = b'\n';

#[derive(Debug)]
pub enum KvStoreCmd {
    Set {
        key: String,
        value: String,
    },
    SetWithGet {
        resp: oneshot::Sender<Option<String>>,
        key: String,
        value: String,
    },
    Get {
        resp: oneshot::Sender<Option<String>>,
        key: String,
    },
}

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

async fn handle_connection(
    mut stream: TcpStream,
    kv_chan: mpsc::Sender<KvStoreCmd>,
) -> anyhow::Result<()> {
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

async fn prepare_kvstore_channel() -> mpsc::Sender<KvStoreCmd> {
    let (tx, mut rx) = channel::<KvStoreCmd>(100);
    tokio::spawn(async move {
        let span = span!(Level::TRACE, "KvStoreChannel");
        let _guard = span.enter();
        let mut map = HashMap::<String, String>::new();
        while let Some(cmd) = rx.recv().await {
            match cmd {
                KvStoreCmd::Set { key, value } => {
                    info!("Setting key: {} with value: {}", key, value);
                    map.insert(key, value);
                }
                KvStoreCmd::SetWithGet { resp, key, value } => {
                    info!("Setting key: {} with value: {}", key, value);
                    let prev_value = map.get(&key).map(|v| v.clone());
                    map.insert(key, value);
                    resp.send(prev_value).unwrap();
                }
                KvStoreCmd::Get { resp, key } => {
                    info!("Getting value for key: {}", key);
                    let prev_value = map.get(&key).map(|v| v.clone());
                    // Ignoring error for now
                    let _ = resp.send(prev_value);
                }
            }
        }
    });
    tx
}
