#![warn(
    clippy::all
    // clippy::correctness,
    // clippy::restriction,
    // clippy::pedantic,
    // clippy::nursery,
    // clippy::cargo
)]

use std::{collections::HashMap, net::SocketAddr};

use anyhow::Context;
use cmd_parser::client_cmd::ClientCmd;
use kvstore::{KvChan, KvStoreCmd};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, Sender},
};
use tracing::debug;

use crate::{
    app_config::AppConfig,
    conn_with_master::prepare_conn_with_master,
    kvstore::prepare_kvstore_channel,
    log::setup_log,
    resp_type::{parser::parse_request, RESPType},
};

pub(crate) mod app_config;
pub(crate) mod cmd_parser;
pub(crate) mod cmd_processor;
pub(crate) mod conn_with_master;
pub(crate) mod kvstore;
pub(crate) mod log;
pub(crate) mod resp_type;

pub const LINE_ENDING: &str = "\r\n";
pub const NEW_LINE: u8 = b'\n';

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_log()?;
    debug!("🚀🚀🚀 Logs from your program will appear here! 🚀🚀🚀");
    let port = AppConfig::get_port();
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    let kv_chan = prepare_kvstore_channel().await;
    let slaves_chan = prepare_master_to_slave_chan().await;
    prepare_conn_with_master(kv_chan.clone()).await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("Got a request from: {:?}", addr);
        let kv_chan = kv_chan.clone();
        let slaves_chan = slaves_chan.clone();
        tokio::spawn(async move {
            handle_connection(stream, addr, kv_chan, slaves_chan)
                .await
                .expect("Connection was disconnected with an error")
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    addr: SocketAddr,
    kv_chan: KvChan,
    slaves_chan: Sender<MasterToSlaveCmd>,
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
        if let ClientCmd::Set { key, value, flags } = &client_cmd {
            slaves_chan
                .send(MasterToSlaveCmd::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                    flags: flags.clone(),
                })
                .await?;
            continue;
        }
        if let ClientCmd::Psync { .. } = client_cmd {
            slaves_chan
                .send(MasterToSlaveCmd::SaveStream {
                    host: addr.ip().to_string(),
                    port: addr.port(),
                    stream,
                })
                .await?;
            return Ok(());
        };
        if let ClientCmd::ExitConn = client_cmd {
            break;
        }
        writer.flush().await?;
    }
    debug!("Connection closed successfully!");
    Ok(())
}

async fn prepare_master_to_slave_chan() -> mpsc::Sender<MasterToSlaveCmd> {
    use MasterToSlaveCmd::*;
    let (tx, mut rx) = mpsc::channel::<MasterToSlaveCmd>(10);
    tokio::spawn(async move {
        let mut streams_map: HashMap<String, TcpStream> = HashMap::new();
        while let Some(cmd) = rx.recv().await {
            match cmd {
                SaveStream { host, port, stream } => {
                    let key = format!("{host}:{port}");
                    streams_map.insert(key, stream);
                }
                Set {
                    key,
                    value,
                    flags: _flags,
                } => {
                    debug!("Sending to stream");
                    let msg = RESPType::Array(vec![
                        RESPType::BulkString("SET".to_string()),
                        RESPType::BulkString(key.clone()),
                        RESPType::BulkString(value.clone()),
                    ]);
                    for (_, v) in &mut streams_map {
                        debug!("Sending to stream asdfadf");
                        let _ = v.write_all(&msg.as_bytes()).await;
                        v.flush().await.unwrap();
                    }
                }
            }
        }
    });
    tx
}

#[derive(Debug)]
pub enum MasterToSlaveCmd {
    SaveStream {
        host: String,
        port: u16,
        stream: TcpStream,
    },
    Set {
        key: String,
        value: String,
        flags: HashMap<String, String>,
    },
}
