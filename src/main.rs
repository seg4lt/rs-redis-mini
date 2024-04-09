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
    sync::{
        mpsc::{self, Sender},
        oneshot,
    },
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
            .process_client_cmd(&mut writer, &kv_chan, &slaves_chan)
            .await
            .context(fdbg!("Unable to write to client stream"))?;
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
                GetNumOfReplicas { resp: recv_chan } => {
                    let _ = recv_chan.send(streams_map.len());
                }
                GetAck { min_ack, resp } => {
                    let mut acks_received = 0;

                    let req = RESPType::Array(vec![
                        RESPType::BulkString("REPLCONF".to_string()),
                        RESPType::BulkString("GETACK".to_string()),
                        RESPType::BulkString("*".to_string()),
                    ]);
                    for (_, v) in &mut streams_map {
                        let _ = v.write_all(&req.as_bytes()).await;
                        v.flush().await.unwrap();
                        let (reader, _) = v.split();
                        let mut reader = BufReader::new(reader);
                        let resp_type = parse_request(&mut reader).await.unwrap();
                        debug!("RESP from slave - {:?}", resp_type);
                        acks_received += 1;
                        debug!(
                            "Acks received {:?} -- min_acks -- {}",
                            acks_received, min_ack
                        );
                        if acks_received >= min_ack {
                            break;
                        }
                    }
                    let _ = resp.send(acks_received);
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
    GetNumOfReplicas {
        resp: oneshot::Sender<usize>,
    },
    GetAck {
        min_ack: usize,
        resp: oneshot::Sender<usize>,
    },
}
