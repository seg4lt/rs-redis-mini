#![warn(clippy::all)]

use std::net::SocketAddr;

use anyhow::Context;
use database::DatabaseEventListener;
use slave_communication::{setup_master_to_slave_communication, MasterToSlaveCmd};
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::mpsc::Sender,
};
use tracing::debug;

use crate::{
    app_config::AppConfig, cmd_parser::client_cmd::ClientCmd,
    conn_with_master::prepare_conn_with_master, database::Database, log::setup_log,
    resp_type::parser::parse_request,
};

pub(crate) mod app_config;
pub(crate) mod cmd_parser;
pub(crate) mod cmd_processor;
pub(crate) mod conn_with_master;
pub(crate) mod database;
pub(crate) mod log;
pub(crate) mod resp_type;
pub(crate) mod slave_communication;

pub const LINE_ENDING: &str = "\r\n";
pub const NEW_LINE: u8 = b'\n';

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_log()?;
    debug!("🚀🚀🚀 Logs from your program will appear here! 🚀🚀🚀");
    let db_event_listener = Database::new();
    let port = AppConfig::get_port();
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    let slaves_chan = setup_master_to_slave_communication().await;
    prepare_conn_with_master(db_event_listener.clone()).await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("Got a request from: {:?}", addr);
        let kv_chan = db_event_listener.clone();
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
    kv_chan: DatabaseEventListener,
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
            writer.flush().await?;
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
            writer.flush().await?;
            break;
        }
        writer.flush().await?;
    }
    debug!("Connection closed successfully!");
    Ok(())
}
