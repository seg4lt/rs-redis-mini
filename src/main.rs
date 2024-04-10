#![warn(clippy::all)]

use std::net::SocketAddr;

use anyhow::Context;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tracing::debug;

use crate::{
    app_config::AppConfig, cmd_parser::client_cmd::ClientCmd,
    conn_with_master::prepare_conn_with_master, database::Database, log::setup_log,
    replication::ReplicationEvent, resp_type::parser::parse_request,
};

pub(crate) mod app_config;
pub(crate) mod cmd_parser;
pub(crate) mod cmd_processor;
pub(crate) mod conn_with_master;
pub(crate) mod database;
pub(crate) mod log;
pub(crate) mod replication;
pub(crate) mod resp_type;

pub const LINE_ENDING: &str = "\r\n";
pub const NEW_LINE: u8 = b'\n';

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_log()?;
    debug!("🚀🚀🚀 Logs from your program will appear here! 🚀🚀🚀");
    Database::new();
    ReplicationEvent::setup();
    let port = AppConfig::get_port();
    let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .unwrap();
    prepare_conn_with_master().await?;
    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("Got a request from: {:?}", addr);
        tokio::spawn(async move {
            handle_connection(stream, addr)
                .await
                .expect("Connection was disconnected with an error")
        });
    }
}

async fn handle_connection(mut stream: TcpStream, addr: SocketAddr) -> anyhow::Result<()> {
    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    loop {
        let resp_type = parse_request(&mut reader).await?;
        let client_cmd = ClientCmd::from_resp_type(&resp_type)?;
        client_cmd
            .process_client_cmd(&mut writer)
            .await
            .context(fdbg!("Unable to write to client stream"))?;
        if let ClientCmd::Psync { .. } = client_cmd {
            writer.flush().await?;
            ReplicationEvent::SaveStream {
                host: addr.ip().to_string(),
                port: addr.port(),
                stream,
            }
            .emit()
            .await?;
            // Doing return here as we are not closing the connection but rather saving it for replication
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
