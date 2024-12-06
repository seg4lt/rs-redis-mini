use std::net::SocketAddr;

use anyhow::Context;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tracing::debug;

use crate::database::db_event::DatabaseEvent;
use crate::database::db_event::DatabaseEvent::Set;
use crate::{
    app_config::AppConfig, cmd_parser::server_command::ServerCommand, fdbg,
    replication::ReplicationEvent, resp_type::RESPType,
};

pub struct Server {}
impl Server {
    pub async fn start() -> anyhow::Result<()> {
        let port = AppConfig::get_port();
        let listener = TcpListener::bind(format!("127.0.0.1:{port}")).await?;
        loop {
            let (stream, addr) = listener.accept().await?;
            debug!("Got a request from: {:?}", addr);
            tokio::spawn(async move {
                Self::handle_stream(stream, addr)
                    .await
                    .expect("Connection was disconnected with an error")
            });
        }
    }
    async fn handle_stream(mut stream: TcpStream, addr: SocketAddr) -> anyhow::Result<()> {
        let (reader, mut writer) = stream.split();
        let mut reader = BufReader::new(reader);
        let mut tx_stack: Vec<Vec<ServerCommand>> = vec![];
        loop {
            let resp_type = RESPType::parse(&mut reader).await?;
            let client_cmd = ServerCommand::from(&resp_type)?;
            let Some(client_cmd) = queue_if_transaction_active(client_cmd, &mut tx_stack) else {
                writer
                    .write(&RESPType::SimpleString("QUEUED".to_string()).as_bytes())
                    .await
                    .context(fdbg!("unable to write queued string"))?;
                continue;
            };
            client_cmd
                .process_client_cmd(&mut writer, &mut tx_stack)
                .await
                .context(fdbg!("Unable to write to client stream"))?;
            writer.flush().await?;
            match client_cmd {
                ServerCommand::PSync { .. } => {
                    let (host, port) = (addr.ip().to_string(), addr.port());
                    ReplicationEvent::SaveStream { host, port, stream }
                        .emit()
                        .await?;
                    break;
                }
                ServerCommand::ExitConn => {
                    debug!("Connection closed successfully!");
                    break;
                }
                _ => continue,
            }
        }
        Ok(())
    }
}

fn queue_if_transaction_active(
    cmd: ServerCommand,
    tx_stack: &mut Vec<Vec<ServerCommand>>,
) -> Option<ServerCommand> {
    use ServerCommand::*;
    if matches!(cmd, Exec) || tx_stack.is_empty() {
        return Some(cmd);
    }
    match cmd {
        Set { .. } => {
            tx_stack.first_mut().unwrap().push(cmd);
            None
        }
        _ => Some(cmd),
    }
}
