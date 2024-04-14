use std::net::SocketAddr;

use anyhow::Context;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tracing::debug;

use crate::{
    app_config::AppConfig, cmd_parser::server_command::ServerCommand, fdbg,
    replication::ReplicationEvent, resp_type::RESPType,
};

pub struct Server {}
impl Server {
    pub async fn start() -> anyhow::Result<()> {
        let port = AppConfig::get_port();
        let listener = TcpListener::bind(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
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
        loop {
            let resp_type = RESPType::parse(&mut reader).await?;
            let client_cmd = ServerCommand::from(&resp_type)?;
            client_cmd
                .process_client_cmd(&mut writer)
                .await
                .context(fdbg!("Unable to write to client stream"))?;
            writer.flush().await?;
            match client_cmd {
                ServerCommand::Psync { .. } => {
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
