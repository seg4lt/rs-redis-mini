use std::net::SocketAddr;

use anyhow::Context;
use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use tracing::debug;

use crate::{
    app_config::AppConfig, cmd_parser::client_cmd::ClientCmd, fdbg, replication::ReplicationEvent,
    resp_type::parser::parse_request,
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
}
