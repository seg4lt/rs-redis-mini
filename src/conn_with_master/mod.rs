use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
};
use tracing::{debug, span, Level};

use crate::{
    app_config::AppConfig,
    resp_type::{parser::parse_request, RESPType},
};

pub(crate) async fn prepare_conn_with_master() -> anyhow::Result<()> {
    if AppConfig::is_master() {
        return Ok(());
    }
    debug!("Starting connection with master");
    let Some((host, port)) = AppConfig::get_replicaof() else {
        panic!("Replica should have --replicaof args");
    };
    let mut stream = TcpStream::connect(format!("{host}:{port}")).await?;
    tokio::spawn(async move {
        let (reader, mut writer) = stream.split();
        let mut reader = BufReader::new(reader);
        let ping = RESPType::Array(vec![RESPType::BulkString("PING".to_string())]);
        writer
            .write_all(&ping.as_bytes())
            .await
            .expect("Should be able to write PING");
        writer.flush().await.expect("Should be able to flush PING");
        let response = parse_request(&mut reader)
            .await
            .expect("Should be able to parse PONG");
    });
    Ok(())
}
